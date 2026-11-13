from ._pysciqlop_cache import Cache as _Cache, Index as _Index, FanoutCache as _FanoutCache, FanoutIndex as _FanoutIndex, Timeout
import atexit
import base64
import functools
import hashlib
import inspect
import io
import pickle
import time
import weakref
from collections.abc import ItemsView, MutableMapping, ValuesView
from datetime import timedelta
from typing import Any, AnyStr, Optional, Union

from .serializers import (
    SERIALIZER_UPGRADES,
    MsgspecSerializer,
    PickleOOBSerializer,
    PickleSerializer,
    Serializer,
    get_serializer_by_name,
)


def _encode_value(serializer, value):
    # A serializer that can hand out separate chunks (pickle-oob) lets the store
    # write them without joining them first, with the GIL released.
    dumps_chunks = getattr(serializer, "dumps_chunks", None)
    return serializer.dumps(value) if dumps_chunks is None else dumps_chunks(value)


_MISSING = object()


def _get_many(store, keys):
    keys = list(keys)
    raw = store._get_many([k if type(k) is str else _encode_key(store, k) for k in keys])
    loads = store._serializer.loads
    return {k: loads(v.memoryview()) for k, v in zip(keys, raw) if v is not None}


def _encoded_items(store, mapping):
    """(key, payload) pairs of a batch, serialized before any lock is taken: a value
    that fails to serialize aborts the batch before anything is written."""
    return [(k if type(k) is str else _encode_key(store, k), _encode_value(store._serializer, v))
            for k, v in mapping.items()]
_META_SERIALIZER = "serializer"
_META_MAX_SIZE = "max_size"
_META_MAX_BLOB_SIZE = "max_blob_size"
_SENTINEL = object()


def _resolve_serializer(store, requested):
    """The serializer to use for `store`, recorded in its meta.

    Reopening without one uses the recorded serializer. Asking for another
    one raises, unless it can read what the recorded one wrote (an upgrade).
    """
    stored = store.get_meta(_META_SERIALIZER)
    if requested is None:
        if stored is not None:
            try:
                return get_serializer_by_name(stored)
            except ValueError:
                raise ValueError(
                    f"{type(store).__name__} was written with the serializer {stored!r}, which is "
                    "not built in: open it with serializer=<that serializer>."
                ) from None
        requested = PickleSerializer()
    elif stored not in (None, requested.name) and (stored, requested.name) not in SERIALIZER_UPGRADES:
        raise ValueError(
            f"{type(store).__name__} metadata {_META_SERIALIZER!r} is {stored!r}, "
            f"but {requested.name!r} was requested."
        )
    if stored != requested.name:
        store.set_meta(_META_SERIALIZER, requested.name)
    return requested

# diskcache tuning knobs (SQLite pragmas, eviction policy, statistics, ...) with no
# equivalent here; accepted and ignored so diskcache configs drop in unchanged.
_IGNORED_SETTINGS = frozenset({
    "statistics", "tag_index", "eviction_policy", "cull_limit", "sqlite_auto_vacuum",
    "sqlite_cache_size", "sqlite_journal_mode", "sqlite_mmap_size", "sqlite_synchronous",
    "disk_pickle_protocol",
})

__all__ = [
    "Cache", "Index", "FanoutCache", "FanoutIndex", "Lock", "Serializer",
    "PickleSerializer", "PickleOOBSerializer", "MsgspecSerializer", "Timeout",
]

# Keys that aren't str/int/float/bytes are pickled. A store whose serializer avoids
# pickle decodes them with _PlainKeyUnpickler, so choosing msgspec keeps the whole
# cache free of code-running unpickling.
_KEY_MARK = "\x00"  # reserved prefix; a str key starting with it is not checked for on the hot path


def _typed_key(key):
    if isinstance(key, int):  # bool included, like diskcache (True stores as 1)
        return f"\x00i{int(key)}"
    if isinstance(key, float):
        return "\x00f" + repr(key)
    if isinstance(key, bytes):
        return "\x00b" + key.decode("latin-1")
    return "\x00p" + base64.b64encode(pickle.dumps(key, protocol=4)).decode("ascii")


class _PlainKeyUnpickler(pickle.Unpickler):
    """Unpickles keys of a store whose serializer avoids pickle: plain hashable
    values only, never an arbitrary callable, so a key planted in the cache folder
    can't run code (https://docs.python.org/3/library/pickle.html#restricting-globals)."""

    _ALLOWED = frozenset({
        ("builtins", "frozenset"), ("builtins", "set"), ("builtins", "complex"),
        ("datetime", "datetime"), ("datetime", "date"), ("datetime", "time"),
        ("datetime", "timedelta"), ("datetime", "timezone"),
    })

    def find_class(self, module, name):
        if (module, name) in self._ALLOWED:
            return super().find_class(module, name)
        raise pickle.UnpicklingError(
            f"key of type {module}.{name} refused: this store's serializer does not use "
            "pickle, so its keys may only hold plain values (tuples, frozensets, dates, ...)"
        )


def _load_plain_key(data):
    return _PlainKeyUnpickler(io.BytesIO(data)).load()


_KEY_DECODERS = {
    "i": int,
    "f": float,
    "b": lambda s: s.encode("latin-1"),
    "p": lambda s: pickle.loads(base64.b64decode(s)),
}
_PLAIN_KEY_DECODERS = {**_KEY_DECODERS, "p": lambda s: _load_plain_key(base64.b64decode(s))}


def _decode_key(raw, decoders=_KEY_DECODERS):
    if raw[:1] != _KEY_MARK:
        return raw
    return decoders[raw[1]](raw[2:])


def _encode_key(store, key):
    if isinstance(key, str):
        return key
    if not store._keys_encoded:
        store.set_meta("encoded_keys", "1")
        store._keys_encoded = True
    return _typed_key(key)


def _decode_keys(store, raw):
    if store.get_meta("encoded_keys") is None:
        return raw
    decoders = _KEY_DECODERS if store._serializer.name.startswith("pickle") else _PLAIN_KEY_DECODERS
    decode = functools.partial(_decode_key, decoders=decoders)
    return [decode(k) for k in raw] if isinstance(raw, list) else map(decode, raw)


def _meta_result(value, meta, expire_time, tag):
    expire, t = meta if meta is not None else (None, None)
    if expire_time and tag:
        return value, expire, t
    if expire_time:
        return value, expire
    return value, t


_open_stores = weakref.WeakSet()


def _memoize_key(serializer, base, args, kwargs, typed):
    key_data = (args, tuple(sorted(kwargs.items())))
    if typed:
        key_data += (
            tuple(type(a) for a in args),
            tuple(type(v) for v in kwargs.values()),
        )
    return f"{base}:{hashlib.sha256(serializer.dumps(key_data)).hexdigest()}"


def _ignored_arguments(func, ignore):
    """`ignore` as (positions, names). A name also covers its parameter's
    position and a position its name, so ``ignore={"session"}`` works however
    the caller passes it (diskcache #240)."""
    positions = {i for i in ignore if isinstance(i, int)}
    names = {i for i in ignore if isinstance(i, str)}
    try:
        params = list(inspect.signature(func).parameters.values())
    except (TypeError, ValueError):  # builtins without a signature
        return frozenset(positions), frozenset(names)
    for index, p in enumerate(params):
        if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD):
            if p.name in names:
                positions.add(index)
            elif index in positions:
                names.add(p.name)
    return frozenset(positions), frozenset(names)


def _memoize_key_function(store, func, typed, version_aware, ignore, key):
    base = f"{func.__module__}.{func.__qualname__}"
    if version_aware:
        base = f"{base}@{hashlib.sha256(func.__code__.co_code).hexdigest()[:16]}"
    positions, names = _ignored_arguments(func, ignore)

    def cache_key(*args, **kwargs):
        if key is not None:
            return _memoize_key(store._serializer, base, (key(*args, **kwargs),), {}, typed)
        if positions or names:
            args = tuple(a for i, a in enumerate(args) if i not in positions)
            kwargs = {k: v for k, v in kwargs.items() if k not in names}
        return _memoize_key(store._serializer, base, args, kwargs, typed)

    return cache_key


def _memoize(store, func, expire, tag, typed, version_aware, ignore, key):
    cache_key = _memoize_key_function(store, func, typed, version_aware, ignore, key)

    # simplify: the async wrapper calls the blocking get()/set() directly. They
    # release the GIL and take microseconds for small values; a big value blocks
    # the event loop while it is written. Upgrade: asyncio.to_thread() for set().
    if inspect.iscoroutinefunction(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            k = cache_key(*args, **kwargs)
            result = store.get(k, _MISSING)
            if result is _MISSING:
                result = await func(*args, **kwargs)
                store.set(k, result, expire=expire, tag=tag)
            return result
    else:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            k = cache_key(*args, **kwargs)
            result = store.get(k, _MISSING)
            if result is _MISSING:
                result = func(*args, **kwargs)
                store.set(k, result, expire=expire, tag=tag)
            return result

    wrapper.__cache_key__ = cache_key
    return wrapper


@atexit.register
def _close_open_stores():
    # A store an embedding host (e.g. Julia via PythonCall) still references at
    # Py_Finalize would outlive the extension module, its SQLite connection never
    # closed. Registered at import, so atexit hooks added later still find it open.
    for store in list(_open_stores):
        store.close()


def _reject_disk(disk):
    if disk is not None:
        raise TypeError("disk= is not supported, use serializer= instead")


def _apply_max_blob_size(store, requested, disk_min_file_size=None):
    """An explicit max_blob_size replaces the recorded one; without one the recorded
    value applies, else the default (8 KB). diskcache's disk_min_file_size is the
    smallest value stored as a file, so it maps to one byte less."""
    if requested is None and disk_min_file_size is not None:
        requested = max(0, int(disk_min_file_size) - 1)
    if requested is None:
        stored = store.get_meta(_META_MAX_BLOB_SIZE)
        if stored is None:
            return
        requested = int(stored)
    elif requested < 0:
        raise ValueError(f"max_blob_size must be >= 0, got {requested}")
    else:
        store.set_meta(_META_MAX_BLOB_SIZE, str(requested))
    store._set_max_blob_size(requested)


def _reject_unknown_settings(cls_name, settings):
    for name in settings:
        if name not in _IGNORED_SETTINGS:
            raise TypeError(f"{cls_name}.__init__() got an unexpected keyword argument {name!r}")


_directory = property(lambda self: self.path(), doc="The cache folder, like diskcache's ``directory``.")


class Lock:
    """Cross-process lock backed by a cache's atomic add() operation.

    Uses a spin-lock: acquire() loops on cache.add() (which is atomic and
    fails if the key already exists), sleeping 1ms between attempts.
    release() deletes the key. Compatible with the context manager protocol.
    """

    __slots__ = ("_cache", "_key", "_expire", "_tag")

    def __init__(self, cache, key, expire=None, tag=None):
        self._cache = cache
        self._key = key
        self._expire = expire
        self._tag = tag

    def acquire(self):
        """Block until the lock is acquired."""
        kwargs = {}
        if self._expire is not None:
            kwargs["expire"] = self._expire
        if self._tag is not None:
            kwargs["tag"] = self._tag
        while not self._cache.add(self._key, b"", **kwargs):
            time.sleep(0.001)

    def release(self) -> bool:
        """Release the lock.

        Returns True if this call actually removed the lock row, False if
        the row was already gone (e.g. the lock expired or was cleared by
        another process — a "lost lock" the caller may want to flag).
        """
        # Bypass __delitem__ (which discards the bool from the C++ binding)
        # so callers can detect a lost lock.
        return self._cache.delete(self._key)

    def locked(self) -> bool:
        """True if some holder currently has the lock."""
        return self._key in self._cache

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, *exc):
        self.release()
        return False


class _TransactionContext:
    __slots__ = ("_store", "_guard", "_begin_args")

    def __init__(self, store, *begin_args):
        self._store = store
        self._guard = None
        self._begin_args = begin_args

    def __enter__(self):
        self._guard = self._store.begin_user_transaction(*self._begin_args)
        return self._store

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self._guard.rollback()
        else:
            self._guard.commit()
        self._guard = None
        return False


class Cache(_Cache):

    directory = _directory

    def __init__(
        self,
        directory: str | None = None,
        max_size: int | object = _SENTINEL,
        serializer: Serializer | None = None,
        *,
        cache_path: str | None = None,
        size_limit: int | None = None,
        timeout: float | None = None,
        disk=None,
        max_blob_size: int | None = None,
        **settings,
    ):
        _reject_disk(disk)
        disk_min_file_size = settings.pop("disk_min_file_size", None)
        _reject_unknown_settings("Cache", settings)
        path = directory if directory is not None else (cache_path if cache_path is not None else ".cache/")
        if size_limit is not None:
            max_size = size_limit
        super().__init__(cache_path=path, max_size=0, timeout=600.0 if timeout is None else timeout)
        _open_stores.add(self)
        self._keys_encoded = False
        self._serializer = _resolve_serializer(self, serializer)
        _apply_max_blob_size(self, max_blob_size, disk_min_file_size)
        effective_max_size = self._resolve_max_size(max_size if max_size is not _SENTINEL else None)
        if effective_max_size != 0:
            super().set_max_cache_size(effective_max_size)

    def _resolve_max_size(self, requested):
        # An explicit max_size replaces the recorded one; without one the
        # recorded value applies (0 = unlimited when nothing is recorded).
        stored = super().get_meta(_META_MAX_SIZE)
        if requested is None:
            if stored is not None:
                return int(stored)
            requested = 0
        super().set_meta(_META_MAX_SIZE, str(requested))
        return requested

    @property
    def serializer(self) -> Serializer:
        """The serializer in use, recorded in the cache on first open."""
        return self._serializer

    def set(
        self,
        key: AnyStr,
        value: Any,
        expire: Optional[Union[timedelta, int, float]] = None,
        read: bool = False,
        tag: Optional[str] = None,
        retry: bool = False,
    ):
        """Store ``value`` under ``key``, replacing any previous value.

        Args:
            key: A string, or any hashable key (numbers, bytes, tuples, ...).
            value: Any value the cache's serializer accepts.
            expire: Lifetime, as a :class:`~datetime.timedelta` or seconds. None
                (default) never expires.
            tag: Optional tag; :meth:`evict_tag` removes all entries of a tag.

        Returns:
            True, like diskcache.
        """
        if type(key) is not str:
            key = _encode_key(self, key)
        if read:
            raise NotImplementedError("read=True (file handles) is not supported")
        if type(expire) in (int, float):
            expire = timedelta(seconds=expire)
        super().set(key, _encode_value(self._serializer, value), expire=expire, tag=tag)
        return True

    def get(
        self,
        key: AnyStr,
        default=None,
        read: bool = False,
        expire_time: bool = False,
        tag: bool = False,
        retry: bool = False,
    ) -> Any:
        """The value stored under ``key``, or ``default`` if it is missing or expired.

        Args:
            key: The key.
            default: Returned when the key is missing.
            expire_time: Also return the entry's expiration time.
            tag: Also return the entry's tag.

        Returns:
            The value, or ``(value, expire_time)``, ``(value, tag)`` or
            ``(value, expire_time, tag)`` when asked for, like diskcache.
        """
        if type(key) is not str:
            key = _encode_key(self, key)
        if read or expire_time or tag:
            return self._get_meta(key, default, read, expire_time, tag)
        value = super().get(key)
        if value is not None:
            return self._serializer.loads(value.memoryview())
        return default

    def _get_meta(self, key, default, read, expire_time, tag):
        if read:
            raise NotImplementedError("read=True (file handles) is not supported")
        value = super().get(key)
        if value is None:
            return _meta_result(default, None, expire_time, tag)
        meta = self.expire_and_tag(key)
        return _meta_result(self._serializer.loads(value.memoryview()), meta, expire_time, tag)

    def pop(
        self,
        key: AnyStr,
        default=None,
        expire_time: bool = False,
        tag: bool = False,
        retry: bool = False,
    ) -> Any:
        """Remove ``key`` and return its value, or ``default`` if it is missing.

        ``expire_time`` and ``tag`` add the same metadata as :meth:`get`.
        """
        if type(key) is not str:
            key = _encode_key(self, key)
        if expire_time or tag:
            return self._pop_meta(key, default, expire_time, tag)
        value = super().pop(key)
        if value is not None:
            return self._serializer.loads(value.memoryview())
        return default

    def _pop_meta(self, key, default, expire_time, tag):
        meta = self.expire_and_tag(key)
        value = super().pop(key)
        if value is None:
            return _meta_result(default, None, expire_time, tag)
        return _meta_result(self._serializer.loads(value.memoryview()), meta, expire_time, tag)

    def add(
        self,
        key: AnyStr,
        value: Any,
        expire: Optional[Union[timedelta, int, float]] = None,
        read: bool = False,
        tag: Optional[str] = None,
        retry: bool = False,
    ) -> bool:
        """Store ``value`` only if ``key`` is absent (atomically).

        Takes the same ``expire`` and ``tag`` as :meth:`set`.

        Returns:
            True if the value was added, False if the key already existed.
        """
        if type(key) is not str:
            key = _encode_key(self, key)
        if read:
            raise NotImplementedError("read=True (file handles) is not supported")
        if type(expire) in (int, float):
            expire = timedelta(seconds=expire)
        return super().add(
            key, _encode_value(self._serializer, value), expire=expire, tag=tag
        )

    def touch(
        self, key: AnyStr, expire: Optional[Union[timedelta, int, float]] = None, retry: bool = False
    ) -> bool:
        """Give an existing entry a new lifetime.

        Args:
            key: The key.
            expire: New lifetime, as a :class:`~datetime.timedelta` or seconds.
                None (default) makes the entry never expire.

        Returns:
            True if the entry existed and had not expired. An expired entry is not
            brought back.
        """
        if type(key) is not str:
            key = _encode_key(self, key)
        if type(expire) in (int, float):
            expire = timedelta(seconds=expire)
        return super().touch(key, expire=expire)

    def delete(self, key: AnyStr, retry: bool = False) -> bool:
        """Remove ``key``. Returns True if it was there."""
        if type(key) is not str:
            key = _encode_key(self, key)
        return super().delete(key)

    def exists(self, key: AnyStr) -> bool:
        """True if ``key`` is stored and not expired."""
        if type(key) is not str:
            key = _encode_key(self, key)
        return super().exists(key)

    def expire(self, retry: bool = False):
        """Remove every expired entry now (the background thread also does it)."""
        return super().expire()

    def evict(self, retry: bool = False):
        """Evict least-recently-used entries until the cache fits ``max_size``."""
        return super().evict()

    def clear(self, retry: bool = False):
        """Remove every entry."""
        return super().clear()

    def check(self, fix: bool = False, retry: bool = False):
        """Verify the store: SQLite integrity, dangling rows, orphaned files, sizes
        and counters. With ``fix=True``, also repair what it can.

        Returns a result with ``ok``, ``orphaned_files``, ``dangling_rows``,
        ``size_mismatches``, ``counters_consistent`` and ``sqlite_integrity_ok``.
        """
        return super().check(fix=fix)

    def incr(self, key: AnyStr, delta: int = 1, default: int = 0, retry: bool = False) -> int:
        """Increment a value in the cache by delta, returning the new value.

        If the key does not exist, sets it to default + delta.
        """
        with self.transact():
            value = self.get(key, default)
            new_value = value + delta
            self.set(key, new_value)
        return new_value

    def decr(self, key: AnyStr, delta: int = 1, default: int = 0, retry: bool = False) -> int:
        """Decrement a value in the cache by delta, returning the new value.

        If the key does not exist, sets it to default - delta.
        """
        return self.incr(key, -delta, default, retry)

    def memoize(self, expire=None, tag=None, typed=False, version_aware=False,
                ignore=(), key=None):
        """Decorator caching a function's results in this cache.

        The key is the function's module and name plus a hash of its arguments.

        Args:
            expire: Lifetime of each result, as a :class:`~datetime.timedelta` or
                seconds. None (default) never expires.
            tag: Optional tag, to remove all results at once with :meth:`evict_tag`.
            typed: Also put the argument types in the key. Arguments are hashed
                through the serializer, so ``f(1)`` and ``f(1.0)`` are separate
                entries either way; this only separates values that serialize
                identically (like diskcache).
            version_aware: Include a hash of the function's bytecode in the key, so
                changing the function invalidates its old results.
            ignore: Arguments to leave out of the key, by position (int) or name
                (str). A name matches whether the argument is passed by position
                or by keyword, so ``ignore={"session"}`` keeps a database session
                out of the key either way.
            key: A function taking the same arguments as the decorated one and
                returning what identifies the call (e.g. a file's hash instead of
                its path). It replaces the arguments in the key.

        ``async def`` functions are supported: the result is cached, not the
        coroutine.

        Example::

            @cache.memoize(expire=300)
            def expensive(x, y):
                return x + y
        """

        return lambda func: _memoize(self, func, expire, tag, typed, version_aware, ignore, key)

    def __getitem__(self, key: AnyStr):
        if type(key) is not str:
            key = _encode_key(self, key)
        value = super().get(key)
        if value is None:
            raise KeyError(_decode_key(key))
        return self._serializer.loads(value.memoryview())

    def __setitem__(self, key: AnyStr, value: Any):
        self.set(key, value)

    def __delitem__(self, key: AnyStr):
        if not self.delete(key):
            raise KeyError(key)

    def __contains__(self, key: AnyStr) -> bool:
        if type(key) is not str:
            key = _encode_key(self, key)
        return super().exists(key)

    def __iter__(self):
        return _decode_keys(self, super().iterkeys())

    def keys(self, tag=None):
        """All keys, as a list. With ``tag``, only the keys stored with that tag."""
        raw = super().keys() if tag is None else super()._keys_with_tag(tag)
        return _decode_keys(self, raw)

    def iterkeys(self, tag=None):
        """Iterate over the keys. With ``tag``, only the keys stored with that tag."""
        if tag is not None:
            return iter(self.keys(tag=tag))
        return _decode_keys(self, super().iterkeys())

    def __repr__(self) -> str:
        return f"Cache({str(super().path())!r}, count={len(self)})"

    def lock(self, key, expire=None, tag=None):
        """A cross-process lock stored in this cache, usable as a context manager.

        ``expire`` (seconds) bounds how long a crashed holder can keep it.
        """
        if type(key) is not str:
            key = _encode_key(self, key)
        return Lock(self, key, expire=expire, tag=tag)

    def get_many(self, keys) -> dict:
        """The values of the ``keys`` that are present, as a dict. Missing and
        expired keys are left out.

        All values come from one snapshot, without blocking writers: a write that
        changes several keys at once is seen entirely or not at all.
        """
        return _get_many(self, keys)

    def set_many(self, mapping, expire=None, tag=None) -> None:
        """Store every item of ``mapping`` in one transaction: all or nothing.

        ``expire`` and ``tag`` apply to every item, as in :meth:`set`.
        """
        items = _encoded_items(self, mapping)
        if type(expire) in (int, float):
            expire = timedelta(seconds=expire)
        super()._set_many(items, expire, tag)

    def delete_many(self, keys) -> int:
        """Remove ``keys`` in one transaction. Returns how many were there."""
        with self.transact():
            return sum(self.delete(k) for k in keys)

    def transact(self, retry: bool = False):
        """Context manager running a block in one transaction.

        Commits on success, rolls back on an exception. Reentrant: a nested
        ``transact()`` block joins the outer one.
        """
        return _TransactionContext(self)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class Index(_Index):

    directory = _directory

    def __init__(
        self,
        directory: str | None = None,
        *mappings,
        serializer: Serializer | None = None,
        path: str | None = None,
        max_blob_size: int | None = None,
        **items,
    ):
        effective_path = directory if directory is not None else (path if path is not None else ".index/")
        super().__init__(path=effective_path)
        _open_stores.add(self)
        self._keys_encoded = False
        self._serializer = _resolve_serializer(self, serializer)
        _apply_max_blob_size(self, max_blob_size)
        for mapping in mappings:
            self.update(mapping)
        self.update(items)

    @property
    def serializer(self) -> Serializer:
        """The serializer in use, recorded in the index on first open."""
        return self._serializer

    def set(self, key: AnyStr, value: Any, retry: bool = False):
        """Store ``value`` under ``key``."""
        if type(key) is not str:
            key = _encode_key(self, key)
        super().set(key, _encode_value(self._serializer, value))

    def get(self, key: AnyStr, default=None, retry: bool = False) -> Any:
        """The value stored under ``key``, or ``default``."""
        if type(key) is not str:
            key = _encode_key(self, key)
        value = super().get(key)
        if value is not None:
            return self._serializer.loads(value.memoryview())
        return default

    def pop(self, key: AnyStr, default=_MISSING, retry: bool = False) -> Any:
        """Remove ``key`` and return its value. Raises KeyError if it is missing,
        unless ``default`` is given.
        """
        if type(key) is not str:
            key = _encode_key(self, key)
        value = super().pop(key)
        if value is not None:
            return self._serializer.loads(value.memoryview())
        if default is _MISSING:
            raise KeyError(_decode_key(key))
        return default

    def add(self, key: AnyStr, value: Any, retry: bool = False) -> bool:
        """Store ``value`` only if ``key`` is absent. Returns True if it was added."""
        if type(key) is not str:
            key = _encode_key(self, key)
        return super().add(key, _encode_value(self._serializer, value))

    def delete(self, key: AnyStr, retry: bool = False) -> bool:
        """Remove ``key``. Returns True if it was there."""
        if type(key) is not str:
            key = _encode_key(self, key)
        return super().delete(key)

    def exists(self, key: AnyStr) -> bool:
        """True if ``key`` is stored."""
        if type(key) is not str:
            key = _encode_key(self, key)
        return super().exists(key)

    def clear(self, retry: bool = False):
        """Remove every entry."""
        return super().clear()

    def check(self, fix: bool = False, retry: bool = False):
        """Verify the store: SQLite integrity, dangling rows, orphaned files, sizes
        and counters. With ``fix=True``, also repair what it can.
        """
        return super().check(fix=fix)

    def incr(self, key: AnyStr, delta: int = 1, default: int = 0, retry: bool = False) -> int:
        """Add ``delta`` to the integer under ``key`` atomically; return the new value.

        A missing key starts at ``default``.
        """
        with self.transact():
            value = self.get(key, default)
            new_value = value + delta
            self.set(key, new_value)
        return new_value

    def decr(self, key: AnyStr, delta: int = 1, default: int = 0, retry: bool = False) -> int:
        """Subtract ``delta`` from the integer under ``key`` atomically; return the new value."""
        return self.incr(key, -delta, default, retry)

    def __getitem__(self, key: AnyStr):
        if type(key) is not str:
            key = _encode_key(self, key)
        value = super().get(key)
        if value is None:
            raise KeyError(_decode_key(key))
        return self._serializer.loads(value.memoryview())

    def __setitem__(self, key: AnyStr, value: Any):
        if type(key) is not str:
            key = _encode_key(self, key)
        super().set(key, _encode_value(self._serializer, value))

    def __delitem__(self, key: AnyStr):
        if not self.delete(key):
            raise KeyError(key)

    def __contains__(self, key: AnyStr) -> bool:
        if type(key) is not str:
            key = _encode_key(self, key)
        return super().exists(key)

    def __iter__(self):
        return _decode_keys(self, super().iterkeys())

    def keys(self):
        """All keys, as a list."""
        return _decode_keys(self, super().keys())

    def iterkeys(self):
        """Iterate over the keys without building a list."""
        return _decode_keys(self, super().iterkeys())

    def __repr__(self) -> str:
        return f"Index({str(super().path())!r}, count={len(self)})"

    def get_many(self, keys) -> dict:
        """The values of the ``keys`` that are present, as a dict, all from one
        snapshot, without blocking writers."""
        return _get_many(self, keys)

    def set_many(self, mapping) -> None:
        """Store every item of ``mapping`` in one transaction: all or nothing."""
        super()._set_many(_encoded_items(self, mapping))

    def delete_many(self, keys) -> int:
        """Remove ``keys`` in one transaction. Returns how many were there."""
        with self.transact():
            return sum(self.delete(k) for k in keys)

    def transact(self, retry: bool = False):
        """Context manager running a block in one transaction.

        Commits on success, rolls back on an exception. Reentrant.
        """
        return _TransactionContext(self)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    update = MutableMapping.update
    setdefault = MutableMapping.setdefault

    def items(self):
        """A view of the (key, value) pairs."""
        return ItemsView(self)

    def values(self):
        """A view of the values."""
        return ValuesView(self)

    def peekitem(self, last=True):
        """Order is key order, not insertion order (differs from diskcache)."""
        keys = self.keys()
        if not keys:
            raise KeyError("empty")
        k = keys[-1] if last else keys[0]
        return k, self[k]

    def popitem(self, last=True):
        """Order is key order, not insertion order (differs from diskcache)."""
        k, v = self.peekitem(last)
        del self[k]
        return k, v


class FanoutCache(_FanoutCache):

    directory = _directory

    def __init__(
        self,
        directory: str | None = None,
        shard_count: int = 8,
        max_size: int = 0,
        serializer: Serializer | None = None,
        *,
        cache_path: str | None = None,
        shards: int | None = None,
        size_limit: int | None = None,
        timeout: float | None = None,
        disk=None,
        max_blob_size: int | None = None,
        **settings,
    ):
        _reject_disk(disk)
        disk_min_file_size = settings.pop("disk_min_file_size", None)
        _reject_unknown_settings("FanoutCache", settings)
        path = directory if directory is not None else (cache_path if cache_path is not None else ".cache/")
        effective_shard_count = shards if shards is not None else shard_count
        effective_max_size = size_limit if size_limit is not None else max_size
        super().__init__(
            cache_path=path, shard_count=effective_shard_count, max_size=effective_max_size,
            timeout=600.0 if timeout is None else timeout,
        )
        _open_stores.add(self)
        self._keys_encoded = False
        self._serializer = _resolve_serializer(self, serializer)
        _apply_max_blob_size(self, max_blob_size, disk_min_file_size)

    @property
    def serializer(self) -> Serializer:
        return self._serializer

    def set(
        self,
        key: AnyStr,
        value: Any,
        expire: Optional[Union[timedelta, int, float]] = None,
        read: bool = False,
        tag: Optional[str] = None,
        retry: bool = False,
    ):
        if type(key) is not str:
            key = _encode_key(self, key)
        if read:
            raise NotImplementedError("read=True (file handles) is not supported")
        if type(expire) in (int, float):
            expire = timedelta(seconds=expire)
        super().set(key, _encode_value(self._serializer, value), expire=expire, tag=tag)
        return True

    def get(
        self,
        key: AnyStr,
        default=None,
        read: bool = False,
        expire_time: bool = False,
        tag: bool = False,
        retry: bool = False,
    ) -> Any:
        if type(key) is not str:
            key = _encode_key(self, key)
        if read or expire_time or tag:
            return self._get_meta(key, default, read, expire_time, tag)
        value = super().get(key)
        if value is not None:
            return self._serializer.loads(value.memoryview())
        return default

    def _get_meta(self, key, default, read, expire_time, tag):
        if read:
            raise NotImplementedError("read=True (file handles) is not supported")
        value = super().get(key)
        if value is None:
            return _meta_result(default, None, expire_time, tag)
        meta = self.expire_and_tag(key)
        return _meta_result(self._serializer.loads(value.memoryview()), meta, expire_time, tag)

    def pop(
        self,
        key: AnyStr,
        default=None,
        expire_time: bool = False,
        tag: bool = False,
        retry: bool = False,
    ) -> Any:
        if type(key) is not str:
            key = _encode_key(self, key)
        if expire_time or tag:
            return self._pop_meta(key, default, expire_time, tag)
        value = super().pop(key)
        if value is not None:
            return self._serializer.loads(value.memoryview())
        return default

    def _pop_meta(self, key, default, expire_time, tag):
        meta = self.expire_and_tag(key)
        value = super().pop(key)
        if value is None:
            return _meta_result(default, None, expire_time, tag)
        return _meta_result(self._serializer.loads(value.memoryview()), meta, expire_time, tag)

    def add(
        self,
        key: AnyStr,
        value: Any,
        expire: Optional[Union[timedelta, int, float]] = None,
        read: bool = False,
        tag: Optional[str] = None,
        retry: bool = False,
    ) -> bool:
        if type(key) is not str:
            key = _encode_key(self, key)
        if read:
            raise NotImplementedError("read=True (file handles) is not supported")
        if type(expire) in (int, float):
            expire = timedelta(seconds=expire)
        return super().add(key, _encode_value(self._serializer, value), expire=expire, tag=tag)

    def touch(
        self, key: AnyStr, expire: Optional[Union[timedelta, int, float]] = None, retry: bool = False
    ) -> bool:
        """Give an existing entry a new lifetime.

        Args:
            key: The key.
            expire: New lifetime, as a :class:`~datetime.timedelta` or seconds.
                None (default) makes the entry never expire.

        Returns:
            True if the entry existed and had not expired. An expired entry is not
            brought back.
        """
        if type(key) is not str:
            key = _encode_key(self, key)
        if type(expire) in (int, float):
            expire = timedelta(seconds=expire)
        return super().touch(key, expire=expire)

    def delete(self, key: AnyStr, retry: bool = False) -> bool:
        if type(key) is not str:
            key = _encode_key(self, key)
        return super().delete(key)

    def exists(self, key: AnyStr) -> bool:
        if type(key) is not str:
            key = _encode_key(self, key)
        return super().exists(key)

    def expire(self, retry: bool = False):
        return super().expire()

    def evict(self, retry: bool = False):
        return super().evict()

    def clear(self, retry: bool = False):
        return super().clear()

    def check(self, fix: bool = False, retry: bool = False):
        return super().check(fix=fix)

    def incr(self, key: AnyStr, delta: int = 1, default: int = 0, retry: bool = False) -> int:
        with self.transact(key):
            value = self.get(key, default)
            new_value = value + delta
            self.set(key, new_value)
        return new_value

    def decr(self, key: AnyStr, delta: int = 1, default: int = 0, retry: bool = False) -> int:
        return self.incr(key, -delta, default, retry)

    def __getitem__(self, key: AnyStr):
        if type(key) is not str:
            key = _encode_key(self, key)
        value = super().get(key)
        if value is None:
            raise KeyError(_decode_key(key))
        return self._serializer.loads(value.memoryview())

    def __setitem__(self, key: AnyStr, value: Any):
        self.set(key, value)

    def __delitem__(self, key: AnyStr):
        if not self.delete(key):
            raise KeyError(key)

    def __contains__(self, key: AnyStr) -> bool:
        if type(key) is not str:
            key = _encode_key(self, key)
        return super().exists(key)

    def __iter__(self):
        return _decode_keys(self, super().iterkeys())

    def keys(self, tag=None):
        raw = super().keys() if tag is None else super()._keys_with_tag(tag)
        return _decode_keys(self, raw)

    def iterkeys(self, tag=None):
        if tag is not None:
            return iter(self.keys(tag=tag))
        return _decode_keys(self, super().iterkeys())

    def __repr__(self) -> str:
        return f"FanoutCache({str(super().path())!r}, shards={self.shard_count()}, count={len(self)})"

    def memoize(self, expire=None, tag=None, typed=False, version_aware=False,
                ignore=(), key=None):
        return lambda func: _memoize(self, func, expire, tag, typed, version_aware, ignore, key)

    def lock(self, key, expire=None, tag=None):
        if type(key) is not str:
            key = _encode_key(self, key)
        return Lock(self, key, expire=expire, tag=tag)

    def get_many(self, keys) -> dict:
        """The values of the ``keys`` that are present, as a dict. Each shard is
        read from one snapshot; shards share none, so keys on different shards may
        come from different moments."""
        return _get_many(self, keys)

    def set_many(self, mapping, expire=None, tag=None) -> None:
        """Store every item of ``mapping``, in one transaction per shard. Shards
        share none, so a failing batch may keep the items of shards already
        written."""
        items = _encoded_items(self, mapping)
        if type(expire) in (int, float):
            expire = timedelta(seconds=expire)
        super()._set_many(items, expire, tag)

    def delete_many(self, keys) -> int:
        """Remove ``keys``, each on its own. Returns how many were there."""
        return sum(self.delete(k) for k in keys)

    def transact(self, key: str, retry: bool = False):
        """Context manager running a block in one transaction on the shard of ``key``.

        Commits on success, rolls back on an exception. There are no cross-shard
        transactions.
        """
        if type(key) is not str:
            key = _encode_key(self, key)
        return _TransactionContext(self, key)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class FanoutIndex(_FanoutIndex):

    directory = _directory

    def __init__(
        self,
        directory: str | None = None,
        *mappings,
        shard_count: int = 8,
        serializer: Serializer | None = None,
        path: str | None = None,
        shards: int | None = None,
        max_blob_size: int | None = None,
        **items,
    ):
        effective_path = directory if directory is not None else (path if path is not None else ".index/")
        effective_shard_count = shards if shards is not None else shard_count
        super().__init__(path=effective_path, shard_count=effective_shard_count)
        _open_stores.add(self)
        self._keys_encoded = False
        self._serializer = _resolve_serializer(self, serializer)
        _apply_max_blob_size(self, max_blob_size)
        for mapping in mappings:
            self.update(mapping)
        self.update(items)

    @property
    def serializer(self) -> Serializer:
        return self._serializer

    def set(self, key: AnyStr, value: Any, retry: bool = False):
        if type(key) is not str:
            key = _encode_key(self, key)
        super().set(key, _encode_value(self._serializer, value))

    def get(self, key: AnyStr, default=None, retry: bool = False) -> Any:
        if type(key) is not str:
            key = _encode_key(self, key)
        value = super().get(key)
        if value is not None:
            return self._serializer.loads(value.memoryview())
        return default

    def pop(self, key: AnyStr, default=_MISSING, retry: bool = False) -> Any:
        if type(key) is not str:
            key = _encode_key(self, key)
        value = super().pop(key)
        if value is not None:
            return self._serializer.loads(value.memoryview())
        if default is _MISSING:
            raise KeyError(_decode_key(key))
        return default

    def add(self, key: AnyStr, value: Any, retry: bool = False) -> bool:
        if type(key) is not str:
            key = _encode_key(self, key)
        return super().add(key, _encode_value(self._serializer, value))

    def delete(self, key: AnyStr, retry: bool = False) -> bool:
        if type(key) is not str:
            key = _encode_key(self, key)
        return super().delete(key)

    def exists(self, key: AnyStr) -> bool:
        if type(key) is not str:
            key = _encode_key(self, key)
        return super().exists(key)

    def clear(self, retry: bool = False):
        return super().clear()

    def check(self, fix: bool = False, retry: bool = False):
        return super().check(fix=fix)

    def incr(self, key: AnyStr, delta: int = 1, default: int = 0, retry: bool = False) -> int:
        with self.transact(key):
            value = self.get(key, default)
            new_value = value + delta
            self.set(key, new_value)
        return new_value

    def decr(self, key: AnyStr, delta: int = 1, default: int = 0, retry: bool = False) -> int:
        return self.incr(key, -delta, default, retry)

    def __getitem__(self, key: AnyStr):
        if type(key) is not str:
            key = _encode_key(self, key)
        value = super().get(key)
        if value is None:
            raise KeyError(_decode_key(key))
        return self._serializer.loads(value.memoryview())

    def __setitem__(self, key: AnyStr, value: Any):
        if type(key) is not str:
            key = _encode_key(self, key)
        super().set(key, _encode_value(self._serializer, value))

    def __delitem__(self, key: AnyStr):
        if not self.delete(key):
            raise KeyError(key)

    def __contains__(self, key: AnyStr) -> bool:
        if type(key) is not str:
            key = _encode_key(self, key)
        return super().exists(key)

    def __iter__(self):
        return _decode_keys(self, super().iterkeys())

    def keys(self):
        return _decode_keys(self, super().keys())

    def iterkeys(self):
        return _decode_keys(self, super().iterkeys())

    def __repr__(self) -> str:
        return f"FanoutIndex({str(super().path())!r}, shards={self.shard_count()}, count={len(self)})"

    def get_many(self, keys) -> dict:
        """The values of the ``keys`` that are present, as a dict. Each shard is
        read from one snapshot; shards share none, so keys on different shards may
        come from different moments."""
        return _get_many(self, keys)

    def set_many(self, mapping) -> None:
        """Store every item of ``mapping``, in one transaction per shard. Shards
        share none, so a failing batch may keep the items of shards already
        written."""
        super()._set_many(_encoded_items(self, mapping))

    def delete_many(self, keys) -> int:
        """Remove ``keys``, each on its own. Returns how many were there."""
        return sum(self.delete(k) for k in keys)

    def transact(self, key: str, retry: bool = False):
        """Context manager running a block in one transaction on the shard of ``key``.

        Commits on success, rolls back on an exception. There are no cross-shard
        transactions.
        """
        if type(key) is not str:
            key = _encode_key(self, key)
        return _TransactionContext(self, key)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    update = MutableMapping.update
    setdefault = MutableMapping.setdefault

    def items(self):
        return ItemsView(self)

    def values(self):
        return ValuesView(self)

    def peekitem(self, last=True):
        """Order is key order, not insertion order (differs from diskcache)."""
        keys = self.keys()
        if not keys:
            raise KeyError("empty")
        k = keys[-1] if last else keys[0]
        return k, self[k]

    def popitem(self, last=True):
        """Order is key order, not insertion order (differs from diskcache)."""
        k, v = self.peekitem(last)
        del self[k]
        return k, v


MutableMapping.register(Index)
MutableMapping.register(FanoutIndex)


def _share_docstrings(target, source):
    """Copy docstrings of methods `target` redefines without one: the fanout
    stores repeat Cache's and Index's methods, one shard at a time."""
    for name, member in vars(target).items():
        if name.startswith("_") or getattr(member, "__doc__", None):
            continue
        documented = getattr(source, name, None)
        if documented is not None and documented.__doc__:
            member.__doc__ = documented.__doc__


_share_docstrings(FanoutCache, Cache)
_share_docstrings(FanoutIndex, Index)
