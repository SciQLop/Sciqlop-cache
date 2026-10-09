from ._pysciqlop_cache import Cache as _Cache, Index as _Index, FanoutCache as _FanoutCache, FanoutIndex as _FanoutIndex, Timeout
import atexit
import base64
import functools
import hashlib
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
_META_SERIALIZER = "serializer"
_META_MAX_SIZE = "max_size"
_SENTINEL = object()


def _resolve_serializer(store, requested):
    """The serializer to use for `store`, recorded in its meta.

    Reopening without one uses the recorded serializer. Asking for another
    one raises, unless it can read what the recorded one wrote (an upgrade).
    """
    stored = store.get_meta(_META_SERIALIZER)
    if requested is None:
        if stored is not None:
            return get_serializer_by_name(stored)
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
    "disk_min_file_size", "disk_pickle_protocol",
})

__all__ = [
    "Cache", "Index", "FanoutCache", "FanoutIndex", "Lock", "Serializer",
    "PickleSerializer", "PickleOOBSerializer", "MsgspecSerializer", "Timeout",
]

# pickle here only encodes/decodes keys this same process wrote to its own local
# on-disk cache; it never deserializes data from another, untrusted source.
_KEY_MARK = "\x00"  # reserved prefix; a str key starting with it is not checked for on the hot path


def _typed_key(key):
    if isinstance(key, int):  # bool included, like diskcache (True stores as 1)
        return f"\x00i{int(key)}"
    if isinstance(key, float):
        return "\x00f" + repr(key)
    if isinstance(key, bytes):
        return "\x00b" + key.decode("latin-1")
    return "\x00p" + base64.b64encode(pickle.dumps(key, protocol=4)).decode("ascii")


_KEY_DECODERS = {
    "i": int,
    "f": float,
    "b": lambda s: s.encode("latin-1"),
    "p": lambda s: pickle.loads(base64.b64decode(s)),
}


def _decode_key(raw):
    if raw[:1] != _KEY_MARK:
        return raw
    return _KEY_DECODERS[raw[1]](raw[2:])


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
    return [_decode_key(k) for k in raw] if isinstance(raw, list) else map(_decode_key, raw)


def _meta_result(value, meta, expire_time, tag):
    expire, t = meta if meta is not None else (None, None)
    if expire_time and tag:
        return value, expire, t
    if expire_time:
        return value, expire
    return value, t


_open_stores = weakref.WeakSet()


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


def _reject_unknown_settings(cls_name, settings):
    for name in settings:
        if name not in _IGNORED_SETTINGS:
            raise TypeError(f"{cls_name}.__init__() got an unexpected keyword argument {name!r}")


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
        **settings,
    ):
        _reject_disk(disk)
        _reject_unknown_settings("Cache", settings)
        path = directory if directory is not None else (cache_path if cache_path is not None else ".cache/")
        if size_limit is not None:
            max_size = size_limit
        super().__init__(cache_path=path, max_size=0, timeout=600.0 if timeout is None else timeout)
        _open_stores.add(self)
        self._keys_encoded = False
        self._serializer = _resolve_serializer(self, serializer)
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

    def _memoize_key(self, base, args, kwargs, typed):
        key_data = (args, tuple(sorted(kwargs.items())))
        if typed:
            key_data += (
                tuple(type(a) for a in args),
                tuple(type(v) for v in kwargs.values()),
            )
        key_hash = hashlib.sha256(
            self._serializer.dumps(key_data)
        ).hexdigest()
        return f"{base}:{key_hash}"

    def memoize(self, expire=None, tag=None, typed=False, version_aware=False):
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

        Example::

            @cache.memoize(expire=300)
            def expensive(x, y):
                return x + y
        """

        def decorator(func):
            base = f"{func.__module__}.{func.__qualname__}"
            if version_aware:
                code_hash = hashlib.sha256(func.__code__.co_code).hexdigest()[:16]
                base = f"{base}@{code_hash}"

            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                key = self._memoize_key(base, args, kwargs, typed)
                result = self.get(key, _MISSING)
                if result is not _MISSING:
                    return result
                result = func(*args, **kwargs)
                self.set(key, result, expire=expire, tag=tag)
                return result

            wrapper.__cache_key__ = lambda *args, **kwargs: self._memoize_key(
                base, args, kwargs, typed
            )
            wrapper.__wrapped__ = func
            return wrapper

        return decorator

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

    def keys(self):
        """All keys, as a list."""
        return _decode_keys(self, super().keys())

    def iterkeys(self):
        """Iterate over the keys without building a list."""
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

    def __init__(
        self,
        directory: str | None = None,
        *mappings,
        serializer: Serializer | None = None,
        path: str | None = None,
        **items,
    ):
        effective_path = directory if directory is not None else (path if path is not None else ".index/")
        super().__init__(path=effective_path)
        _open_stores.add(self)
        self._keys_encoded = False
        self._serializer = _resolve_serializer(self, serializer)
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
        **settings,
    ):
        _reject_disk(disk)
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

    def keys(self):
        return _decode_keys(self, super().keys())

    def iterkeys(self):
        return _decode_keys(self, super().iterkeys())

    def __repr__(self) -> str:
        return f"FanoutCache({str(super().path())!r}, shards={self.shard_count()}, count={len(self)})"

    def _memoize_key(self, base, args, kwargs, typed):
        key_data = (args, tuple(sorted(kwargs.items())))
        if typed:
            key_data += (
                tuple(type(a) for a in args),
                tuple(type(v) for v in kwargs.values()),
            )
        key_hash = hashlib.sha256(
            self._serializer.dumps(key_data)
        ).hexdigest()
        return f"{base}:{key_hash}"

    def memoize(self, expire=None, tag=None, typed=False, version_aware=False):
        def decorator(func):
            base = f"{func.__module__}.{func.__qualname__}"
            if version_aware:
                code_hash = hashlib.sha256(func.__code__.co_code).hexdigest()[:16]
                base = f"{base}@{code_hash}"

            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                key = self._memoize_key(base, args, kwargs, typed)
                result = self.get(key, _MISSING)
                if result is not _MISSING:
                    return result
                result = func(*args, **kwargs)
                self.set(key, result, expire=expire, tag=tag)
                return result

            wrapper.__cache_key__ = lambda *args, **kwargs: self._memoize_key(
                base, args, kwargs, typed
            )
            wrapper.__wrapped__ = func
            return wrapper

        return decorator

    def lock(self, key, expire=None, tag=None):
        if type(key) is not str:
            key = _encode_key(self, key)
        return Lock(self, key, expire=expire, tag=tag)

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

    def __init__(
        self,
        directory: str | None = None,
        *mappings,
        shard_count: int = 8,
        serializer: Serializer | None = None,
        path: str | None = None,
        shards: int | None = None,
        **items,
    ):
        effective_path = directory if directory is not None else (path if path is not None else ".index/")
        effective_shard_count = shards if shards is not None else shard_count
        super().__init__(path=effective_path, shard_count=effective_shard_count)
        _open_stores.add(self)
        self._keys_encoded = False
        self._serializer = _resolve_serializer(self, serializer)
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
