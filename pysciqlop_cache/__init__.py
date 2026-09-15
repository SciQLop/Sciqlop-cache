from ._pysciqlop_cache import Cache as _Cache, Index as _Index, FanoutCache as _FanoutCache, FanoutIndex as _FanoutIndex, Timeout
import base64
import functools
import hashlib
import pickle
import time
from collections.abc import ItemsView, MutableMapping, ValuesView
from datetime import timedelta
from typing import Any, AnyStr, Optional, Union

from .serializers import (
    MsgspecSerializer,
    PickleSerializer,
    Serializer,
    get_serializer_by_name,
)

_MISSING = object()
_META_SERIALIZER = "serializer"
_META_MAX_SIZE = "max_size"
_SENTINEL = object()

# diskcache tuning knobs (SQLite pragmas, eviction policy, statistics, ...) with no
# equivalent here; accepted and ignored so diskcache configs drop in unchanged.
_IGNORED_SETTINGS = frozenset({
    "statistics", "tag_index", "eviction_policy", "cull_limit", "sqlite_auto_vacuum",
    "sqlite_cache_size", "sqlite_journal_mode", "sqlite_mmap_size", "sqlite_synchronous",
    "disk_min_file_size", "disk_pickle_protocol",
})

__all__ = [
    "Cache", "Index", "FanoutCache", "FanoutIndex", "Lock", "Serializer",
    "PickleSerializer", "MsgspecSerializer", "Timeout",
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
        self._keys_encoded = False
        self._serializer = self._resolve_meta(
            _META_SERIALIZER, serializer,
            default=PickleSerializer,
            to_str=lambda s: s.name,
            from_str=get_serializer_by_name,
            mismatch_ok=False,
        )
        effective_max_size = self._resolve_meta(
            _META_MAX_SIZE, max_size if max_size is not _SENTINEL else None,
            default=lambda: 0,
            to_str=str,
            from_str=int,
            mismatch_ok=True,
        )
        if effective_max_size != 0:
            super().set_max_cache_size(effective_max_size)

    def _resolve_meta(self, key, explicit, *, default, to_str, from_str, mismatch_ok):
        stored = super().get_meta(key)
        if explicit is not None:
            if stored is not None and to_str(explicit) != stored and not mismatch_ok:
                raise ValueError(
                    f"Cache metadata {key!r} is {stored!r}, "
                    f"but {to_str(explicit)!r} was requested."
                )
            super().set_meta(key, to_str(explicit))
            return explicit
        if stored is not None:
            return from_str(stored)
        value = default()
        super().set_meta(key, to_str(value))
        return value

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
        """Set a value in the cache with an optional expiration time and tag.

        Parameters:
        key (str): The key under which to store the value.
        value: The value to store in the cache.
        expire (Optional[Union[timedelta, int, float]]): Expiration time.
            Can be a `timedelta`, an integer (seconds), or a float (seconds).
            If `None`, the entry will not expire.
        tag (Optional[str]): Optional tag for grouping cache entries.
            Use `evict_tag()` to bulk-remove entries by tag.
        """
        if type(key) is not str:
            key = _encode_key(self, key)
        if read:
            raise NotImplementedError("read=True (file handles) is not supported")
        if type(expire) in (int, float):
            expire = timedelta(seconds=expire)
        super().set(key, self._serializer.dumps(value), expire=expire, tag=tag)
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
        """Get a value from the cache.

        Parameters:
        key (str): The key of the value to retrieve.
        Returns:
        Any: The value, or `default` if the key does not exist or has expired.
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
        """Remove a value from the cache and return it.

        Parameters:
        key (str): The key of the value to remove.
        Returns:
        Any: The value, or `default` if the key does not exist.
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
        """Add a value to the cache if the key does not already exist.

        Parameters:
        key (str): The key under which to store the value.
        value: The value to store in the cache.
        tag (Optional[str]): Optional tag for grouping cache entries.
        Returns:
        bool: `True` if the value was added, `False` if the key already exists.
        """
        if type(key) is not str:
            key = _encode_key(self, key)
        if read:
            raise NotImplementedError("read=True (file handles) is not supported")
        if type(expire) in (int, float):
            expire = timedelta(seconds=expire)
        return super().add(
            key, self._serializer.dumps(value), expire=expire, tag=tag
        )

    def touch(
        self, key: AnyStr, expire: Optional[Union[timedelta, int, float]] = None, retry: bool = False
    ) -> bool:
        """Update the expiration time of an existing entry.

        Parameters:
        key (str): The key of the entry to touch.
        expire (Optional[Union[timedelta, int, float]]): New expiration time.
            Can be a `timedelta`, an integer (seconds), or a float (seconds).
            If `None` (default), the entry will no longer expire.
        Returns:
        bool: `True` if the entry existed and was not expired, `False` otherwise.
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
        """Decorator to memoize function results in cache.

        Parameters:
        expire: Expiration time (timedelta, int seconds, or float seconds).
            None = no expiry.
        tag (str): Optional tag for bulk eviction of memoized entries.
        typed (bool): If True, arguments of different types are cached
            separately (e.g. f(1) and f(1.0) get different cache entries).
        version_aware (bool): If True, the function's bytecode is included
            in the cache key so that implementation changes automatically
            invalidate cached results.

        Usage:
            cache = Cache()

            @cache.memoize()
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
        return _decode_keys(self, super().keys())

    def iterkeys(self):
        return _decode_keys(self, super().iterkeys())

    def __repr__(self) -> str:
        return f"Cache({str(super().path())!r}, count={len(self)})"

    def lock(self, key, expire=None, tag=None):
        if type(key) is not str:
            key = _encode_key(self, key)
        return Lock(self, key, expire=expire, tag=tag)

    def transact(self, retry: bool = False):
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
        self._keys_encoded = False
        stored = super().get_meta(_META_SERIALIZER)
        if serializer is not None:
            if stored is not None and serializer.name != stored:
                raise ValueError(
                    f"Index metadata {_META_SERIALIZER!r} is {stored!r}, "
                    f"but {serializer.name!r} was requested."
                )
            super().set_meta(_META_SERIALIZER, serializer.name)
            self._serializer = serializer
        elif stored is not None:
            self._serializer = get_serializer_by_name(stored)
        else:
            self._serializer = PickleSerializer()
            super().set_meta(_META_SERIALIZER, self._serializer.name)
        for mapping in mappings:
            self.update(mapping)
        self.update(items)

    @property
    def serializer(self) -> Serializer:
        return self._serializer

    def set(self, key: AnyStr, value: Any, retry: bool = False):
        if type(key) is not str:
            key = _encode_key(self, key)
        super().set(key, self._serializer.dumps(value))

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
        return super().add(key, self._serializer.dumps(value))

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
        with self.transact():
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
        super().set(key, self._serializer.dumps(value))

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
        return f"Index({str(super().path())!r}, count={len(self)})"

    def transact(self, retry: bool = False):
        return _TransactionContext(self)

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
        self._keys_encoded = False
        self._serializer = serializer or PickleSerializer()

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
        super().set(key, self._serializer.dumps(value), expire=expire, tag=tag)
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
        return super().add(key, self._serializer.dumps(value), expire=expire, tag=tag)

    def touch(
        self, key: AnyStr, expire: Optional[Union[timedelta, int, float]] = None, retry: bool = False
    ) -> bool:
        """Update the expiration time of an existing entry.

        Parameters:
        key (str): The key of the entry to touch.
        expire (Optional[Union[timedelta, int, float]]): New expiration time.
            Can be a `timedelta`, an integer (seconds), or a float (seconds).
            If `None` (default), the entry will no longer expire.
        Returns:
        bool: `True` if the entry existed and was not expired, `False` otherwise.
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
        """Decorator to memoize function results in cache.

        See Cache.memoize for full documentation.
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

    def lock(self, key, expire=None, tag=None):
        if type(key) is not str:
            key = _encode_key(self, key)
        return Lock(self, key, expire=expire, tag=tag)

    def transact(self, key: str, retry: bool = False):
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
        self._keys_encoded = False
        self._serializer = serializer or PickleSerializer()
        for mapping in mappings:
            self.update(mapping)
        self.update(items)

    @property
    def serializer(self) -> Serializer:
        return self._serializer

    def set(self, key: AnyStr, value: Any, retry: bool = False):
        if type(key) is not str:
            key = _encode_key(self, key)
        super().set(key, self._serializer.dumps(value))

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
        return super().add(key, self._serializer.dumps(value))

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
        super().set(key, self._serializer.dumps(value))

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
