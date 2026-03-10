from ._pysciqlop_cache import Cache as _Cache
import functools
import hashlib
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

__all__ = ["Cache", "Serializer", "PickleSerializer", "MsgspecSerializer"]


class Cache(_Cache):

    def __init__(
        self,
        cache_path: str = ".cache/",
        max_size: int | object = _SENTINEL,
        serializer: Serializer | None = None,
    ):
        super().__init__(cache_path=cache_path, max_size=0)
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
        tag: Optional[str] = None,
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
        if type(expire) in (int, float):
            expire = timedelta(seconds=expire)
        super().set(key, self._serializer.dumps(value), expire=expire, tag=tag)

    def get(self, key: AnyStr, default=None) -> Any:
        """Get a value from the cache.

        Parameters:
        key (str): The key of the value to retrieve.
        Returns:
        Any: The value, or `default` if the key does not exist or has expired.
        """
        value = super().get(key)
        if value is not None:
            return self._serializer.loads(value.memoryview())
        return default

    def pop(self, key: AnyStr, default=None) -> Any:
        """Remove a value from the cache and return it.

        Parameters:
        key (str): The key of the value to remove.
        Returns:
        Any: The value, or `default` if the key does not exist.
        """
        value = super().pop(key)
        if value is not None:
            return self._serializer.loads(value.memoryview())
        return default

    def add(
        self,
        key: AnyStr,
        value: Any,
        expire: Optional[Union[timedelta, int, float]] = None,
        tag: Optional[str] = None,
    ) -> bool:
        """Add a value to the cache if the key does not already exist.

        Parameters:
        key (str): The key under which to store the value.
        value: The value to store in the cache.
        tag (Optional[str]): Optional tag for grouping cache entries.
        Returns:
        bool: `True` if the value was added, `False` if the key already exists.
        """
        if type(expire) in (int, float):
            expire = timedelta(seconds=expire)
        return super().add(
            key, self._serializer.dumps(value), expire=expire, tag=tag
        )

    def incr(self, key: AnyStr, delta: int = 1, default: int = 0) -> int:
        """Increment a value in the cache by delta, returning the new value.

        If the key does not exist, sets it to default + delta.
        """
        value = self.get(key, default)
        new_value = value + delta
        self.set(key, new_value)
        return new_value

    def decr(self, key: AnyStr, delta: int = 1, default: int = 0) -> int:
        """Decrement a value in the cache by delta, returning the new value.

        If the key does not exist, sets it to default - delta.
        """
        return self.incr(key, -delta, default)

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

    def memoize(self, expire=None, tag=None, typed=False):
        """Decorator to memoize function results in cache.

        Parameters:
        expire: Expiration time (timedelta, int seconds, or float seconds).
            None = no expiry.
        tag (str): Optional tag for bulk eviction of memoized entries.
        typed (bool): If True, arguments of different types are cached
            separately (e.g. f(1) and f(1.0) get different cache entries).

        Usage:
            cache = Cache()

            @cache.memoize()
            def expensive(x, y):
                return x + y
        """

        def decorator(func):
            base = f"{func.__module__}.{func.__qualname__}"

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
        return self.get(key)

    def __setitem__(self, key: AnyStr, value: Any):
        self.set(key, value)

    def __delitem__(self, key: AnyStr):
        super().delete(key)

    def __contains__(self, key: AnyStr) -> bool:
        return super().exists(key)

    def __iter__(self):
        return iter(super().keys())

    def __repr__(self) -> str:
        return f"Cache({str(super().path())!r}, count={len(self)})"

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False
