from ._pysciqlop_cache import Cache as _Cache
import functools
import hashlib
import pickle
from datetime import timedelta
from typing import Any, AnyStr, Optional, Union

_MISSING = object()

__all__ = ["Cache"]


class Cache(_Cache):

    def __init__(self, cache_path=".cache/", max_size=0):
        super().__init__(cache_path=cache_path, max_size=max_size)
        self._pickle_protocol = pickle.HIGHEST_PROTOCOL

    @property
    def pickle_protocol(self):
        return self._pickle_protocol

    @pickle_protocol.setter
    def pickle_protocol(self, value):
        self._pickle_protocol = value

    def set(self, key:AnyStr, value:Any, expire:Optional[Union[timedelta, int, float]]=None,
            tag:Optional[str]=None):
        """
        Set a value in the cache with an optional expiration time and tag.

        Parameters:
        key (str): The key under which to store the value.
        value: The value to store in the cache.
        expire (Optional[Union[timedelta, int, float]]): The expiration time for the cache entry. If provided, it can be a `timedelta`, an integer (seconds), or a float (seconds). If `None`, the entry will not expire.
        tag (Optional[str]): An optional tag for grouping cache entries. Use `evict_tag()` to bulk-remove entries by tag.
        """
        if type(expire) in (int, float):
            expire = timedelta(seconds=expire)
        super().set(key, pickle.dumps(value, self._pickle_protocol), expire=expire, tag=tag)

    def get(self, key:AnyStr, default=None) -> Any:
        """
        Get a value from the cache.
        Parameters:
        key (str): The key of the value to retrieve.
        Returns:
        Any: The value associated with the key, or `None` if the key does not exist or has expired.
        """
        value = super().get(key)
        if value is not None:
            return pickle.loads(value.memoryview())
        return default

    def pop(self, key:AnyStr, default=None) -> Any:
        """
        Remove a value from the cache and return it.
        Parameters:
        key (str): The key of the value to remove.
        Returns:
        Any: The value associated with the key, or `default` if the key does not exist.
        """
        value = super().pop(key)
        if value is not None:
            return pickle.loads(value.memoryview())
        return default

    def add(self, key:AnyStr, value:Any, expire:Optional[Union[timedelta, int, float]]=None,
            tag:Optional[str]=None) -> bool:
        """
        Add a value to the cache if the key does not already exist.
        Parameters:
        key (str): The key under which to store the value.
        value: The value to store in the cache.
        tag (Optional[str]): An optional tag for grouping cache entries.
        Returns:
        bool: `True` if the value was added, `False` if the key already exists.
        """
        if type(expire) in (int, float):
            expire = timedelta(seconds=expire)
        return super().add(key, pickle.dumps(value, self._pickle_protocol), expire=expire, tag=tag)

    def incr(self, key:AnyStr, delta:int=1, default:int=0) -> int:
        """
        Increment a value in the cache by delta, returning the new value.
        If the key does not exist, sets it to default + delta.

        Parameters:
        key (str): The key to increment.
        delta (int): The amount to increment by (default 1).
        default (int): The default value if the key does not exist (default 0).
        Returns:
        int: The new value after incrementing.
        """
        value = self.get(key, default)
        new_value = value + delta
        self.set(key, new_value)
        return new_value

    def decr(self, key:AnyStr, delta:int=1, default:int=0) -> int:
        """
        Decrement a value in the cache by delta, returning the new value.
        If the key does not exist, sets it to default - delta.

        Parameters:
        key (str): The key to decrement.
        delta (int): The amount to decrement by (default 1).
        default (int): The default value if the key does not exist (default 0).
        Returns:
        int: The new value after decrementing.
        """
        return self.incr(key, -delta, default)

    def _memoize_key(self, base, args, kwargs, typed):
        key_data = (args, tuple(sorted(kwargs.items())))
        if typed:
            key_data += (tuple(type(a) for a in args),
                         tuple(type(v) for v in kwargs.values()))
        key_hash = hashlib.sha256(pickle.dumps(key_data)).hexdigest()
        return f"{base}:{key_hash}"

    def memoize(self, expire=None, tag=None, typed=False):
        """
        Decorator to memoize function results in cache.

        Parameters:
        expire: Expiration time (timedelta, int seconds, or float seconds). None = no expiry.
        tag (str): Optional tag for bulk eviction of memoized entries.
        typed (bool): If True, arguments of different types are cached separately
            (e.g. f(1) and f(1.0) get different cache entries).

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
                base, args, kwargs, typed)
            wrapper.__wrapped__ = func
            return wrapper
        return decorator

    def __getitem__(self, key:AnyStr):
        """
        Get a value from the cache using the indexing operator.
        Parameters:
        key (str): The key of the value to retrieve.
        Returns:
        Any: The value associated with the key, or `None` if the key does not exist or has expired.
        """
        return self.get(key)

    def __setitem__(self, key:AnyStr, value:Any):
        """
        Set a value in the cache using the indexing operator.
        Parameters:
        key (str): The key under which to store the value.
        value: The value to store in the cache.
        """
        self.set(key, value)

    def __delitem__(self, key:AnyStr):
        """
        Delete a value from the cache using the indexing operator.
        Parameters:
        key (str): The key of the value to delete.
        """
        super().delete(key)
