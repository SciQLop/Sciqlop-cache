from ._pysciqlop_cache import Cache as _Cache
import  pickle
from  datetime import timedelta
from typing import Any, AnyStr, Optional, Union

__all__ = ["Cache"]


class Cache(_Cache):

    def set(self, key:AnyStr, value:Any, expire:Optional[Union[timedelta, int, float]]=None):
        """
        Set a value in the cache with an optional expiration time.

        Parameters:
        key (str): The key under which to store the value.
        value: The value to store in the cache.
        expire (Optional[Union[timedelta, int, float]]): The expiration time for the cache entry. If provided, it can be a `timedelta`, an integer (seconds), or a float (seconds). If `None`, the entry will not expire.
        """
        if type(expire) in (int, float):
            expire = timedelta(seconds=expire)
        super().set(key, pickle.dumps(value), expire=expire or timedelta(seconds=36000))  # Default to 1 hour if no expire is set

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
            return pickle.loads(value)
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
            return pickle.loads(value)
        return default

    def add(self, key:AnyStr, value:Any, expire:Optional[Union[timedelta, int, float]]=None) -> bool:
        """
        Add a value to the cache if the key does not already exist.
        Parameters:
        key (str): The key under which to store the value.
        value: The value to store in the cache.
        Returns:
        bool: `True` if the value was added, `False` if the key already exists.
        """
        if type(expire) in (int, float):
            expire = timedelta(seconds=expire)
        super().add(key, pickle.dumps(value),
                    expire=expire or timedelta(seconds=36000))  # Default to 1 hour if no expire is set

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
