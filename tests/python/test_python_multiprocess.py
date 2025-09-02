import os
import shutil
import unittest
from pysciqlop_cache import  Cache
from  tempfile import TemporaryDirectory
from multiprocessing import Pool
from functools import partial

def get_cache_value(path, key):
    """
    Function to get a value from the cache.
    """
    cache = Cache(path)
    return cache.get(key)

def set_cache_value(path, key, value):
    """
    Function to set a value in the cache.
    """
    cache = Cache(path)
    cache.set(key, value)


class TestMultiProcessCache(unittest.TestCase):
    """
    Test the Cache functionality in a multi-process environment.
    """

    def setUp(self):
        """
        Set up the test environment.
        """
        self.tmp_dir = TemporaryDirectory(delete=False)
        self.cache = Cache(self.tmp_dir.name)

    def tearDown(self):
        """
        Clean up the test environment.
        """
        if hasattr(self, 'cache'):
            del self.cache
        shutil.rmtree(self.tmp_dir.name)

    def test_multiprocess_cache_get(self):
        """
        Test the Cache functionality in a multi-process environment.
        """
        with Pool(processes=20) as pool:
            # Set a value in the cache
            self.cache.set("shared_key", "shared_value")

            # Define a function to get the value from the cache

            # Use the pool to get the value from the cache in parallel
            results = pool.map(partial(get_cache_value, self.tmp_dir.name), ["shared_key"] * 20)

            # Check that both processes got the same value
            assert all(result == "shared_value" for result in results), "Cache values should match across processes"

    def test_multiprocess_cache_set(self):
        """
        Test the Cache functionality in a multi-process environment.
        """
        with Pool(processes=20) as pool:
            # Use the pool to set the value in the cache in parallel
            pool.map(partial(set_cache_value, self.tmp_dir.name, "shared_key"), ["shared_value"] * 20)

            # Check that the value was set correctly
            assert self.cache.get("shared_key") == "shared_value", "Cache value should be 'shared_value'"


if __name__ == "__main__":
    unittest.main()
