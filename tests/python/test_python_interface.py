import os
import shutil
import unittest
from pysciqlop_cache import  Cache
from  tempfile import TemporaryDirectory
import time

class TestCache(unittest.TestCase):

    def setUp(self):
        """
        Set up the test environment.
        """
        self.tmp_dir = TemporaryDirectory(delete=False)
        self.cache = Cache(str(self.tmp_dir))

    def tearDown(self):
        """
        Clean up the test environment.
        """
        if hasattr(self, 'cache'):
            del self.cache
        shutil.rmtree(str(self.tmp_dir))

    def test_simple_set_get(self):
        """
        Test the Cache functionality.
        """

        key = "test_key"
        value = "test_value"

        # Test setting a value
        self.cache.set(key, value)
        assert self.cache.get(key) == value


    def test_expiration(self):
        # Test expiration
        key = "test_expire_key"
        value = "test_expire_value"
        self.cache.set(key, value, expire=1)
        time.sleep(2)
        self.cache.expire()
        assert self.cache.get(key) is None, "Cache should return None after expiration"


if __name__ == "__main__":
    unittest.main()
