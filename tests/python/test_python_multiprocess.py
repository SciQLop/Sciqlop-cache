import shutil
import unittest
from pysciqlop_cache import Cache
from tempfile import TemporaryDirectory
from multiprocessing import Pool
from functools import partial


def get_cache_value(path, key):
    cache = Cache(path)
    return cache.get(key)


def set_cache_value(path, key, value):
    cache = Cache(path)
    cache.set(key, value)


def locked_increment(path, n):
    cache = Cache(path)
    for _ in range(n):
        with cache.lock("counter_lock"):
            val = cache.get("counter", 0)
            cache.set("counter", val + 1)


class TestMultiProcessCache(unittest.TestCase):

    def setUp(self):
        self.tmp_dir = TemporaryDirectory(delete=False)

    def tearDown(self):
        shutil.rmtree(self.tmp_dir.name)

    def test_multiprocess_cache_get(self):
        cache = Cache(self.tmp_dir.name)
        cache.set("shared_key", "shared_value")
        del cache

        with Pool(processes=2) as pool:
            results = pool.map(
                partial(get_cache_value, self.tmp_dir.name),
                ["shared_key"] * 4,
            )

        assert all(
            result == "shared_value" for result in results
        ), "Cache values should match across processes"

    def test_multiprocess_cache_set(self):
        with Pool(processes=2) as pool:
            pool.map(
                partial(set_cache_value, self.tmp_dir.name, "shared_key"),
                ["shared_value"] * 4,
            )

        cache = Cache(self.tmp_dir.name)
        assert (
            cache.get("shared_key") == "shared_value"
        ), "Cache value should be 'shared_value'"


    def test_multiprocess_lock_prevents_lost_updates(self):
        cache = Cache(self.tmp_dir.name)
        cache.set("counter", 0)
        del cache

        n_processes = 4
        increments_per_process = 50
        with Pool(processes=n_processes) as pool:
            pool.starmap(
                locked_increment,
                [(self.tmp_dir.name, increments_per_process)] * n_processes,
            )

        cache = Cache(self.tmp_dir.name)
        self.assertEqual(
            cache.get("counter"),
            n_processes * increments_per_process,
        )


if __name__ == "__main__":
    unittest.main()
