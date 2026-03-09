import shutil
import timeit
import unittest
from tempfile import TemporaryDirectory

try:
    import diskcache
    HAS_DISKCACHE = True
except ImportError:
    HAS_DISKCACHE = False

from pysciqlop_cache import Cache


def bench(fn, number=2000, warmup=500):
    for _ in range(warmup):
        fn()
    return timeit.timeit(fn, number=number) / number


@unittest.skipUnless(HAS_DISKCACHE, "diskcache not installed")
class TestPerfVsDiskcache(unittest.TestCase):

    MARGIN = 2.0  # fail if sciqlop-cache is more than 2x slower

    def setUp(self):
        self.sq_dir = TemporaryDirectory(delete=False)
        self.dc_dir = TemporaryDirectory(delete=False)
        self.sq_cache = Cache(self.sq_dir.name)
        self.dc_cache = diskcache.Cache(self.dc_dir.name)
        self.key = "bench/key/test"
        self.value = b"x" * 200

    def tearDown(self):
        del self.sq_cache
        self.dc_cache.close()
        shutil.rmtree(self.sq_dir.name, ignore_errors=True)
        shutil.rmtree(self.dc_dir.name, ignore_errors=True)

    def test_set_not_slower_than_diskcache(self):
        key, value = self.key, self.value

        sq = self.sq_cache
        dc = self.dc_cache

        sq_time = bench(lambda: sq.set(key, value))
        dc_time = bench(lambda: dc.set(key, value))

        print(f"\nset: sciqlop={sq_time*1e6:.1f}μs  diskcache={dc_time*1e6:.1f}μs  "
              f"ratio={sq_time/dc_time:.2f}x")
        self.assertLess(sq_time, dc_time * self.MARGIN,
                        f"set() is {sq_time/dc_time:.1f}x slower than diskcache")

    def test_get_not_slower_than_diskcache(self):
        key, value = self.key, self.value

        sq = self.sq_cache
        dc = self.dc_cache
        sq.set(key, value)
        dc.set(key, value)

        sq_time = bench(lambda: sq.get(key))
        dc_time = bench(lambda: dc.get(key))

        print(f"\nget: sciqlop={sq_time*1e6:.1f}μs  diskcache={dc_time*1e6:.1f}μs  "
              f"ratio={sq_time/dc_time:.2f}x")
        self.assertLess(sq_time, dc_time * self.MARGIN,
                        f"get() is {sq_time/dc_time:.1f}x slower than diskcache")


if __name__ == "__main__":
    unittest.main()
