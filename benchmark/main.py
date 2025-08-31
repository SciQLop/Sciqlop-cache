import time
from tempfile import TemporaryDirectory
from pysciqlop_cache import Cache as SciqlopCache
from diskcache import Cache as DiskCache
import numpy as np

def timed(func, *args, **kwargs):
    start_time = time.time()
    func(*args, **kwargs)
    end_time = time.time()
    return (end_time - start_time)


def benchmark_set_get(cache, key, value, iterations=1000):
    for _ in range(iterations):
        cache.set(key, value)
        _ = cache.get(key)

def pysciqlop_cache_get_set_key(cache_path, key, value, iterations=1000):
    cache = SciqlopCache(cache_path)
    return timed(benchmark_set_get, cache, key, value, iterations)

def diskcache_get_set_key(cache_path, key, value, iterations=1000):
    cache = DiskCache(cache_path)
    return timed(benchmark_set_get, cache, key, value, iterations)


if __name__ == "__main__":
    pysciqlop_time = []
    diskcache_time = []
    for size in [10, 100, 1000]:
        print(f"Benchmarking with array size: {size}x{size}")
        value = np.random.rand(size, size)
        pysciqlop_time.append((size*size, pysciqlop_cache_get_set_key(TemporaryDirectory().name, "test_key", value, 100)))
        diskcache_time.append((size*size, diskcache_get_set_key(TemporaryDirectory().name, "test_key", value, 100)))

    print("\nSciqlop Cache Times (elements, time in seconds):")
    for size, t in pysciqlop_time:
        print(f"{size}, {t:.6f}")

    print("\nDiskCache Times (elements, time in seconds):")
    for size, t in diskcache_time:
        print(f"{size}, {t:.6f}")
