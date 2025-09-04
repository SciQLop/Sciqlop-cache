import time
from tempfile import TemporaryDirectory
from pysciqlop_cache import Cache as SciqlopCache
from diskcache import Cache as DiskCache
import humanize
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

def benchmark_get(cache, key, value, iterations=1000):
    cache.set(key, value)
    for _ in range(iterations):
        _ = cache.get(key)

def benchmark_set(cache, key, value, iterations=1000):
    for _ in range(iterations):
        _ = cache.set(key, value)

def _benchmark(func, cache_type, cache_path, *args, iterations=1000, **kwargs):
    cache = cache_type(cache_path)
    return timed(func, cache, *args, **kwargs, iterations=iterations)/iterations


def benchmark(func):
    pysciqlop_time = []
    diskcache_time = []
    print("Starting benchmarks for function:", func.__name__)
    for repeat, size in [(100,10), (100,100), (10,1000), (10,10000)]:
        print(f"Benchmarking with array size: {size}x{size}")
        value = np.random.rand(size, size)
        with TemporaryDirectory() as temp_dir:
            pysciqlop_time.append((value.nbytes, _benchmark(func, SciqlopCache, temp_dir, "test_key", value, iterations=repeat)))
        with TemporaryDirectory() as temp_dir:
            diskcache_time.append((value.nbytes, _benchmark(func, DiskCache, temp_dir, "test_key", value, iterations=repeat)))

    print("\nSciqlop Cache Times (elements, time in seconds):")
    for size, t in pysciqlop_time:
        print(f"{humanize.naturalsize(size)}, {humanize.naturaldelta(t, minimum_unit='microseconds')} {humanize.naturalsize(size/t)}/s ({size} Bytes, {t:.6f} seconds)")

    print("\nDiskCache Times (elements, time in seconds):")
    for size, t in diskcache_time:
        print(f"{humanize.naturalsize(size)}, {humanize.naturaldelta(t, minimum_unit='microseconds')} {humanize.naturalsize(size/t)}/s ({size} Bytes, {t:.6f} seconds)")

    print("\nBenchmarking completed.")

if __name__ == "__main__":
    benchmark(benchmark_set_get)
    benchmark(benchmark_get)
    benchmark(benchmark_set)
