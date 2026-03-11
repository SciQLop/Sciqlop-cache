import os
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

def benchmark_batch_set(cache, key, value, iterations=1000):
    keys = [f"{key}_{i}" for i in range(100)]
    for _ in range(iterations):
        with cache.transact():
            for k in keys:
                cache.set(k, value)

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


def benchmark_by_value_size(func):
    sizes = [200, 4096, 16*1024, 64*1024, 256*1024, 1024*1024]
    print(f"\n{'='*70}")
    print(f"Value-size benchmark for: {func.__name__}")
    print(f"{'='*70}")
    print(f"{'Size':>12s}  {'sciqlop (μs)':>14s}  {'diskcache (μs)':>14s}  {'ratio':>8s}")
    print(f"{'-'*12}  {'-'*14}  {'-'*14}  {'-'*8}")

    for sz in sizes:
        value = os.urandom(sz)
        iterations = max(10, 1000 // max(1, sz // 1024))
        with TemporaryDirectory() as tmp:
            sq_t = _benchmark(func, SciqlopCache, tmp, "test_key", value, iterations=iterations)
        with TemporaryDirectory() as tmp:
            dc_t = _benchmark(func, DiskCache, tmp, "test_key", value, iterations=iterations)
        ratio = sq_t / dc_t if dc_t > 0 else float('inf')
        print(f"{humanize.naturalsize(sz):>12s}  {sq_t*1e6:>14.1f}  {dc_t*1e6:>14.1f}  {ratio:>7.2f}x")


def benchmark_batch():
    print(f"\n{'='*70}")
    print("Batch benchmark: 100 set() ops per transaction")
    print(f"{'='*70}")
    print(f"{'Value size':>12s}  {'sciqlop (μs)':>14s}  {'diskcache (μs)':>14s}  {'ratio':>8s}")
    print(f"{'-'*12}  {'-'*14}  {'-'*14}  {'-'*8}")

    for sz in [200, 16*1024, 64*1024]:
        value = os.urandom(sz)
        iterations = max(5, 200 // max(1, sz // 1024))

        with TemporaryDirectory() as tmp:
            sq_t = _benchmark(benchmark_batch_set, SciqlopCache, tmp, "batch", value, iterations=iterations)
        with TemporaryDirectory() as tmp:
            dc_t = _benchmark(benchmark_batch_set, DiskCache, tmp, "batch", value, iterations=iterations)
        ratio = sq_t / dc_t if dc_t > 0 else float('inf')
        print(f"{humanize.naturalsize(sz):>12s}  {sq_t*1e6:>14.1f}  {dc_t*1e6:>14.1f}  {ratio:>7.2f}x")


if __name__ == "__main__":
    benchmark(benchmark_set_get)
    benchmark(benchmark_get)
    benchmark(benchmark_set)
    benchmark_by_value_size(benchmark_set)
    benchmark_by_value_size(benchmark_get)
    benchmark_batch()
