import time
from tempfile import TemporaryDirectory
from multiprocessing import Pool
from functools import partial
from pysciqlop_cache import Cache as CustomCache
from diskcache import Cache as DiskCache

def benchmark_set_get(cache_class, cache_path, key, value, iterations=1000):
    cache = cache_class(cache_path)
    start_time = time.time()
    for _ in range(iterations):
        cache.set(key, value)
        _ = cache.get(key)
    duration = time.time() - start_time
    return duration

def parallel_set(cache_class, cache_path, key, value):
    cache = cache_class(cache_path)
    cache.set(key, value)

def parallel_get(cache_class, cache_path, key):
    cache = cache_class(cache_path)
    return cache.get(key)

def benchmark_multiprocess(cache_class, cache_path, key, value, processes=10):
    cache = cache_class(cache_path)
    cache.set(key, value)

    with Pool(processes=processes) as pool:
        get_func = partial(parallel_get, cache_class, cache_path, key)
        start_get = time.time()
        results = pool.map(lambda _: get_func(), range(processes))
        duration_get = time.time() - start_get

        set_func = partial(parallel_set, cache_class, cache_path, key, value)
        start_set = time.time()
        pool.map(lambda _: set_func(), range(processes))
        duration_set = time.time() - start_set

    return duration_get, duration_set

def run_comparison():
    key = "bench_key"
    value = "bench_value"
    iterations = 1000
    processes = 20

    with TemporaryDirectory() as tmpdir_custom, TemporaryDirectory() as tmpdir_disk:
        print("=== Single-threaded Benchmark ===")
        time_custom = benchmark_set_get(CustomCache, tmpdir_custom, key, value, iterations)
        time_disk = benchmark_set_get(DiskCache, tmpdir_disk, key, value, iterations)
        print(f"Custom Cache: {time_custom:.4f} seconds")
        print(f"DiskCache: {time_disk:.4f} seconds")

        print("\n=== Multiprocess Benchmark ===")
        get_time_custom, set_time_custom = benchmark_multiprocess(CustomCache, tmpdir_custom, key, value, processes)
        get_time_disk, set_time_disk = benchmark_multiprocess(DiskCache, tmpdir_disk, key, value, processes)
        print(f"Custom Cache GET: {get_time_custom:.4f} sec")
        print(f"DiskCache GET: {get_time_disk:.4f} sec")
        print(f"Custom Cache SET: {set_time_custom:.4f} sec")
        print(f"DiskCache SET: {set_time_disk:.4f} sec")

if __name__ == "__main__":
    run_comparison()
