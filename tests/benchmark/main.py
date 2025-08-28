import time
import random
import string
from diskcache import Cache

# Replace with your own cache
from my_custom_cache import MyCache  

def random_string(size=32):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=size))

def benchmark_cache(cache, num_items=100_000):
    keys = [random_string(16) for _ in range(num_items)]
    values = [random_string(64) for _ in range(num_items)]

    # WRITE benchmark
    start = time.perf_counter()
    for k, v in zip(keys, values):
        cache[k] = v
    write_time = time.perf_counter() - start

    # READ benchmark
    start = time.perf_counter()
    for k in keys:
        _ = cache[k]
    read_time = time.perf_counter() - start

    return write_time, read_time

if __name__ == "__main__":
    # DiskCache
    disk_cache = Cache('tmp_diskcache')
    disk_write, disk_read = benchmark_cache(disk_cache)
    disk_cache.close()

    # Your Cache
    custom_cache = MyCache()
    custom_write, custom_read = benchmark_cache(custom_cache)

    print("DiskCache:")
    print(f"  Write time: {disk_write:.2f}s")
    print(f"  Read time:  {disk_read:.2f}s")

    print("Custom Cache:")
    print(f"  Write time: {custom_write:.2f}s")
    print(f"  Read time:  {custom_read:.2f}s")
