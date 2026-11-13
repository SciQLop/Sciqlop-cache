#!/usr/bin/env python3
"""Value-size and batch benchmark: compare sciqlop-cache vs diskcache across value sizes.

Outputs CSV to stdout.
Usage:
    PYTHONPATH=build python benchmark/bench_valuesize.py > benchmark/valuesize_results.csv
"""

import csv
import os
import sys
import timeit
from tempfile import TemporaryDirectory

from pysciqlop_cache import Cache as SciqlopCache
from diskcache import Cache as DiskCache

VALUE_SIZES = [64, 200, 1024, 4096, 8192, 16384, 65536, 262144, 1048576]
BATCH_SIZES = [1, 10, 50, 100, 500]
BATCH_VALUE_SIZES = [200, 16384, 65536]
SIZE_LIMIT = 1 << 30  # diskcache's default size_limit

SINGLE_OP_BACKENDS = [
    ("sciqlop", SciqlopCache),
    ("sciqlop (size limit)", lambda path: SciqlopCache(path, max_size=SIZE_LIMIT)),
    ("diskcache", DiskCache),
]


def measure(fn, number):
    return timeit.timeit(fn, number=number) / number * 1e6  # μs


def measure_first_get(cache, value, number):
    """Each key read once: what a get() costs before any in-process cache has seen it."""
    keys = [f"first_{i}" for i in range(number)]
    for k in keys:
        cache.set(k, value)
    it = iter(keys)
    return measure(lambda: cache.get(next(it)), number)


def bench_single_ops(writer):
    """Benchmark individual set/get across value sizes."""
    for sz in VALUE_SIZES:
        value = os.urandom(sz)
        iterations = max(20, 2000 // max(1, sz // 1024))

        for name, factory in SINGLE_OP_BACKENDS:
            with TemporaryDirectory() as tmp:
                cache = factory(tmp)
                key = "bench_key"

                # warmup
                for _ in range(min(50, iterations)):
                    cache.set(key, value)
                    cache.get(key)

                set_t = measure(lambda: cache.set(key, value), iterations)
                cache.set(key, value)
                get_t = measure(lambda: cache.get(key), iterations)
                first_get_t = measure_first_get(cache, value, iterations)

                writer.writerow([name, "single_set", sz, 1, f"{set_t:.2f}"])
                writer.writerow([name, "single_get", sz, 1, f"{get_t:.2f}"])
                writer.writerow([name, "first_get", sz, 1, f"{first_get_t:.2f}"])

                if hasattr(cache, 'close'):
                    cache.close()

        print(f"  single ops: value_size={sz:>10,} bytes done", file=sys.stderr)


def batch_ways(cache, keys, value):
    """(backend suffix, operation, batch function) for each way `cache` can do a batch.
    Sets come first: the gets read the keys they wrote."""
    items = {k: value for k in keys}

    def set_loop():
        with cache.transact():
            for k in keys:
                cache.set(k, value)

    def get_loop():
        # Keeps the values, like get_many and like real code: a loop that drops each
        # value reuses one buffer and skips the allocations a caller really pays for.
        return {k: cache.get(k) for k in keys}

    ways = [("", "batch_set", set_loop)]
    if hasattr(cache, "set_many"):
        ways.append((" (set_many)", "batch_set", lambda: cache.set_many(items)))
    ways.append(("", "batch_get", get_loop))
    if hasattr(cache, "get_many"):
        ways.append((" (get_many)", "batch_get", lambda: cache.get_many(keys)))
    return ways


def bench_batch_ops(writer):
    """Benchmark batched set (loop in a transaction vs set_many) and batched get
    (loop vs get_many) across batch and value sizes."""
    for sz in BATCH_VALUE_SIZES:
        value = os.urandom(sz)

        for batch_n in BATCH_SIZES:
            iterations = max(5, 200 // max(1, batch_n * sz // 4096))
            keys = [f"batch_{i}" for i in range(batch_n)]

            for name, factory in [("sciqlop", SciqlopCache), ("diskcache", DiskCache)]:
                with TemporaryDirectory() as tmp:
                    cache = factory(tmp)
                    for suffix, operation, do_batch in batch_ways(cache, keys, value):
                        # Reads are cheap: few iterations of a big batch are too noisy.
                        n = iterations if operation == "batch_set" else max(50, iterations)
                        for _ in range(min(10, n)):  # warmup
                            do_batch()
                        t = measure(do_batch, n)
                        writer.writerow([name + suffix, operation, sz, batch_n, f"{t:.2f}"])
                        writer.writerow([name + suffix, f"{operation}_per_op", sz, batch_n,
                                         f"{t / batch_n:.2f}"])

                    if hasattr(cache, 'close'):
                        cache.close()

            print(f"  batch: value_size={sz:>10,}  batch_n={batch_n:>4} done", file=sys.stderr)


def main():
    writer = csv.writer(sys.stdout)
    writer.writerow(["backend", "operation", "value_size", "batch_size", "latency_us"])
    sys.stdout.flush()

    print("Running single-op benchmarks...", file=sys.stderr)
    bench_single_ops(writer)
    sys.stdout.flush()

    print("Running batch benchmarks...", file=sys.stderr)
    bench_batch_ops(writer)
    sys.stdout.flush()

    print("Done.", file=sys.stderr)


if __name__ == "__main__":
    main()
