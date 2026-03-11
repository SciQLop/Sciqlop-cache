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


def measure(fn, number):
    return timeit.timeit(fn, number=number) / number * 1e6  # μs


def bench_single_ops(writer):
    """Benchmark individual set/get across value sizes."""
    for sz in VALUE_SIZES:
        value = os.urandom(sz)
        iterations = max(20, 2000 // max(1, sz // 1024))

        for name, factory in [("sciqlop", SciqlopCache), ("diskcache", DiskCache)]:
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

                writer.writerow([name, "single_set", sz, 1, f"{set_t:.2f}"])
                writer.writerow([name, "single_get", sz, 1, f"{get_t:.2f}"])

                if hasattr(cache, 'close'):
                    cache.close()

        print(f"  single ops: value_size={sz:>10,} bytes done", file=sys.stderr)


def bench_batch_ops(writer):
    """Benchmark batched set inside a transaction across batch and value sizes."""
    for sz in BATCH_VALUE_SIZES:
        value = os.urandom(sz)

        for batch_n in BATCH_SIZES:
            iterations = max(5, 200 // max(1, batch_n * sz // 4096))
            keys = [f"batch_{i}" for i in range(batch_n)]

            for name, factory in [("sciqlop", SciqlopCache), ("diskcache", DiskCache)]:
                with TemporaryDirectory() as tmp:
                    cache = factory(tmp)

                    def do_batch():
                        with cache.transact():
                            for k in keys:
                                cache.set(k, value)

                    # warmup
                    for _ in range(min(10, iterations)):
                        do_batch()

                    t = measure(do_batch, iterations)
                    per_op = t / batch_n
                    writer.writerow([name, "batch_set", sz, batch_n, f"{t:.2f}"])
                    writer.writerow([name, "batch_set_per_op", sz, batch_n, f"{per_op:.2f}"])

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
