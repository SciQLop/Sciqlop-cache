#!/usr/bin/env python3
"""max_blob_size sweep, for several thresholds and value sizes: set/get latency, and the
footprint of 200 stored values (database bytes, value files and allocated disk space
per value).

Values up to max_blob_size are stored in the database, bigger ones as files.
Outputs CSV to stdout.
Usage:
    PYTHONPATH=build python benchmark/bench_blob_size.py > benchmark/blob_size_results.csv
"""

import csv
import os
import sys
from tempfile import TemporaryDirectory

from bench_valuesize import measure, measure_first_get
from pysciqlop_cache import Cache

MAX_BLOB_SIZES = [8 * 1024, 32 * 1024, 128 * 1024, 1024 * 1024]
VALUE_SIZES = [1024, 4096, 16384, 32768, 65536, 131072, 262144, 1048576]


def bench_one(writer, max_blob_size, sz):
    value = os.urandom(sz)
    iterations = max(20, 2000 // max(1, sz // 1024))
    with TemporaryDirectory() as tmp:
        cache = Cache(tmp, max_blob_size=max_blob_size)
        key = "bench_key"
        for _ in range(min(50, iterations)):  # warmup
            cache.set(key, value)
            cache.get(key)
        metrics = {
            "set_us": measure(lambda: cache.set(key, value), iterations),
            "get_us": measure(lambda: cache.get(key), iterations),
            "first_get_us": measure_first_get(cache, value, iterations),
        }
        cache.close()
    metrics.update(footprint(max_blob_size, value))
    for metric, v in metrics.items():
        writer.writerow([max_blob_size, metric, sz, f"{v:.2f}"])


def footprint(max_blob_size, value, count=200):
    """Per stored value: database bytes, value files, and disk space allocated for
    both (st_blocks, so a file's partly used last block counts in full)."""
    with TemporaryDirectory() as tmp:
        cache = Cache(tmp, max_blob_size=max_blob_size)
        with cache.transact():
            for i in range(count):
                cache.set(f"k{i}", value)
        cache.close()
        db = files = allocated = 0
        for root, _, names in os.walk(tmp):
            for name in names:
                st = os.stat(os.path.join(root, name))
                allocated += st.st_blocks * 512
                if name.startswith("sciqlop-cache.db"):
                    db += st.st_size
                else:
                    files += 1
    return {"db_bytes_per_value": db / count, "files_per_value": files / count,
            "disk_bytes_per_value": allocated / count}


def main():
    writer = csv.writer(sys.stdout)
    writer.writerow(["max_blob_size", "metric", "value_size", "value"])
    for max_blob_size in MAX_BLOB_SIZES:
        for sz in VALUE_SIZES:
            bench_one(writer, max_blob_size, sz)
            sys.stdout.flush()
        print(f"  max_blob_size={max_blob_size:>9,} done", file=sys.stderr)


if __name__ == "__main__":
    main()
