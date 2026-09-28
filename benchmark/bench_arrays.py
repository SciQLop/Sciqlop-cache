#!/usr/bin/env python3
"""Numpy-array benchmark: diskcache vs sciqlop-cache (pickle) vs sciqlop-cache (pickle-oob).

A value is a measurement series: `rows` samples of 4 float32 values plus a
datetime64[ns] time axis (24 bytes per row), the kind of object a data
cache holds. Two sweeps:

- size:    one thread, rows from 4 Ki to 4 Mi (100 KB to 100 MB per value):
           set/get latency (ms) and disk use (MB per value);
- threads: 24 MB values, 1 to 16 threads each getting (or setting) its own
           keys: total throughput (GB/s).

Outputs CSV to stdout.
Usage:
    TMPDIR=/dev/shm PYTHONPATH=build python benchmark/bench_arrays.py > benchmark/arrays_results.csv
"""

import csv
import os
import statistics
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from tempfile import TemporaryDirectory

import numpy as np
from diskcache import Cache as DiskCache

from pysciqlop_cache import Cache as SciqlopCache
from pysciqlop_cache import PickleOOBSerializer

BACKENDS = {
    "diskcache": DiskCache,
    "sciqlop pickle": SciqlopCache,
    "sciqlop pickle-oob": lambda path: SciqlopCache(path, serializer=PickleOOBSerializer()),
}
SIZE_ROWS = [4_096, 16_384, 65_536, 262_144, 1_048_576, 4_194_304]
THREAD_ROWS = 1_048_576
THREADS = [1, 2, 4, 8, 16]
THREAD_KEYS = 32
ROUNDS = 5


class Series:
    def __init__(self, rows, seed=0):
        rng = np.random.default_rng(seed)
        self.time = (np.arange(rows, dtype=np.int64) * 62_500_000).astype("datetime64[ns]")
        self.values = rng.standard_normal((rows, 4), dtype=np.float32)
        self.meta = {"units": "nT", "sampling": "16 Hz"}

    @property
    def nbytes(self):
        return self.time.nbytes + self.values.nbytes


def median_ms(fn, repeat):
    times = []
    for _ in range(repeat):
        t0 = time.perf_counter()
        fn()
        times.append(time.perf_counter() - t0)
    return statistics.median(times) * 1e3


def disk_bytes(path):
    return sum(f.stat().st_size for f in Path(path).rglob("*") if f.is_file())


def size_sweep(writer):
    for rows in SIZE_ROWS:
        value = Series(rows)
        repeat = max(5, min(200, 2_000_000_000 // (value.nbytes * 100)))
        for name, factory in BACKENDS.items():
            with TemporaryDirectory() as tmp:
                cache = factory(tmp)
                keys = iter(range(10**9))
                cache.set("warm", value)
                cache.get("warm")
                set_ms = median_ms(lambda: cache.set(f"k{next(keys) % 4}", value), repeat)
                get_ms = median_ms(lambda: cache.get("k0"), repeat)
                before = disk_bytes(tmp)
                cache.set("measured", value)
                per_value = disk_bytes(tmp) - before
                cache.close()
            for op, result in (("set", set_ms), ("get", get_ms), ("disk", per_value / 1e6)):
                writer.writerow([name, "size", op, value.nbytes, f"{result:.4f}"])
        print(f"  size sweep: {value.nbytes / 1e6:8.1f} MB done", file=sys.stderr)


def throughput(threads, fn, keys, nbytes):
    with ThreadPoolExecutor(threads) as pool:
        list(pool.map(fn, keys))  # warm up the threads and the page cache
        t0 = time.perf_counter()
        for _ in range(ROUNDS):
            list(pool.map(fn, keys))
        elapsed = time.perf_counter() - t0
    return ROUNDS * len(keys) * nbytes / elapsed / 1e9


def thread_sweep(writer):
    value = Series(THREAD_ROWS)
    keys = [f"k{i}" for i in range(THREAD_KEYS)]
    for name, factory in BACKENDS.items():
        with TemporaryDirectory() as tmp:
            cache = factory(tmp)
            for key in keys:
                cache.set(key, value)
            for threads in THREADS:
                get_gbs = throughput(threads, cache.get, keys, value.nbytes)
                set_gbs = throughput(threads, lambda k: cache.set(k, value), keys, value.nbytes)
                writer.writerow([name, "threads", "get", threads, f"{get_gbs:.4f}"])
                writer.writerow([name, "threads", "set", threads, f"{set_gbs:.4f}"])
            cache.close()
        print(f"  thread sweep: {name} done", file=sys.stderr)


def main():
    writer = csv.writer(sys.stdout, lineterminator="\n")
    writer.writerow(["backend", "sweep", "operation", "x", "value"])
    size_sweep(writer)
    thread_sweep(writer)


if __name__ == "__main__":
    main()
