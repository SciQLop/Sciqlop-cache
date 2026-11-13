#!/usr/bin/env python3
"""Array benchmark: diskcache vs sciqlop-cache (pickle) vs sciqlop-cache (pickle-oob).

Values come from public datasets (see bench_datasets.py): CIFAR-10 images (uint8),
NYC taxi trips (pandas DataFrame), Hurricane ISABEL fields (float32), household power
readings (datetime64 + float64), and random noise as the incompressible worst case.
Two sweeps:

- size:    one thread, values from 100 KB to 1 GB of each dataset:
           set/get latency (ms) and disk use (MB per value);
- threads: 24 MB of Hurricane fields, 1 to 16 threads each getting (or setting)
           its own keys: total throughput (GB/s).

Outputs CSV to stdout. The datasets are downloaded on first use (~1.6 GB).
Usage:
    TMPDIR=/dev/shm PYTHONPATH=build python benchmark/bench_arrays.py > benchmark/arrays_results.csv
    ... bench_arrays.py --sweeps size --datasets images table

The 1 GB point keeps about 6 GB on TMPDIR at its peak (values plus the trash's
5 s grace period) and ~3 GB in the process, plus the dataset itself (up to 1.5 GB).
"""

import argparse
import csv
import gc
import statistics
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from tempfile import TemporaryDirectory

from diskcache import Cache as DiskCache

from bench_datasets import DATASETS, Fields, value_nbytes
from pysciqlop_cache import Cache as SciqlopCache
from pysciqlop_cache import PickleOOBSerializer

BACKENDS = {
    "diskcache": DiskCache,
    "sciqlop pickle": SciqlopCache,
    "sciqlop pickle-oob": lambda path: SciqlopCache(path, serializer=PickleOOBSerializer()),
}
SIZES = [100_000, 400_000, 1_600_000, 6_300_000, 25_000_000, 100_000_000, 400_000_000,
         1_000_000_000]
# Above this, fewer rotating keys and repetitions keep RAM use bounded (TMPDIR is RAM).
BIG_VALUE = 200_000_000
THREAD_BYTES = 24_000_000
THREADS = [1, 2, 4, 8, 16]
THREAD_KEYS = 32
ROUNDS = 5


def median_ms(fn, repeat):
    times = []
    for _ in range(repeat):
        t0 = time.perf_counter()
        fn()
        times.append(time.perf_counter() - t0)
    return statistics.median(times) * 1e3


def disk_bytes(path):
    return sum(f.stat().st_size for f in Path(path).rglob("*") if f.is_file())


def disk_per_value(factory, value):
    """Disk use of one value, in a fresh store: in a store being overwritten, eviction
    (diskcache's 1 GiB default limit) and the trash's delayed deletes skew the delta."""
    with TemporaryDirectory() as tmp:
        factory(tmp).close()  # measured closed, like below: an open store holds a WAL file
        empty = disk_bytes(tmp)
        cache = factory(tmp)
        cache.set("measured", value)
        cache.close()
        return disk_bytes(tmp) - empty


def measure(writer, dataset, value):
    nbytes = value_nbytes(value)
    big = nbytes >= BIG_VALUE
    repeat = 3 if big else max(5, min(200, 2_000_000_000 // (nbytes * 100)))
    rotating = 2 if big else 4
    for name, factory in BACKENDS.items():
        with TemporaryDirectory() as tmp:
            cache = factory(tmp)
            keys = iter(range(10**9))
            # get is timed before any overwrite: overwriting big values makes the
            # background thread free their files a few seconds later, and that kernel
            # work slowed concurrent reads (400 MB pickle-oob get: ~450 ms vs 68 ms).
            cache.set("k0", value)
            assert cache.get("k0") is not None
            get_ms = median_ms(lambda: cache.get("k0"), repeat)
            set_ms = median_ms(lambda: cache.set(f"k{next(keys) % rotating}", value), repeat)
            cache.close()
        per_value = disk_per_value(factory, value)
        for op, result in (("set", set_ms), ("get", get_ms), ("disk", per_value / 1e6)):
            writer.writerow([dataset, name, "size", op, nbytes, f"{result:.4f}"])


def size_sweep(writer, datasets):
    for dataset in datasets:
        source = DATASETS[dataset]()
        for size in SIZES:
            measure(writer, dataset, source.value(size))
            print(f"  size sweep: {dataset:7s} {size / 1e6:8.1f} MB done", file=sys.stderr)
        del source
        gc.collect()


def throughput(threads, fn, keys, nbytes):
    with ThreadPoolExecutor(threads) as pool:
        list(pool.map(fn, keys))  # warm up the threads and the page cache
        t0 = time.perf_counter()
        for _ in range(ROUNDS):
            list(pool.map(fn, keys))
        elapsed = time.perf_counter() - t0
    return ROUNDS * len(keys) * nbytes / elapsed / 1e9


def thread_sweep(writer):
    value = Fields().value(THREAD_BYTES)
    nbytes = value_nbytes(value)
    keys = [f"k{i}" for i in range(THREAD_KEYS)]
    for name, factory in BACKENDS.items():
        with TemporaryDirectory() as tmp:
            cache = factory(tmp)
            for key in keys:
                cache.set(key, value)
            for threads in THREADS:
                get_gbs = throughput(threads, cache.get, keys, nbytes)
                set_gbs = throughput(threads, lambda k: cache.set(k, value), keys, nbytes)
                writer.writerow(["fields", name, "threads", "get", threads, f"{get_gbs:.4f}"])
                writer.writerow(["fields", name, "threads", "set", threads, f"{set_gbs:.4f}"])
            cache.close()
        print(f"  thread sweep: {name} done", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--sweeps", nargs="+", choices=["size", "threads"], default=["size", "threads"])
    parser.add_argument("--datasets", nargs="+", choices=list(DATASETS), default=list(DATASETS))
    args = parser.parse_args()
    writer = csv.writer(sys.stdout, lineterminator="\n")
    writer.writerow(["dataset", "backend", "sweep", "operation", "x", "value"])
    if "size" in args.sweeps:
        size_sweep(writer, args.datasets)
    if "threads" in args.sweeps:
        thread_sweep(writer)


if __name__ == "__main__":
    main()
