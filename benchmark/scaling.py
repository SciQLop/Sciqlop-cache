#!/usr/bin/env python3
"""Scaling benchmark: measure set/get latency as cache grows from 0 to N entries.

Outputs CSV to stdout, human-readable progress to stderr.
Usage:
    python scaling.py [--max-entries 10000000] [--value-size 256] [--sample-ops 1000]
"""

import argparse
import csv
import os
import sys
import time
import timeit
from tempfile import TemporaryDirectory

from pysciqlop_cache import Cache


def make_checkpoints(max_entries):
    """Generate logarithmically-spaced checkpoints from 0 to max_entries."""
    pts = set()
    n = 100
    while n < max_entries:
        pts.add(n)
        n = int(n * 1.5)
        if n > 500_000:
            n = round(n, -5)
    pts.add(max_entries)
    return sorted(pts)


def measure_latency(fn, ops):
    """Return per-op latency in microseconds."""
    t = timeit.timeit(fn, number=ops)
    return t / ops * 1e6


def run(max_entries, value_size, sample_ops):
    value = os.urandom(value_size)
    checkpoints = make_checkpoints(max_entries)

    writer = csv.writer(sys.stdout)
    writer.writerow([
        "entries", "set_us", "get_hit_us", "get_miss_us", "db_size_mb",
    ])
    sys.stdout.flush()

    with TemporaryDirectory() as tmp:
        cache = Cache(tmp)
        inserted = 0
        cp_idx = 0

        for cp in checkpoints:
            batch_start = inserted
            for i in range(batch_start, cp):
                cache.set(f"k{i}", value)
            inserted = cp

            seq = [0]

            def set_op():
                key = f"bench_set_{seq[0]}"
                seq[0] += 1
                cache.set(key, value)

            hit_keys = [f"k{i % inserted}" for i in range(sample_ops)]
            miss_keys = [f"miss_{i}" for i in range(sample_ops)]

            def get_hit_op(idx=[0]):
                cache.get(hit_keys[idx[0]])
                idx[0] += 1

            def get_miss_op(idx=[0]):
                cache.get(miss_keys[idx[0]])
                idx[0] += 1

            set_us = measure_latency(set_op, sample_ops)
            get_hit_us = measure_latency(get_hit_op, sample_ops)
            get_miss_us = measure_latency(get_miss_op, sample_ops)

            # clean up bench keys
            for i in range(seq[0]):
                cache.delete(f"bench_set_{i}")

            db_size_mb = sum(
                os.path.getsize(os.path.join(tmp, f)) / (1024 * 1024)
                for f in os.listdir(tmp)
                if f.startswith("sciqlop-cache.db")
            )

            writer.writerow([
                inserted,
                f"{set_us:.2f}",
                f"{get_hit_us:.2f}",
                f"{get_miss_us:.2f}",
                f"{db_size_mb:.2f}",
            ])
            sys.stdout.flush()

            cp_idx += 1
            pct = cp_idx / len(checkpoints) * 100
            print(
                f"  [{pct:5.1f}%] {inserted:>10,} entries | "
                f"set={set_us:7.2f}μs  get_hit={get_hit_us:7.2f}μs  get_miss={get_miss_us:7.2f}μs  "
                f"db={db_size_mb:.1f}MB",
                file=sys.stderr,
            )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--max-entries", type=int, default=10_000_000)
    parser.add_argument("--value-size", type=int, default=256,
                        help="bytes per value (kept small to stress index, not I/O)")
    parser.add_argument("--sample-ops", type=int, default=1000,
                        help="operations per latency measurement")
    args = parser.parse_args()
    run(args.max_entries, args.value_size, args.sample_ops)


if __name__ == "__main__":
    main()
