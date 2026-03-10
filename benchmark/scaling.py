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


def measure_latencies(fn, rounds, batch_size):
    """Return list of per-op latencies (µs), one per batch of batch_size ops."""
    times = []
    for _ in range(rounds):
        t = timeit.timeit(fn, number=batch_size)
        times.append(t / batch_size * 1e6)
    return times


def fill_cache(cache, value, start, end):
    for i in range(start, end):
        cache.set(f"k{i}", value)


def sample_at_checkpoint(cache, value, inserted, sample_ops, raw=False, rounds=50):
    total_ops = rounds * (sample_ops // rounds) if raw else sample_ops
    seq = [0]

    def set_op():
        cache.set(f"bench_set_{seq[0]}", value)
        seq[0] += 1

    hit_keys = [f"k{i % inserted}" for i in range(total_ops)]
    miss_keys = [f"miss_{i}" for i in range(total_ops)]

    def get_hit_op(idx=[0]):
        cache.get(hit_keys[idx[0] % len(hit_keys)])
        idx[0] += 1

    def get_miss_op(idx=[0]):
        cache.get(miss_keys[idx[0] % len(miss_keys)])
        idx[0] += 1

    if raw:
        batch_size = sample_ops // rounds
        set_times = measure_latencies(set_op, rounds, batch_size)
        get_hit_times = measure_latencies(get_hit_op, rounds, batch_size)
        get_miss_times = measure_latencies(get_miss_op, rounds, batch_size)
    else:
        set_times = measure_latency(set_op, sample_ops)
        get_hit_times = measure_latency(get_hit_op, sample_ops)
        get_miss_times = measure_latency(get_miss_op, sample_ops)

    for i in range(seq[0]):
        cache.delete(f"bench_set_{i}")

    return set_times, get_hit_times, get_miss_times


def db_size_mb(tmp):
    return sum(
        os.path.getsize(os.path.join(tmp, f)) / (1024 * 1024)
        for f in os.listdir(tmp)
        if f.startswith("sciqlop-cache.db")
    )


def run(max_entries, value_size, sample_ops, raw=False):
    value = os.urandom(value_size)
    checkpoints = make_checkpoints(max_entries)

    writer = csv.writer(sys.stdout)
    if raw:
        writer.writerow(["entries", "operation", "latency_us"])
    else:
        writer.writerow(["entries", "set_us", "get_hit_us", "get_miss_us", "db_size_mb"])
    sys.stdout.flush()

    with TemporaryDirectory() as tmp:
        cache = Cache(tmp)
        inserted = 0

        for cp_idx, cp in enumerate(checkpoints):
            fill_cache(cache, value, inserted, cp)
            inserted = cp

            set_t, hit_t, miss_t = sample_at_checkpoint(
                cache, value, inserted, sample_ops, raw=raw)

            size = db_size_mb(tmp)

            if raw:
                for t in set_t:
                    writer.writerow([inserted, "set", f"{t:.2f}"])
                for t in hit_t:
                    writer.writerow([inserted, "get_hit", f"{t:.2f}"])
                for t in miss_t:
                    writer.writerow([inserted, "get_miss", f"{t:.2f}"])
            else:
                writer.writerow([
                    inserted, f"{set_t:.2f}", f"{hit_t:.2f}",
                    f"{miss_t:.2f}", f"{size:.2f}",
                ])
            sys.stdout.flush()

            pct = (cp_idx + 1) / len(checkpoints) * 100
            if raw:
                import statistics
                print(
                    f"  [{pct:5.1f}%] {inserted:>10,} entries | "
                    f"set={statistics.median(set_t):7.2f}μs  "
                    f"get_hit={statistics.median(hit_t):7.2f}μs  "
                    f"get_miss={statistics.median(miss_t):7.2f}μs  "
                    f"db={size:.1f}MB",
                    file=sys.stderr,
                )
            else:
                print(
                    f"  [{pct:5.1f}%] {inserted:>10,} entries | "
                    f"set={set_t:7.2f}μs  get_hit={hit_t:7.2f}μs  "
                    f"get_miss={miss_t:7.2f}μs  db={size:.1f}MB",
                    file=sys.stderr,
                )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--max-entries", type=int, default=10_000_000)
    parser.add_argument("--value-size", type=int, default=256,
                        help="bytes per value (kept small to stress index, not I/O)")
    parser.add_argument("--sample-ops", type=int, default=1000,
                        help="operations per latency measurement")
    parser.add_argument("--raw", action="store_true",
                        help="output per-op latencies (for violin/distribution plots)")
    args = parser.parse_args()
    run(args.max_entries, args.value_size, args.sample_ops, raw=args.raw)


if __name__ == "__main__":
    main()
