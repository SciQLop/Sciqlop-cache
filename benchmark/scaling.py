#!/usr/bin/env python3
"""Scaling benchmark: measure set/get latency as cache grows from 0 to N entries.

Outputs CSV to stdout, human-readable progress to stderr.
Usage:
    python scaling.py [--max-entries 10000000] [--value-size 256] [--sample-ops 1000]
    python scaling.py --raw --backend both > raw.csv
"""

import argparse
import csv
import os
import statistics
import sys
import timeit
from tempfile import TemporaryDirectory

from pysciqlop_cache import Cache as SciqlopCache
from diskcache import Cache as DiskCache


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
    return timeit.timeit(fn, number=ops) / ops * 1e6


def measure_latencies(fn, rounds, batch_size):
    """Return list of per-op latencies (µs), one per batch of batch_size ops."""
    return [timeit.timeit(fn, number=batch_size) / batch_size * 1e6
            for _ in range(rounds)]


def fill_cache(cache, value, start, end):
    for i in range(start, end):
        cache.set(f"k{i}", value)


def sample_at_checkpoint(cache, value, inserted, sample_ops, raw=False, rounds=50):
    seq = [0]

    def set_op():
        cache.set(f"bench_set_{seq[0]}", value)
        seq[0] += 1

    total_ops = rounds * (sample_ops // rounds) if raw else sample_ops
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
        set_t = measure_latencies(set_op, rounds, batch_size)
        hit_t = measure_latencies(get_hit_op, rounds, batch_size)
        miss_t = measure_latencies(get_miss_op, rounds, batch_size)
    else:
        set_t = measure_latency(set_op, sample_ops)
        hit_t = measure_latency(get_hit_op, sample_ops)
        miss_t = measure_latency(get_miss_op, sample_ops)

    for i in range(seq[0]):
        cache.delete(f"bench_set_{i}")

    return set_t, hit_t, miss_t


def run_one_backend(name, cache_factory, checkpoints, value, sample_ops, raw, writer):
    with TemporaryDirectory() as tmp:
        cache = cache_factory(tmp)
        inserted = 0

        for cp_idx, cp in enumerate(checkpoints):
            fill_cache(cache, value, inserted, cp)
            inserted = cp

            set_t, hit_t, miss_t = sample_at_checkpoint(
                cache, value, inserted, sample_ops, raw=raw)

            if raw:
                for t in set_t:
                    writer.writerow([name, inserted, "set", f"{t:.2f}"])
                for t in hit_t:
                    writer.writerow([name, inserted, "get_hit", f"{t:.2f}"])
                for t in miss_t:
                    writer.writerow([name, inserted, "get_miss", f"{t:.2f}"])
            else:
                writer.writerow([name, inserted, f"{set_t:.2f}", f"{hit_t:.2f}", f"{miss_t:.2f}"])
            sys.stdout.flush()

            pct = (cp_idx + 1) / len(checkpoints) * 100
            if raw:
                print(
                    f"  [{name}] [{pct:5.1f}%] {inserted:>10,} entries | "
                    f"set={statistics.median(set_t):7.2f}μs  "
                    f"get_hit={statistics.median(hit_t):7.2f}μs  "
                    f"get_miss={statistics.median(miss_t):7.2f}μs",
                    file=sys.stderr,
                )
            else:
                print(
                    f"  [{name}] [{pct:5.1f}%] {inserted:>10,} entries | "
                    f"set={set_t:7.2f}μs  get_hit={hit_t:7.2f}μs  get_miss={miss_t:7.2f}μs",
                    file=sys.stderr,
                )


BACKENDS = {
    "sciqlop": SciqlopCache,
    "diskcache": DiskCache,
}


def run(max_entries, value_size, sample_ops, raw, backends):
    value = os.urandom(value_size)
    checkpoints = make_checkpoints(max_entries)

    writer = csv.writer(sys.stdout)
    if raw:
        writer.writerow(["backend", "entries", "operation", "latency_us"])
    else:
        writer.writerow(["backend", "entries", "set_us", "get_hit_us", "get_miss_us"])
    sys.stdout.flush()

    for name in backends:
        run_one_backend(name, BACKENDS[name], checkpoints, value, sample_ops, raw, writer)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--max-entries", type=int, default=10_000_000)
    parser.add_argument("--value-size", type=int, default=256,
                        help="bytes per value (kept small to stress index, not I/O)")
    parser.add_argument("--sample-ops", type=int, default=1000,
                        help="operations per latency measurement")
    parser.add_argument("--raw", action="store_true",
                        help="output per-op latencies (for violin/distribution plots)")
    parser.add_argument("--backend", default="both",
                        choices=["sciqlop", "diskcache", "both"],
                        help="which backend(s) to benchmark")
    args = parser.parse_args()

    backends = list(BACKENDS.keys()) if args.backend == "both" else [args.backend]
    run(args.max_entries, args.value_size, args.sample_ops, args.raw, backends)


if __name__ == "__main__":
    main()
