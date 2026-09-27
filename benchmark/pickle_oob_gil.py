"""pickle vs pickle-oob on speasy-shaped values: latency, GIL starvation, threaded throughput.

A value is one day of MMS FGM srvy: 1.38 M rows, 4 x float32 + datetime64[ns]
time axis, 60 metadata attributes (~33 MB). Mirrors SciQLop, where one fetch
thread per graph loads days from the cache at the same time.

Usage: python benchmark/pickle_oob_gil.py [cache_dir]   (default ~/.cache/sciqlop-oob-bench)
Run it on a real disk, not a quota-limited tmpfs.
"""

import shutil
import statistics
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import numpy as np

from pysciqlop_cache import Cache, PickleOOBSerializer, PickleSerializer

ROWS = 1_380_000
DAYS = 8
THREADS = 4
REPEATS = 5


class Variable:
    def __init__(self, rows):
        self.time = (np.arange(rows, dtype=np.int64) * 62_500_000).astype("datetime64[ns]")
        self.values = np.random.rand(rows, 4).astype(np.float32)
        self.meta = {f"ATTR_{i}": f"value {i}" for i in range(60)}


class GilProbe:
    """Sleeps 1 ms in a loop; time lost beyond that is time spent waiting for the GIL."""

    def __init__(self, tick=1e-3):
        self.tick, self.starved, self._stop = tick, 0.0, threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def _run(self):
        while not self._stop.is_set():
            t0 = time.perf_counter()
            time.sleep(self.tick)
            self.starved += max(0.0, time.perf_counter() - t0 - self.tick - 1e-4)

    def __enter__(self):
        self._thread.start()
        return self

    def __exit__(self, *exc):
        self._stop.set()
        self._thread.join()


def timed(fn):
    t0 = time.perf_counter()
    fn()
    return time.perf_counter() - t0


def fill(cache):
    day = Variable(ROWS)
    return [timed(lambda k=k: cache.set(f"day{k}", day)) for k in range(DAYS)]


def get_all_serial(cache):
    for k in range(DAYS):
        cache.get(f"day{k}")


def get_all_threaded(cache, pool):
    list(pool.map(lambda k: cache.get(f"day{k}"), range(DAYS)))


def measure(cache, pool):
    get_all_serial(cache)  # warm the page cache
    serial = [timed(lambda: get_all_serial(cache)) for _ in range(REPEATS)]
    starved = []
    for _ in range(REPEATS):
        with GilProbe() as probe:
            get_all_serial(cache)
        starved.append(probe.starved)
    threaded = [timed(lambda: get_all_threaded(cache, pool)) for _ in range(REPEATS)]
    return serial, starved, threaded


def report(name, sets, serial, starved, threaded):
    ms = lambda xs: statistics.median(xs) * 1e3
    print(f"{name:>11} | set/day {ms(sets):6.1f} ms | get/day {ms(serial) / DAYS:6.2f} ms"
          f" | GIL starved/day {ms(starved) / DAYS:6.2f} ms"
          f" | {THREADS} threads, {DAYS} days {ms(threaded):7.1f} ms")


def main():
    root = Path(sys.argv[1] if len(sys.argv) > 1 else Path.home() / ".cache/sciqlop-oob-bench")
    serializers = [PickleSerializer(), PickleOOBSerializer()]
    with ThreadPoolExecutor(THREADS) as pool:
        for ser in serializers:
            path = root / ser.name
            shutil.rmtree(path, ignore_errors=True)
            cache = Cache(str(path), serializer=ser)
            sets = fill(cache)
            report(ser.name, sets, *measure(cache, pool))
            del cache
            shutil.rmtree(path, ignore_errors=True)


if __name__ == "__main__":
    main()
