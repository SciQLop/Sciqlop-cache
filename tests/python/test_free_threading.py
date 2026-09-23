"""Free-threading torture: many Python threads hammering ONE shared store object.

On a free-threaded interpreter (3.13t/3.14t) these threads really run in
parallel, so this exercises the bindings and the store mutex without the GIL
serialising the Python side. On a regular build it still runs as a threaded
stress test.

Always-on with a short default duration so CI's 3.14t job runs it.
CLI: FT_TORTURE_DURATION=60 python test_free_threading.py
"""

import hashlib
import os
import random
import sys
import sysconfig
import threading
import time
import unittest
from tempfile import TemporaryDirectory

from pysciqlop_cache import Cache, Index, FanoutCache, FanoutIndex

DURATION = float(os.environ.get("FT_TORTURE_DURATION", 3))
N_THREADS = max(8, 2 * (os.cpu_count() or 4))
NUM_KEYS = 64
MAX_VALUE_SIZE = 32 * 1024  # crosses the 8KB blob/file threshold
COUNTER_KEY = "shared_counter"

FREE_THREADED_BUILD = bool(sysconfig.get_config_var("Py_GIL_DISABLED"))


def _make_value(key, rng):
    payload = rng.randbytes(rng.randint(16, MAX_VALUE_SIZE))
    return key.encode() + b"|" + hashlib.blake2b(payload, digest_size=8).digest() + payload


def _assert_value_intact(key, value):
    """A value read back must be one that was written for this very key, untorn."""
    prefix = key.encode() + b"|"
    assert value.startswith(prefix), f"{key!r}: value belongs to another key"
    digest, payload = value[len(prefix):len(prefix) + 8], value[len(prefix) + 8:]
    assert hashlib.blake2b(payload, digest_size=8).digest() == digest, f"{key!r}: torn value"


def _transact(store, key):
    if isinstance(store, (FanoutCache, FanoutIndex)):
        return store.transact(key)
    return store.transact()


def _read_modify_write(store, key, rng):
    with _transact(store, key):
        old = store.get(key)
        if old is not None:
            _assert_value_intact(key, old)
        store.set(key, _make_value(key, rng))


def _random_op(store, rng):
    """One random operation; returns how much it added to the shared counter."""
    key = f"k_{rng.randrange(NUM_KEYS)}"
    op = rng.random()
    if op < 0.30:
        store.set(key, _make_value(key, rng))
    elif op < 0.55:
        value = store.get(key)
        if value is not None:
            _assert_value_intact(key, value)
    elif op < 0.65:
        store.delete(key)
    elif op < 0.72:
        value = store.pop(key, None)
        if value is not None:
            _assert_value_intact(key, value)
    elif op < 0.78:
        store.add(key, _make_value(key, rng))
    elif op < 0.85:
        _read_modify_write(store, key, rng)
    else:
        store.incr(COUNTER_KEY)
        return 1
    return 0


def _writer(store, seed, deadline, barrier):
    rng = random.Random(seed)
    barrier.wait()
    increments = 0
    while time.monotonic() < deadline:
        increments += _random_op(store, rng)
    return increments


def _iterator(store, deadline, barrier):
    barrier.wait()
    while time.monotonic() < deadline:
        for key in list(store):
            if key == COUNTER_KEY:
                continue
            value = store.get(key)
            if value is not None:
                _assert_value_intact(key, value)
        len(store)
    return 0


def _run_threads(targets):
    """Run callables in parallel threads; return their results, re-raising the first failure."""
    results, errors = [None] * len(targets), []

    def run(i, target):
        try:
            results[i] = target()
        except BaseException as e:  # noqa: BLE001 — surfaced to the main thread below
            errors.append(e)

    threads = [threading.Thread(target=run, args=(i, t)) for i, t in enumerate(targets)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    if errors:
        raise errors[0]
    return results


def _make_store(store_cls, path):
    if store_cls in (FanoutCache, FanoutIndex):
        return store_cls(path, shard_count=4)
    return store_cls(path)


@unittest.skipUnless(FREE_THREADED_BUILD, "needs a free-threaded (3.13t+) interpreter")
class GilStaysDisabled(unittest.TestCase):
    def test_import_does_not_reenable_gil(self):
        # An extension that doesn't declare Py_mod_gil makes CPython turn the
        # GIL back on at import, silently turning every test below into a
        # GIL-serialised one.
        self.assertFalse(sys._is_gil_enabled())


class SharedStoreTorture(unittest.TestCase):
    def _run_for_store(self, store_cls):
        with TemporaryDirectory() as tmp:
            store = _make_store(store_cls, tmp)
            store.set(COUNTER_KEY, 0)
            deadline = time.monotonic() + DURATION
            barrier = threading.Barrier(N_THREADS + 1)
            writers = [lambda s=i: _writer(store, s, deadline, barrier) for i in range(N_THREADS)]

            increments = _run_threads(writers + [lambda: _iterator(store, deadline, barrier)])

            self.assertGreater(sum(increments), 0)
            self.assertEqual(store.get(COUNTER_KEY), sum(increments), "lost incr() updates")
            for key in store:
                if key != COUNTER_KEY:
                    _assert_value_intact(key, store.get(key))
            self.assertEqual(len(store), len(list(store.keys())))
            self.assertTrue(store.check().ok)
            store.close()  # Windows can't remove the temp dir while the DB is open

    def test_cache(self):
        self._run_for_store(Cache)

    def test_index(self):
        self._run_for_store(Index)

    def test_fanout_cache(self):
        self._run_for_store(FanoutCache)

    def test_fanout_index(self):
        self._run_for_store(FanoutIndex)


if __name__ == "__main__":
    unittest.main()
