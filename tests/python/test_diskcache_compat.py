"""diskcache API compatibility: non-str keys, retry=, get(tag=/expire_time=),
constructor kwargs, MutableMapping Index, KeyError semantics, Timeout."""
import os
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
from collections.abc import MutableMapping

import pysciqlop_cache
from pysciqlop_cache import Cache, FanoutCache, Index, FanoutIndex, Timeout


class _Tmp(unittest.TestCase):
    def setUp(self):
        self.dirs = []

    def tearDown(self):
        for d in self.dirs:
            shutil.rmtree(d, ignore_errors=True)

    def tmp(self):
        d = tempfile.mkdtemp()
        self.dirs.append(d)
        return d

    def stores(self):
        yield "Cache", Cache(self.tmp())
        yield "FanoutCache", FanoutCache(self.tmp(), shard_count=2)
        yield "Index", Index(self.tmp())
        yield "FanoutIndex", FanoutIndex(self.tmp(), shard_count=2)


class TestNonStrKeys(_Tmp):
    KEYS = [42, -7, 3.5, True, b"raw\xff", ("a", 1), frozenset({1, 2}), "plain"]

    def test_roundtrip_all_key_types(self):
        for name, s in self.stores():
            for k in self.KEYS:
                with self.subTest(store=name, key=k):
                    s.set(k, "v")
                    self.assertIn(k, s)
                    self.assertEqual(s.get(k), "v")
                    self.assertEqual(s[k], "v")
                    self.assertTrue(s.delete(k))
                    self.assertNotIn(k, s)

    def test_keys_and_iteration_return_original_objects(self):
        for name, s in self.stores():
            with self.subTest(store=name):
                for k in self.KEYS:
                    s[k] = 1
                self.assertEqual(set(s.keys()), set(self.KEYS))
                self.assertEqual(set(s), set(self.KEYS))
                self.assertEqual(set(s.iterkeys()), set(self.KEYS))

    def test_plain_str_keys_are_stored_verbatim(self):
        c = Cache(self.tmp())
        c["plain"] = 1
        self.assertEqual(super(Cache, c).keys(), ["plain"])

    def test_int_and_bool_keys_are_distinct_from_str(self):
        c = Cache(self.tmp())
        c[1] = "int"
        c["1"] = "str"
        self.assertEqual(c[1], "int")
        self.assertEqual(c["1"], "str")

    def test_pop_add_touch_incr_with_non_str_key(self):
        c = Cache(self.tmp())
        self.assertTrue(c.add((1, 2), 5))
        self.assertFalse(c.add((1, 2), 6))
        self.assertEqual(c.incr((1, 2)), 6)
        self.assertEqual(c.decr((1, 2)), 5)
        self.assertTrue(c.touch((1, 2), expire=10))
        self.assertEqual(c.pop((1, 2)), 5)
        self.assertIsNone(c.pop((1, 2)))


class TestRetryKwarg(_Tmp):
    def test_retry_accepted_everywhere(self):
        c = Cache(self.tmp())
        c.set("k", 1, retry=True)
        self.assertEqual(c.get("k", retry=True), 1)
        self.assertFalse(c.add("k", 2, retry=True))
        self.assertEqual(c.incr("k", retry=True), 2)
        self.assertEqual(c.decr("k", retry=True), 1)
        self.assertTrue(c.touch("k", expire=10, retry=True))
        self.assertEqual(c.pop("k", retry=True), 1)
        self.assertFalse(c.delete("k", retry=True))
        c.expire(retry=True)
        c.evict(retry=True)
        c.check(retry=True)
        with c.transact(retry=True):
            c.set("t", 1)
        c.clear(retry=True)
        self.assertEqual(len(c), 0)

    def test_retry_accepted_on_fanout(self):
        f = FanoutCache(self.tmp(), shard_count=2)
        f.set("k", 1, retry=True)
        self.assertEqual(f.get("k", retry=True), 1)
        self.assertTrue(f.delete("k", retry=True))
        f.clear(retry=True)


class TestGetWithMeta(_Tmp):
    def test_get_tag_and_expire_time(self):
        for name, c in (("Cache", Cache(self.tmp())), ("FanoutCache", FanoutCache(self.tmp(), shard_count=2))):
            with self.subTest(store=name):
                before = time.time()
                c.set("k", "v", expire=3600, tag="t")
                c.set("plain", "p")
                self.assertEqual(c.get("k", tag=True), ("v", "t"))
                value, exp = c.get("k", expire_time=True)
                self.assertEqual(value, "v")
                self.assertAlmostEqual(exp, before + 3600, delta=3)
                value, exp, tag = c.get("k", expire_time=True, tag=True)
                self.assertEqual((value, tag), ("v", "t"))
                self.assertEqual(c.get("plain", expire_time=True, tag=True), ("p", None, None))
                self.assertEqual(c.get("missing", tag=True), (None, None))
                self.assertEqual(c.get("missing", default=0, expire_time=True, tag=True), (0, None, None))

    def test_pop_tag_and_expire_time(self):
        c = Cache(self.tmp())
        c.set("k", "v", expire=3600, tag="t")
        value, exp, tag = c.pop("k", expire_time=True, tag=True)
        self.assertEqual((value, tag), ("v", "t"))
        self.assertGreater(exp, time.time())
        self.assertNotIn("k", c)
        self.assertEqual(c.pop("k", tag=True), (None, None))

    def test_set_returns_true(self):
        c = Cache(self.tmp())
        self.assertIs(c.set("k", 1), True)

    def test_read_true_is_explicitly_unsupported(self):
        c = Cache(self.tmp())
        c.set("k", "v")
        with self.assertRaises(NotImplementedError):
            c.get("k", read=True)
        self.assertEqual(c.get("k", read=False), "v")


class TestConstructorKwargs(_Tmp):
    def test_cache_directory_and_size_limit(self):
        d = self.tmp()
        c = Cache(directory=d, size_limit=12345)
        self.assertEqual(os.path.realpath(str(c.path())), os.path.realpath(d))
        self.assertEqual(c.get_meta("max_size"), "12345")

    def test_legacy_kwargs_still_work(self):
        d = self.tmp()
        c = Cache(cache_path=d, max_size=99)
        self.assertEqual(c.get_meta("max_size"), "99")
        f = FanoutCache(cache_path=self.tmp(), shard_count=2, max_size=5)
        self.assertEqual(f.shard_count(), 2)

    def test_diskcache_settings_are_accepted(self):
        c = Cache(self.tmp(), timeout=60, disk=None, statistics=1, tag_index=1,
                  eviction_policy="least-recently-stored", cull_limit=10,
                  sqlite_cache_size=8192, sqlite_mmap_size=2**20, disk_min_file_size=32768,
                  disk_pickle_protocol=5)
        c.set("k", 1)
        self.assertEqual(c["k"], 1)

    def test_unknown_kwarg_and_custom_disk_are_rejected(self):
        with self.assertRaises(TypeError):
            Cache(self.tmp(), bogus=1)
        with self.assertRaises(TypeError):
            Cache(self.tmp(), disk=object)

    def test_fanout_shards_alias(self):
        f = FanoutCache(self.tmp(), shards=2, size_limit=10)
        self.assertEqual(f.shard_count(), 2)

    def test_index_directory_and_initial_items(self):
        ix = Index(directory=self.tmp())
        ix["a"] = 1
        self.assertEqual(ix["a"], 1)
        ix2 = Index(self.tmp(), {"a": 1, "b": 2}, c=3)
        self.assertEqual(dict(ix2.items()), {"a": 1, "b": 2, "c": 3})


class TestKeyErrorSemantics(_Tmp):
    def test_getitem_and_delitem_raise_keyerror(self):
        for name, s in self.stores():
            with self.subTest(store=name):
                with self.assertRaises(KeyError):
                    s["missing"]
                with self.assertRaises(KeyError):
                    del s["missing"]
                s["n"] = None
                self.assertIsNone(s["n"])
                self.assertIsNone(s.get("missing"))

    def test_index_pop_raises_without_default(self):
        ix = Index(self.tmp())
        with self.assertRaises(KeyError):
            ix.pop("missing")
        self.assertEqual(ix.pop("missing", 7), 7)
        self.assertEqual(ix.pop("missing", default=8), 8)
        ix["a"] = 1
        self.assertEqual(ix.pop("a"), 1)

    def test_cache_pop_keeps_none_default(self):
        c = Cache(self.tmp())
        self.assertIsNone(c.pop("missing"))


class TestIndexMapping(_Tmp):
    def test_index_is_a_mutable_mapping(self):
        for cls in (Index, FanoutIndex):
            ix = cls(self.tmp())
            with self.subTest(cls=cls.__name__):
                self.assertIsInstance(ix, MutableMapping)
                ix.update({"a": 1, "b": 2}, c=3)
                self.assertEqual(sorted(ix.items()), [("a", 1), ("b", 2), ("c", 3)])
                self.assertEqual(sorted(ix.values()), [1, 2, 3])
                self.assertEqual(sorted(ix.keys()), ["a", "b", "c"])
                self.assertEqual(len(ix.items()), 3)
                self.assertIn(("a", 1), ix.items())
                self.assertEqual(ix.setdefault("a", 9), 1)
                self.assertEqual(ix.setdefault("d", 4), 4)
                k, v = ix.popitem()
                self.assertNotIn(k, ix)
                k2, v2 = ix.peekitem()
                self.assertIn(k2, ix)
                self.assertEqual(ix[k2], v2)


# The lock must come from another process: POSIX file locks are per process and
# each SQLite build tracks its own, so python's sqlite3 module cannot lock against
# the vendored SQLite inside this process (it even SIGBUSes the WAL index).
_LOCKER = """
import sqlite3, sys
con = sqlite3.connect(sys.argv[1], timeout=0, isolation_level=None)
con.execute("BEGIN EXCLUSIVE")
print("locked", flush=True)
sys.stdin.readline()
con.execute("ROLLBACK")
"""


class _ForeignLock:
    def __init__(self, cache_dir):
        self.db = os.path.join(cache_dir, "sciqlop-cache.db")

    def __enter__(self):
        self.proc = subprocess.Popen([sys.executable, "-c", _LOCKER, self.db],
                                     stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
        assert self.proc.stdout.readline().strip() == "locked"
        return self

    def __exit__(self, *exc):
        self.proc.stdin.write("\n")
        self.proc.stdin.flush()
        self.proc.wait(timeout=10)
        return False


class TestTimeout(_Tmp):
    def test_timeout_is_raised_when_db_stays_locked(self):
        d = self.tmp()
        c = Cache(d, timeout=0.05)
        with _ForeignLock(d):
            t0 = time.time()
            with self.assertRaises(Timeout) as cm:
                c.set("k", 1)
            self.assertIsInstance(cm.exception, RuntimeError)
            self.assertLess(time.time() - t0, 5)
        c.set("k", 1)
        self.assertEqual(c["k"], 1)

    def test_timeout_on_fanout(self):
        d = self.tmp()
        f = FanoutCache(d, shard_count=1, timeout=0.05)
        with _ForeignLock(os.path.join(d, "00")):
            with self.assertRaises(Timeout):
                f.set("k", 1)

    def test_timeout_exported(self):
        self.assertTrue(issubclass(pysciqlop_cache.Timeout, RuntimeError))


if __name__ == "__main__":
    unittest.main()
