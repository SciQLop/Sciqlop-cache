"""The same contract on every store type.

The Python wrappers of FanoutCache and FanoutIndex repeat Cache's and Index's
code shard by shard. These tests run one contract over each pair, so a copy
that drifts is caught.
"""
import gc
import shutil
import tempfile
import time
import unittest
from datetime import timedelta

from pysciqlop_cache import Cache, FanoutCache, FanoutIndex, Index, MsgspecSerializer

NON_STR_KEYS = [42, 3.5, b"raw", ("sensor", 7), frozenset({1, 2})]


class _TempDir(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()

    def tearDown(self):
        gc.collect()
        shutil.rmtree(self.tmp, ignore_errors=True)


class CacheContract(_TempDir):
    STORES = (Cache, FanoutCache)

    def _each(self):
        for cls in self.STORES:
            with self.subTest(cls.__name__):
                yield cls(f"{self.tmp}/{cls.__name__}")

    def test_non_str_keys_through_every_method(self):
        for store in self._each():
            for key in NON_STR_KEYS:
                self.assertTrue(store.set(key, "v"))
                self.assertEqual(store.get(key), "v")
                self.assertEqual(store[key], "v")
                self.assertIn(key, store)
                self.assertTrue(store.exists(key))
                self.assertTrue(store.touch(key, expire=60))
                self.assertFalse(store.add(key, "other"))
                self.assertEqual(store.pop(key), "v")
                self.assertTrue(store.add(key, "again"))
                self.assertTrue(store.delete(key))
                self.assertFalse(store.delete(key))
            store.set(("a", 1), 1)
            self.assertIn(("a", 1), list(store))
            self.assertIn(("a", 1), store.keys())

    def test_missing_keys(self):
        for store in self._each():
            with self.assertRaises(KeyError):
                store["missing"]
            with self.assertRaises(KeyError):
                store[("missing", 1)]
            with self.assertRaises(KeyError):
                del store["missing"]
            self.assertIsNone(store.pop("missing"))
            self.assertEqual(store.get("missing", "default"), "default")

    def test_add_with_expire_and_tag(self):
        for store in self._each():
            self.assertTrue(store.add("k", 1, expire=timedelta(hours=1), tag="t"))
            value, expire_time, tag = store.get("k", expire_time=True, tag=True)
            self.assertEqual((value, tag), (1, "t"))
            self.assertGreater(expire_time, time.time())
            self.assertEqual(store.evict_tag("t"), 1)

    def test_pop_with_metadata(self):
        for store in self._each():
            store.set("k", "v", expire=3600, tag="t")
            value, expire_time, tag = store.pop("k", expire_time=True, tag=True)
            self.assertEqual((value, tag), ("v", "t"))
            self.assertGreater(expire_time, time.time())
            self.assertEqual(store.pop("k", default="d", tag=True), ("d", None))
            self.assertEqual(store.get("k", default="d", expire_time=True), ("d", None))

    def test_touch_and_expire(self):
        for store in self._each():
            store.set(7, "v", expire=3600)
            self.assertTrue(store.touch(7, expire=0))
            store.expire()
            self.assertNotIn(7, store)
            self.assertFalse(store.touch("missing"))

    def test_read_true_is_rejected(self):
        for store in self._each():
            for call in (lambda: store.set("k", b"v", read=True),
                         lambda: store.add("k", b"v", read=True),
                         lambda: store.get("k", read=True)):
                with self.assertRaises(NotImplementedError):
                    call()

    def test_counters(self):
        for store in self._each():
            self.assertEqual(store.incr(("n", 1)), 1)
            self.assertEqual(store.incr(("n", 1), delta=5), 6)
            self.assertEqual(store.decr(("n", 1), delta=2), 4)
            self.assertEqual(store.decr("fresh", default=10), 9)

    def test_memoize_typed_and_version_aware(self):
        for store in self._each():
            calls = []

            @store.memoize(typed=True, version_aware=True, tag="memo")
            def double(x):
                calls.append(x)
                return 2 * x

            self.assertEqual((double(1), double(1), double(1.0)), (2, 2, 2.0))
            self.assertEqual(calls, [1, 1.0])
            self.assertIn(double.__cache_key__(1), store)
            self.assertEqual(store.evict_tag("memo"), 2)

    def test_lock_with_tag_and_expire(self):
        for store in self._each():
            lock = store.lock(("resource", 1), expire=30, tag="locks")
            with lock:
                self.assertTrue(lock.locked())
            self.assertFalse(lock.locked())
            self.assertFalse(lock.release())

    def test_maintenance(self):
        for store in self._each():
            store.set("k", "v" * 20000)
            store.evict()
            store.clear()
            self.assertEqual(len(store), 0)
            self.assertTrue(store.check(fix=True).ok)

    def test_cache_repr_names_path_and_count(self):
        cache = Cache(f"{self.tmp}/repr")
        cache["k"] = 1
        self.assertIn("count=1", repr(cache))

    def test_set_max_cache_size(self):
        for store in self._each():
            store.set_max_cache_size(1_000_000)
            store.set("k", "v")
            self.assertEqual(store["k"], "v")


class RawBindingEdgeCases(_TempDir):
    def test_an_empty_value_reads_back_as_an_empty_memoryview(self):
        cache = Cache(f"{self.tmp}/empty")
        raw = type(cache).__mro__[1]
        raw.set(cache, "empty", b"")
        view = raw.get(cache, "empty").memoryview()
        self.assertEqual(bytes(view), b"")

    def test_a_serializer_may_return_empty_bytes(self):
        class RawBytes:
            name = "raw-bytes"
            dumps = staticmethod(bytes)
            loads = staticmethod(bytes)

        cache = Cache(f"{self.tmp}/raw", serializer=RawBytes())
        cache["empty"] = b""
        cache["full"] = b"data"
        self.assertEqual((cache["empty"], cache["full"]), (b"", b"data"))


class IndexContract(_TempDir):
    STORES = (Index, FanoutIndex)

    def _each(self, *mappings, **items):
        for cls in self.STORES:
            with self.subTest(cls.__name__):
                yield cls(f"{self.tmp}/{cls.__name__}", *mappings, **items)

    def test_seeded_from_mappings_and_items(self):
        for store in self._each({"a": 1}, {"b": 2}, c=3):
            self.assertEqual(dict(store.items()), {"a": 1, "b": 2, "c": 3})
            self.assertEqual(sorted(store.values()), [1, 2, 3])

    def test_non_str_keys_through_every_method(self):
        for store in self._each():
            for key in NON_STR_KEYS:
                store.set(key, "v")
                self.assertEqual(store.get(key), "v")
                self.assertEqual(store[key], "v")
                self.assertIn(key, store)
                self.assertTrue(store.exists(key))
                self.assertFalse(store.add(key, "other"))
                self.assertEqual(store.pop(key), "v")
                self.assertTrue(store.add(key, "again"))
                self.assertTrue(store.delete(key))
                self.assertFalse(store.delete(key))

    def test_missing_keys(self):
        for store in self._each():
            with self.assertRaises(KeyError):
                store["missing"]
            with self.assertRaises(KeyError):
                store[("missing", 1)]
            with self.assertRaises(KeyError):
                store.pop("missing")
            self.assertEqual(store.pop("missing", "d"), "d")
            with self.assertRaises(KeyError):
                store.peekitem()
            with self.assertRaises(KeyError):
                store.popitem()

    def test_counters(self):
        for store in self._each():
            self.assertEqual(store.incr(("n", 1)), 1)
            self.assertEqual(store.decr(("n", 1), delta=3), -2)

    def test_peekitem_and_popitem_follow_key_order(self):
        for store in self._each({"b": 2, "a": 1, "c": 3}):
            self.assertEqual(store.peekitem(), ("c", 3))
            self.assertEqual(store.peekitem(last=False), ("a", 1))
            self.assertEqual(store.popitem(last=False), ("a", 1))
            self.assertEqual(len(store), 2)

    def test_transact_and_maintenance(self):
        for store in self._each():
            key_args = ("k",) if isinstance(store, FanoutIndex) else ()
            with store.transact(*key_args):
                store["k"] = 1
            self.assertEqual(store["k"], 1)
            self.assertTrue(store.check(fix=True).ok)
            store.clear()
            self.assertEqual(len(store), 0)


class SerializerChoiceParity(_TempDir):
    def test_msgspec_roundtrips_on_every_store(self):
        try:
            import msgspec  # noqa: F401
        except ImportError:
            self.skipTest("msgspec not installed")
        for cls in (Cache, FanoutCache, Index, FanoutIndex):
            with self.subTest(cls.__name__):
                store = cls(f"{self.tmp}/{cls.__name__}", serializer=MsgspecSerializer())
                store["k"] = {"a": [1, 2]}
                self.assertEqual(store["k"], {"a": [1, 2]})
                self.assertEqual(store.serializer.name, "msgspec")


if __name__ == "__main__":
    unittest.main()
