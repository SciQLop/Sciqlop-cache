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

    def test_memoize_ignore_by_position_and_name(self):
        for store in self._each():
            calls = []

            @store.memoize(ignore={"session", 2})
            def fetch(entity, session, verbose=False):
                calls.append(entity)
                return entity * 10

            unpicklable = lambda: None  # noqa: E731 -- must never reach the serializer
            self.assertEqual(fetch(1, unpicklable), 10)
            self.assertEqual(fetch(1, session=object()), 10)
            self.assertEqual(fetch(1, object(), True), 10)
            self.assertEqual(fetch(2, object()), 20)
            self.assertEqual(calls, [1, 2])

    def test_memoize_custom_key(self):
        for store in self._each():
            calls = []

            @store.memoize(key=lambda path, **_: path.lower())
            def load(path, retries=3):
                calls.append(path)
                return len(path)

            self.assertEqual((load("A.txt"), load("a.TXT", retries=5)), (5, 5))
            self.assertEqual(calls, ["A.txt"])
            self.assertIn(load.__cache_key__("a.txt"), store)

    def test_memoize_async_function_caches_the_result(self):
        import asyncio

        for store in self._each():
            calls = []

            @store.memoize(tag="async")
            async def compute(x):
                calls.append(x)
                await asyncio.sleep(0)
                return x * 2

            async def run():
                return [await compute(3), await compute(3), await compute(4)]

            self.assertTrue(asyncio.iscoroutinefunction(compute))
            self.assertEqual(asyncio.run(run()), [6, 6, 8])
            self.assertEqual(calls, [3, 4])
            self.assertEqual(store.evict_tag("async"), 2)

    def test_get_set_delete_many(self):
        for store in self._each():
            items = {f"k{i}": i for i in range(50)}
            items[("tuple", 1)] = "t"
            self.assertIsNone(store.set_many(items, expire=3600, tag="batch"))
            store.set("old", 1, expire=-1)
            wanted = ["k0", "k49", ("tuple", 1), "missing", "old"]
            self.assertEqual(store.get_many(wanted), {"k0": 0, "k49": 49, ("tuple", 1): "t"})
            self.assertEqual(store.get_many([]), {})
            self.assertEqual(store.delete_many(["k0", "k1", "missing", ("tuple", 1)]), 3)
            self.assertNotIn("k0", store)
            self.assertEqual(len(store.get_many(list(items))), 48)
            self.assertEqual(store.evict_tag("batch"), 48)

    def test_set_many_is_atomic_on_failure(self):
        for store in self._each():
            if isinstance(store, FanoutCache):
                continue  # one transaction per shard, documented
            class Unpicklable:
                def __reduce__(self):
                    raise TypeError("nope")
            with self.assertRaises(TypeError):
                store.set_many({"a": 1, "b": Unpicklable()})
            self.assertNotIn("a", store)

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

    def test_get_set_delete_many(self):
        for store in self._each():
            store.set_many({"a": 1, "b": 2, ("c",): 3})
            self.assertEqual(store.get_many(["a", ("c",), "zz"]), {"a": 1, ("c",): 3})
            self.assertEqual(store.delete_many(["a", "zz"]), 1)
            self.assertEqual(sorted(map(str, store.keys())), ["('c',)", "b"])

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

    def test_index_peekitem_and_popitem_follow_key_order(self):
        index = Index(f"{self.tmp}/ordered", {"b": 2, "a": 1, "c": 3})
        self.assertEqual(index.peekitem(), ("c", 3))
        self.assertEqual(index.peekitem(last=False), ("a", 1))
        self.assertEqual(index.popitem(), ("c", 3))          # last by default
        self.assertEqual(index.popitem(last=False), ("a", 1))
        self.assertEqual(dict(index.items()), {"b": 2})

    def test_peekitem_ends_are_the_ends_of_keys(self):
        for store in self._each({"b": 2, "a": 1, "c": 3}):
            keys = store.keys()
            self.assertEqual(store.peekitem()[0], keys[-1])
            self.assertEqual(store.peekitem(last=False)[0], keys[0])

    def test_popitem_removes_what_peekitem_shows(self):
        # FanoutIndex order is per shard, and shards depend on the platform's hash.
        for store in self._each({"b": 2, "a": 1, "c": 3}):
            for last in (True, False):
                peeked = store.peekitem(last=last)
                self.assertEqual(store.popitem(last=last), peeked)
                self.assertNotIn(peeked[0], store)
            self.assertEqual(len(store), 1)

    def test_transact_and_maintenance(self):
        for store in self._each():
            key_args = ("k",) if isinstance(store, FanoutIndex) else ()
            with store.transact(*key_args):
                store["k"] = 1
            self.assertEqual(store["k"], 1)
            self.assertTrue(store.check(fix=True).ok)
            store.clear()
            self.assertEqual(len(store), 0)


ALL_STORES = (Cache, FanoutCache, Index, FanoutIndex)


def _transact(store, key="k"):
    return store.transact(key) if isinstance(store, (FanoutCache, FanoutIndex)) else store.transact()


class ExceptionsEscape(_TempDir):
    def test_an_exception_in_transact_rolls_back_and_propagates(self):
        for cls in ALL_STORES:
            with self.subTest(cls.__name__):
                store = cls(f"{self.tmp}/{cls.__name__}")
                store["k"] = "before"
                with self.assertRaises(ZeroDivisionError):
                    with _transact(store):
                        store["k"] = "during"
                        1 / 0
                self.assertEqual(store["k"], "before")

    def test_with_store_does_not_swallow_exceptions(self):
        for cls in ALL_STORES:
            with self.subTest(cls.__name__):
                store = cls(f"{self.tmp}/{cls.__name__}")
                with self.assertRaises(ZeroDivisionError):
                    with store:
                        1 / 0
                store["still"] = "usable"

    def test_an_exception_in_a_lock_releases_it_and_propagates(self):
        for cls in (Cache, FanoutCache):
            with self.subTest(cls.__name__):
                lock = cls(f"{self.tmp}/{cls.__name__}").lock("resource")
                with self.assertRaises(ZeroDivisionError):
                    with lock:
                        1 / 0
                self.assertFalse(lock.locked())


class LockSemantics(_TempDir):
    def test_a_held_lock_blocks_a_second_holder(self):
        import threading
        cache = Cache(f"{self.tmp}/locks")
        first, second = cache.lock("resource"), cache.lock("resource")
        first.acquire()
        taken = threading.Event()

        def take():
            second.acquire()
            taken.set()

        thread = threading.Thread(target=take)
        thread.start()
        self.assertFalse(taken.wait(0.3), "a second holder got a held lock")
        first.release()
        self.assertTrue(taken.wait(5))
        thread.join()
        second.release()

    def test_an_expired_lock_can_be_taken(self):
        cache = Cache(f"{self.tmp}/locks")
        cache.lock("resource", expire=0.2).acquire()   # never released: a crashed holder
        time.sleep(1.3)
        taken = cache.lock("resource")
        taken.acquire()
        self.assertTrue(taken.locked())

    def test_a_tagged_lock_goes_with_its_tag(self):
        cache = Cache(f"{self.tmp}/locks")
        lock = cache.lock("resource", tag="locks")
        lock.acquire()
        self.assertEqual(cache.evict_tag("locks"), 1)
        self.assertFalse(lock.locked())


class ConstructorsAndDefaults(_TempDir):
    def test_the_directory_argument_is_used(self):
        import os
        for cls, alias in ((Cache, "cache_path"), (FanoutCache, "cache_path"),
                           (Index, "path"), (FanoutIndex, "path")):
            with self.subTest(cls.__name__):
                for kwargs in ({"directory": f"{self.tmp}/{cls.__name__}-d"},
                               {alias: f"{self.tmp}/{cls.__name__}-a"}):
                    store = cls(**kwargs)
                    wanted = os.path.realpath(next(iter(kwargs.values())))
                    self.assertEqual(os.path.realpath(store.path()), wanted)

    def test_no_size_limit_by_default(self):
        for cls in (Cache, FanoutCache):
            with self.subTest(cls.__name__):
                store = cls(f"{self.tmp}/{cls.__name__}")
                for i in range(30):
                    store[f"k{i}"] = b"x" * 20_000
                store.evict()
                self.assertEqual(len(store), 30)

    def test_max_size_is_enforced(self):
        cache = Cache(f"{self.tmp}/bounded", max_size=100_000)
        for i in range(20):
            cache[f"k{i}"] = b"x" * 20_000
        cache.evict()
        self.assertLessEqual(cache.size(), 100_000)
        self.assertLess(len(cache), 20)
        self.assertIn("k19", cache, "the most recently used entry must survive")

    def test_fanout_defaults(self):
        self.assertEqual(FanoutCache(f"{self.tmp}/fc").shard_count(), 8)
        self.assertEqual(FanoutIndex(f"{self.tmp}/fi").shard_count(), 8)

    def test_decr_defaults(self):
        for cls in ALL_STORES:
            with self.subTest(cls.__name__):
                store = cls(f"{self.tmp}/{cls.__name__}")
                self.assertEqual(store.decr("n"), -1)


class KeyEncodingOnDisk(_TempDir):
    """Non-str keys are stored in this encoding: existing caches depend on it."""

    GOLDEN = {
        42: "\x00i42",
        True: "\x00i1",
        3.5: "\x00f3.5",
        b"x\xff": "\x00bx\xff",
        ("sensor", 7): "\x00pgASVDgAAAAAAAACMBnNlbnNvcpRLB4aULg==",
        frozenset({1}): "\x00pgASVBgAAAAAAAAAoSwGRlC4=",
    }

    def test_stored_key_strings(self):
        cache = Cache(f"{self.tmp}/keys")
        for key in self.GOLDEN:
            cache[key] = repr(key)
        raw_keys = set(type(cache).__mro__[1].keys(cache))
        self.assertEqual(raw_keys, set(self.GOLDEN.values()))
        for key in self.GOLDEN:
            self.assertEqual(cache[key], repr(key))



class GetManyIsOneSnapshot(_TempDir):
    """get_many reads every key from one snapshot: a writer updating two keys in
    one transaction is never seen half done (no exclusive lock taken)."""

    def test_never_sees_half_a_transaction(self):
        import threading
        for cls in (Cache, Index):
            with self.subTest(cls.__name__):
                store = cls(f"{self.tmp}/{cls.__name__}")
                store.set_many({"a": 0, "b": 0})
                stop = threading.Event()

                def writer():
                    i = 0
                    while not stop.is_set():
                        i += 1
                        with store.transact():
                            store["a"] = i
                            store["b"] = i

                t = threading.Thread(target=writer)
                t.start()
                torn = []
                try:
                    deadline = time.monotonic() + 3
                    while time.monotonic() < deadline and not torn:
                        got = store.get_many(["a", "b"])
                        if got["a"] != got["b"]:
                            torn.append(got)
                finally:
                    stop.set()
                    t.join()
                self.assertEqual(torn, [])


def _value_files(directory):
    """How many values are stored as files (anything but the SQLite files)."""
    import os
    return sum(1 for root, _, files in os.walk(directory) for f in files
               if not f.startswith("sciqlop-cache.db"))


class MaxBlobSize(_TempDir):
    """max_blob_size: values up to it stay in the database, bigger ones go to files.
    Recorded in the store, like max_size."""
    STORES = (Cache, Index, FanoutCache, FanoutIndex)

    def _path(self, cls, name):
        return f"{self.tmp}/{cls.__name__}-{name}"

    def test_threshold_decides_database_or_file(self):
        mid = b"m" * 100_000
        for cls in self.STORES:
            with self.subTest(cls.__name__):
                default = cls(self._path(cls, "default"))
                default["small"] = b"s" * 100
                default["mid"] = mid
                self.assertEqual(_value_files(self._path(cls, "default")), 1)

                roomy = cls(self._path(cls, "roomy"), max_blob_size=1 << 20)
                roomy["mid"] = mid
                self.assertEqual(_value_files(self._path(cls, "roomy")), 0)
                self.assertEqual(roomy["mid"], mid)
                self.assertEqual(roomy.max_blob_size(), 1 << 20)

                files_only = cls(self._path(cls, "files"), max_blob_size=0)
                files_only["small"] = b"s"
                self.assertEqual(_value_files(self._path(cls, "files")), 1)
                self.assertEqual(files_only["small"], b"s")

    def test_recorded_and_reused_on_reopen(self):
        for cls in self.STORES:
            with self.subTest(cls.__name__):
                path = self._path(cls, "reopen")
                cls(path, max_blob_size=1 << 20).close()
                reopened = cls(path)
                self.assertEqual(reopened.max_blob_size(), 1 << 20)
                reopened["mid"] = b"m" * 100_000
                self.assertEqual(_value_files(path), 0)
                self.assertEqual(cls(path, max_blob_size=10).max_blob_size(), 10)

    def test_default_and_validation(self):
        for cls in self.STORES:
            with self.subTest(cls.__name__):
                self.assertEqual(cls(self._path(cls, "d")).max_blob_size(), 8192)
                with self.assertRaises(ValueError):
                    cls(self._path(cls, "neg"), max_blob_size=-1)

    def test_diskcache_min_file_size_is_honoured(self):
        # diskcache stores a value as a file from disk_min_file_size bytes on.
        for cls in (Cache, FanoutCache):
            with self.subTest(cls.__name__):
                store = cls(self._path(cls, "dc"), disk_min_file_size=1 << 20)
                self.assertEqual(store.max_blob_size(), (1 << 20) - 1)


class FanoutDocstrings(unittest.TestCase):
    def test_fanout_methods_share_the_documentation(self):
        self.assertEqual(FanoutCache.get.__doc__, Cache.get.__doc__)
        self.assertEqual(FanoutCache.memoize.__doc__, Cache.memoize.__doc__)
        self.assertEqual(FanoutIndex.items.__doc__, Index.items.__doc__)
        self.assertIn("shard", FanoutCache.transact.__doc__)


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


class _ReprSerializer:
    name = "repr-text"

    def dumps(self, value):
        return repr(value).encode()

    def loads(self, data):
        return bytes(data).decode()


class DiskcacheAttributes(_TempDir):
    def test_directory_is_the_folder(self):
        for cls in (Cache, FanoutCache, Index, FanoutIndex):
            with self.subTest(cls.__name__):
                folder = f"{self.tmp}/{cls.__name__}"
                self.assertEqual(cls(folder).directory, folder)

    def test_reopening_a_custom_serializer_store_says_to_pass_it(self):
        for cls in (Cache, FanoutCache, Index, FanoutIndex):
            with self.subTest(cls.__name__):
                folder = f"{self.tmp}/{cls.__name__}"
                store = cls(folder, serializer=_ReprSerializer())
                store["k"] = 1
                store.close()
                with self.assertRaisesRegex(ValueError, r"repr-text.*serializer="):
                    cls(folder)
                self.assertEqual(cls(folder, serializer=_ReprSerializer())["k"], "1")


if __name__ == "__main__":
    unittest.main()


class GetManyReturnsSmallValuesAsBytes(_TempDir):
    """A small value comes back as bytes: one Python object instead of a Buffer
    plus its memoryview, which cost more than reading it. A value stored as a
    file stays a Buffer, so big arrays are not copied."""

    def test_raw_batch_read_types(self):
        for cls in ALL_STORES:
            with self.subTest(cls.__name__):
                store = cls(f"{self.tmp}/{cls.__name__}")
                small, big = b"s" * 100, b"b" * (64 * 1024)
                store.set("small", small)
                store.set("big", big)
                raw = store._get_many(["small", "big", "missing"])
                self.assertIs(type(raw[0]), bytes)
                self.assertNotIsInstance(raw[1], bytes)
                self.assertIsNone(raw[2])
                self.assertEqual(store.get_many(["small", "big", "missing"]),
                                 {"small": small, "big": big})

    def test_the_boundary_follows_max_blob_size(self):
        store = Index(f"{self.tmp}/blob", max_blob_size=100)
        store.set("below", b"a" * 50)  # pickled: a few bytes more
        store.set("above", b"a" * 200)
        below, above = store._get_many(["below", "above"])
        self.assertIs(type(below), bytes)
        self.assertNotIsInstance(above, bytes)


class SetManyTakesPairs(_TempDir):
    """Like dict.update, set_many takes a mapping or an iterable of (key, value)
    pairs, so a caller holding pairs doesn't build a dict first."""

    def test_pairs(self):
        for cls in ALL_STORES:
            with self.subTest(cls.__name__):
                store = cls(f"{self.tmp}/{cls.__name__}")
                store.set_many([("a", 1), (("t", 2), "x")])
                store.set_many(iter([("b", 2)]))
                self.assertEqual(store.get_many(["a", ("t", 2), "b"]), {"a": 1, ("t", 2): "x", "b": 2})

