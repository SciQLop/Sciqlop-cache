import shutil
import types
import unittest
import tempfile

import numpy as np

from pysciqlop_cache import Cache, MsgspecSerializer, PickleSerializer

try:
    import msgspec
    _has_msgspec = True
except ImportError:
    _has_msgspec = False


class NestedAxis:
    def __init__(self, name="", data=None):
        self.name = name
        self.data = data if data is not None else np.array([])


class FakeSpeasyVariable:
    """Mimics SpeasyVariable's nested structure for testing."""

    def __init__(self, name="", values=None, axes=None, meta=None):
        self.name = name
        self.values = values if values is not None else np.array([])
        self.axes = axes or []
        self.meta = meta or {}


class TestPickleSerializer(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.cache = Cache(
            self.tmp_dir, serializer=PickleSerializer()
        )

    def tearDown(self):
        del self.cache
        shutil.rmtree(self.tmp_dir)

    def test_basic_types(self):
        for key, value in [
            ("int", 42),
            ("float", 3.14),
            ("str", "hello"),
            ("bytes", b"\x00\x01\x02"),
            ("list", [1, "two", 3.0]),
            ("dict", {"a": 1, "b": [2, 3]}),
            ("none", None),
            ("bool", True),
            ("tuple", (1, 2, 3)),
            ("set", {1, 2, 3}),
        ]:
            self.cache.set(key, value)
            result = self.cache.get(key)
            self.assertEqual(result, value, f"Failed for {key}")

    def test_numpy_array(self):
        arr = np.arange(100, dtype=np.float64).reshape(10, 10)
        self.cache.set("arr", arr)
        result = self.cache.get("arr")
        np.testing.assert_array_equal(result, arr)

    def test_nested_object(self):
        var = FakeSpeasyVariable(
            name="B_GSE",
            values=np.random.rand(100, 3),
            axes=[
                NestedAxis("time", np.linspace(0, 1, 100)),
                NestedAxis("component", np.array([0, 1, 2])),
            ],
            meta={"UNITS": "nT", "CATDESC": "Magnetic field"},
        )
        self.cache.set("var", var)
        result = self.cache.get("var")
        self.assertEqual(result.name, var.name)
        np.testing.assert_array_equal(result.values, var.values)
        self.assertEqual(len(result.axes), 2)
        self.assertEqual(result.meta, var.meta)


@unittest.skipUnless(_has_msgspec, "msgspec not installed")
class TestMsgspecSerializer(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.cache = Cache(
            self.tmp_dir, serializer=MsgspecSerializer(types=[FakeSpeasyVariable, NestedAxis])
        )

    def tearDown(self):
        del self.cache
        shutil.rmtree(self.tmp_dir)

    def test_basic_types(self):
        for key, value in [
            ("int", 42),
            ("float", 3.14),
            ("str", "hello"),
            ("bytes", b"\x00\x01\x02"),
            ("list", [1, "two", 3.0]),
            ("dict", {"a": 1, "b": [2, 3]}),
            ("none", None),
            ("bool", True),
        ]:
            self.cache.set(key, value)
            result = self.cache.get(key)
            self.assertEqual(result, value, f"Failed for {key}")

    def test_numpy_1d(self):
        arr = np.arange(50, dtype=np.float32)
        self.cache.set("arr", arr)
        result = self.cache.get("arr")
        np.testing.assert_array_equal(result, arr)
        self.assertEqual(result.dtype, arr.dtype)

    def test_numpy_2d(self):
        arr = np.random.rand(10, 3).astype(np.float64)
        self.cache.set("arr", arr)
        result = self.cache.get("arr")
        np.testing.assert_array_equal(result, arr)
        self.assertEqual(result.shape, arr.shape)

    def test_numpy_fortran_order(self):
        arr = np.asfortranarray(np.arange(12).reshape(3, 4))
        self.cache.set("arr", arr)
        result = self.cache.get("arr")
        np.testing.assert_array_equal(result, arr)

    def test_numpy_integer_dtypes(self):
        for dtype in [np.int8, np.int16, np.int32, np.int64, np.uint8]:
            arr = np.array([1, 2, 3], dtype=dtype)
            self.cache.set(f"arr_{dtype.__name__}", arr)
            result = self.cache.get(f"arr_{dtype.__name__}")
            np.testing.assert_array_equal(result, arr)
            self.assertEqual(result.dtype, arr.dtype)

    def test_numpy_scalar_types(self):
        self.cache.set("np_int", np.int64(42))
        self.assertEqual(self.cache.get("np_int"), 42)
        self.cache.set("np_float", np.float64(3.14))
        self.assertAlmostEqual(self.cache.get("np_float"), 3.14)
        self.cache.set("np_bool", np.bool_(True))
        self.assertEqual(self.cache.get("np_bool"), True)

    def test_nested_object(self):
        var = FakeSpeasyVariable(
            name="B_GSE",
            values=np.random.rand(100, 3),
            axes=[
                NestedAxis("time", np.linspace(0, 1, 100)),
                NestedAxis("component", np.array([0, 1, 2])),
            ],
            meta={"UNITS": "nT", "CATDESC": "Magnetic field"},
        )
        self.cache.set("var", var)
        result = self.cache.get("var")
        self.assertEqual(result.name, var.name)
        np.testing.assert_array_equal(result.values, var.values)
        self.assertEqual(len(result.axes), 2)
        self.assertEqual(result.axes[0].name, "time")
        np.testing.assert_array_equal(result.axes[0].data, var.axes[0].data)
        self.assertEqual(result.meta, var.meta)

    def test_deeply_nested(self):
        """Test 3 levels of nesting: dict -> object -> numpy array."""
        var = FakeSpeasyVariable(
            name="deep",
            values=np.array([1.0, 2.0, 3.0]),
            meta={"nested": {"level2": {"level3": [1, 2, 3]}}},
        )
        payload = {"variables": [var], "timestamp": 12345}
        self.cache.set("deep", payload)
        result = self.cache.get("deep")
        self.assertEqual(result["timestamp"], 12345)
        restored_var = result["variables"][0]
        self.assertEqual(restored_var.name, "deep")
        np.testing.assert_array_equal(restored_var.values, var.values)

    def test_set_becomes_list(self):
        """msgpack has no set type — sets are encoded as arrays."""
        self.cache.set("s", {1, 2, 3})
        result = self.cache.get("s")
        self.assertEqual(sorted(result), [1, 2, 3])
        self.assertIsInstance(result, list)

    def test_memoize_with_msgspec(self):
        call_count = 0

        @self.cache.memoize()
        def compute(x):
            nonlocal call_count
            call_count += 1
            return x * 2

        self.assertEqual(compute(5), 10)
        self.assertEqual(compute(5), 10)
        self.assertEqual(call_count, 1)


class TestSerializerPersistence(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmp_dir)

    @unittest.skipUnless(_has_msgspec, "msgspec not installed")
    def test_reopen_without_serializer_arg(self):
        cache = Cache(self.tmp_dir, serializer=MsgspecSerializer())
        cache.set("key", "value")
        del cache

        # Reopen without specifying serializer — should auto-detect
        cache2 = Cache(self.tmp_dir)
        self.assertEqual(cache2.get("key"), "value")
        self.assertEqual(cache2.serializer.name, "msgspec")

    def test_reopen_with_matching_serializer(self):
        cache = Cache(self.tmp_dir, serializer=PickleSerializer())
        cache.set("key", 42)
        del cache

        cache2 = Cache(self.tmp_dir, serializer=PickleSerializer())
        self.assertEqual(cache2.get("key"), 42)

    @unittest.skipUnless(_has_msgspec, "msgspec not installed")
    def test_reopen_with_wrong_serializer_raises(self):
        cache = Cache(self.tmp_dir, serializer=MsgspecSerializer())
        cache.set("key", "value")
        del cache

        with self.assertRaises(ValueError):
            Cache(self.tmp_dir, serializer=PickleSerializer())

    def test_default_is_pickle(self):
        cache = Cache(self.tmp_dir)
        self.assertEqual(cache.serializer.name, "pickle")


class TestSerializerErrors(unittest.TestCase):
    def test_unknown_serializer_name(self):
        from pysciqlop_cache.serializers import get_serializer_by_name
        with self.assertRaisesRegex(ValueError, "Unknown serializer"):
            get_serializer_by_name("nope")

    @unittest.skipUnless(_has_msgspec, "msgspec not installed")
    def test_msgspec_rejects_what_it_cannot_encode(self):
        with self.assertRaises(TypeError):
            MsgspecSerializer().dumps(object())

    @unittest.skipUnless(_has_msgspec, "msgspec not installed")
    def test_msgspec_rejects_an_unknown_extension_type(self):
        data = msgspec.msgpack.encode(msgspec.msgpack.Ext(99, b"x"))
        with self.assertRaisesRegex(ValueError, "Unknown ext type code"):
            MsgspecSerializer().loads(data)


@unittest.skipUnless(_has_msgspec, "msgspec not installed")
class TestMsgspecOnDiskFormat(unittest.TestCase):
    """Bytes written by an earlier version must still decode: the extension
    codes and layouts below are persisted in existing caches."""

    GOLDEN = {
        "int16 array": b"\xc7'\x01\x1d\x00\x00\x00\x83\xa5dtype\xa5int16\xa5shape\x91\x03"
                       b"\xa5order\xa1F\x00\x00\x01\x00\x02\x00",
        "dict of float64 array": b"\x81\xa1k\xc7+\x01\x1f\x00\x00\x00\x83\xa5dtype\xa7float64"
                                 b"\xa5shape\x91\x01\xa5order\xa1F\x00\x00\x00\x00\x00\x00\xf8?",
        "object": b"\xc71\x02\x82\xa8__type__\xb5types.SimpleNamespace\xa8__data__\x82\xa1a"
                  b"\x01\xa1b\xa1x",
    }

    def values(self):
        import types
        return {
            "int16 array": np.arange(3, dtype="<i2"),
            "dict of float64 array": {"k": np.array([1.5], dtype="<f8")},
            "object": types.SimpleNamespace(a=1, b="x"),
        }

    def test_golden_bytes_decode(self):
        ser = MsgspecSerializer(types=[types.SimpleNamespace])
        for name, value in self.values().items():
            with self.subTest(name):
                decoded = ser.loads(self.GOLDEN[name])
                if isinstance(value, np.ndarray):
                    np.testing.assert_array_equal(decoded, value)
                    self.assertEqual(decoded.dtype, value.dtype)
                elif isinstance(value, dict):
                    np.testing.assert_array_equal(decoded["k"], value["k"])
                else:
                    self.assertEqual(vars(decoded), vars(value))

    def test_encoding_is_unchanged(self):
        ser = MsgspecSerializer(types=[types.SimpleNamespace])
        for name, value in self.values().items():
            with self.subTest(name):
                self.assertEqual(ser.dumps(value), self.GOLDEN[name])




class _Exploit:
    """Unpickling this calls ``record(...)``: stands in for os.system."""
    def __reduce__(self):
        return (_record_call, ("pwned",))


_calls = []


def _record_call(arg):
    _calls.append(arg)
    return arg


def _plant_key(store, obj):
    """Write a key the way a hostile process with access to the cache folder could:
    an encoded key whose payload is an arbitrary pickle."""
    import base64
    import pickle
    raw = "\x00p" + base64.b64encode(pickle.dumps(obj, protocol=4)).decode("ascii")
    store.set_meta("encoded_keys", "1")
    type(store).__mro__[1].set(store, raw, b"x")


@unittest.skipUnless(_has_msgspec, "msgspec not installed")
class KeysOfNonPickleStores(unittest.TestCase):
    """A store that chose a serializer without pickle never unpickles arbitrary
    objects, not even through its keys (pickle docs: "Restricting Globals")."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        _calls.clear()

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_plain_hashable_keys_still_round_trip(self):
        import datetime
        cache = Cache(self.tmp, serializer=MsgspecSerializer())
        keys = [("sensor", 7), frozenset({1, 2}), (None, True, 2.5, b"b", ("nested",)),
                datetime.datetime(2026, 1, 2, 3, 4, 5), datetime.date(2026, 1, 2),
                datetime.timedelta(seconds=3), complex(1, 2)]
        for k in keys:
            cache[k] = 1
        self.assertCountEqual(cache.keys(), keys)
        self.assertCountEqual(list(cache), keys)

    def test_planted_key_is_refused_not_executed(self):
        import pickle
        cache = Cache(self.tmp, serializer=MsgspecSerializer())
        _plant_key(cache, _Exploit())
        with self.assertRaises(pickle.UnpicklingError):
            cache.keys()
        with self.assertRaises(pickle.UnpicklingError):
            list(cache)
        self.assertEqual(_calls, [])

    def test_pickle_stores_keep_full_key_unpickling(self):
        cache = Cache(self.tmp, serializer=PickleSerializer())
        _plant_key(cache, _Exploit())
        self.assertEqual(cache.keys(), ["pwned"])


def _object_entry(type_path, data):
    """The bytes a hostile process could plant: an object extension naming any class."""
    payload = msgspec.msgpack.encode({"__type__": type_path, "__data__": data})
    return msgspec.msgpack.encode(msgspec.msgpack.Ext(2, payload))


@unittest.skipUnless(_has_msgspec, "msgspec not installed")
class MsgspecObjectsAreAllowListed(unittest.TestCase):
    """Building an object named by stored data is a deserialization hole (the
    jsonpickle class of bug, CVE-2020-22083): only listed classes are built."""

    def test_planted_object_imports_nothing(self):
        import sys
        sys.modules.pop("this", None)  # importing ``this`` prints; it stands in for any module
        with self.assertRaises(ValueError):
            MsgspecSerializer().loads(_object_entry("this.Anything", {}))
        self.assertNotIn("this", sys.modules)

    def test_planted_object_of_an_imported_class_is_not_built(self):
        with self.assertRaises(ValueError):
            MsgspecSerializer().loads(_object_entry("types.SimpleNamespace", {"a": 1}))

    def test_unlisted_class_is_refused_on_write(self):
        with self.assertRaises(TypeError):
            MsgspecSerializer().dumps(NestedAxis("time"))

    def test_listed_classes_round_trip(self):
        ser = MsgspecSerializer(types=[FakeSpeasyVariable, NestedAxis])
        var = FakeSpeasyVariable("B", np.arange(3.0), [NestedAxis("time", np.arange(3))], {"u": "nT"})
        out = ser.loads(ser.dumps(var))
        self.assertIsInstance(out, FakeSpeasyVariable)
        self.assertIsInstance(out.axes[0], NestedAxis)
        np.testing.assert_array_equal(out.values, var.values)

    def test_a_listed_class_does_not_allow_another(self):
        ser = MsgspecSerializer(types=[NestedAxis])
        with self.assertRaises(ValueError):
            ser.loads(MsgspecSerializer(types=[FakeSpeasyVariable]).dumps(FakeSpeasyVariable("x")))

    def test_types_must_be_classes(self):
        with self.assertRaises(TypeError):
            MsgspecSerializer(types=["types.SimpleNamespace"])

    def test_registered_class_round_trips(self):
        ser = MsgspecSerializer()
        ser.register(NestedAxis)
        out = ser.loads(ser.dumps(NestedAxis("time")))
        self.assertIsInstance(out, NestedAxis)
        self.assertEqual(out.name, "time")

    def test_register_works_as_a_decorator(self):
        ser = MsgspecSerializer()

        @ser.register
        class Point:
            def __init__(self, x):
                self.x = x

        self.assertIs(type(ser.loads(ser.dumps(Point(1)))), Point)

    def test_register_only_affects_its_serializer(self):
        MsgspecSerializer().register(NestedAxis)
        with self.assertRaises(TypeError):
            MsgspecSerializer().dumps(NestedAxis("time"))

    def test_register_takes_classes_only(self):
        with self.assertRaises(TypeError):
            MsgspecSerializer().register("types.SimpleNamespace")

    def test_registering_works_on_an_open_cache(self):
        with tempfile.TemporaryDirectory() as d:
            ser = MsgspecSerializer()
            cache = Cache(d, serializer=ser)
            with self.assertRaises(TypeError):
                cache["k"] = NestedAxis("time")
            ser.register(NestedAxis)
            cache["k"] = NestedAxis("time")
            self.assertIsInstance(cache["k"], NestedAxis)


if __name__ == "__main__":
    unittest.main()
