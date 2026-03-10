import shutil
import unittest
from tempfile import TemporaryDirectory

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
        self.tmp_dir = TemporaryDirectory(delete=False)
        self.cache = Cache(
            self.tmp_dir.name, serializer=PickleSerializer()
        )

    def tearDown(self):
        del self.cache
        shutil.rmtree(self.tmp_dir.name)

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
        self.tmp_dir = TemporaryDirectory(delete=False)
        self.cache = Cache(
            self.tmp_dir.name, serializer=MsgspecSerializer()
        )

    def tearDown(self):
        del self.cache
        shutil.rmtree(self.tmp_dir.name)

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
        self.tmp_dir = TemporaryDirectory(delete=False)

    def tearDown(self):
        shutil.rmtree(self.tmp_dir.name)

    @unittest.skipUnless(_has_msgspec, "msgspec not installed")
    def test_reopen_without_serializer_arg(self):
        cache = Cache(self.tmp_dir.name, serializer=MsgspecSerializer())
        cache.set("key", "value")
        del cache

        # Reopen without specifying serializer — should auto-detect
        cache2 = Cache(self.tmp_dir.name)
        self.assertEqual(cache2.get("key"), "value")
        self.assertEqual(cache2.serializer.name, "msgspec")

    def test_reopen_with_matching_serializer(self):
        cache = Cache(self.tmp_dir.name, serializer=PickleSerializer())
        cache.set("key", 42)
        del cache

        cache2 = Cache(self.tmp_dir.name, serializer=PickleSerializer())
        self.assertEqual(cache2.get("key"), 42)

    @unittest.skipUnless(_has_msgspec, "msgspec not installed")
    def test_reopen_with_wrong_serializer_raises(self):
        cache = Cache(self.tmp_dir.name, serializer=MsgspecSerializer())
        cache.set("key", "value")
        del cache

        with self.assertRaises(ValueError):
            Cache(self.tmp_dir.name, serializer=PickleSerializer())

    def test_default_is_pickle(self):
        cache = Cache(self.tmp_dir.name)
        self.assertEqual(cache.serializer.name, "pickle")


if __name__ == "__main__":
    unittest.main()
