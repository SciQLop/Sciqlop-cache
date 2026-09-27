import gc
import shutil
import tempfile
import unittest

import numpy as np

from pysciqlop_cache import Cache, Index, PickleOOBSerializer, PickleSerializer


class FakeSpeasyVariable:
    def __init__(self, time, values, meta):
        self.time = time
        self.values = values
        self.meta = meta


def fgm_like_day(rows=200_000):
    return FakeSpeasyVariable(
        time=np.arange(rows, dtype=np.int64).astype("datetime64[ns]"),
        values=np.random.rand(rows, 4).astype(np.float32),
        meta={"UNITS": "nT", "FILLVAL": -1e31, "DEPEND_0": "Epoch"},
    )


class TestFormat(unittest.TestCase):
    def setUp(self):
        self.ser = PickleOOBSerializer()

    def test_values_without_buffers_stay_plain_pickle(self):
        for value in [42, "hello", {"a": [1, 2]}, None, np.array([object()], dtype=object)]:
            data = self.ser.dumps(value)
            self.assertEqual(data[:1], b"\x80")

    def test_small_arrays_stay_plain_pickle(self):
        data = self.ser.dumps(fgm_like_day(100))
        self.assertEqual(data[:1], b"\x80")

    def test_numpy_arrays_go_out_of_band(self):
        var = fgm_like_day()
        data = self.ser.dumps(var)
        self.assertEqual(data[:8], PickleOOBSerializer.MAGIC)
        payload = var.time.nbytes + var.values.nbytes
        self.assertLess(len(data) - payload, 4096, "time axis or values stayed in-band")

    def test_roundtrip_preserves_dtypes_shapes_and_order(self):
        arrays = {
            "dt": np.array(["2020-01-01T00:00:00.123"], dtype="datetime64[ms]"),
            "td": np.arange(5).astype("timedelta64[s]"),
            "f_order": np.asfortranarray(np.arange(12, dtype=np.float64).reshape(3, 4)),
            "empty": np.empty((0, 3), dtype=np.float32),
            "scalar": np.array(3.5),
            "strided": np.arange(10)[::2],
            "dt_2d": np.arange(6).astype("datetime64[ns]").reshape(2, 3),
        }
        self.ser.MIN_OOB_BUFFER = 0
        data = self.ser.dumps(arrays)
        self.assertEqual(data[:8], PickleOOBSerializer.MAGIC)
        result = self.ser.loads(data)
        for name, arr in arrays.items():
            with self.subTest(name):
                self.assertEqual(result[name].dtype, arr.dtype)
                self.assertEqual(result[name].shape, arr.shape)
                np.testing.assert_array_equal(result[name], arr)
        self.assertTrue(result["f_order"].flags.f_contiguous)

    def test_reads_plain_pickle_entries(self):
        var = fgm_like_day(100)
        result = self.ser.loads(PickleSerializer().dumps(var))
        np.testing.assert_array_equal(result.values, var.values)


class TestInCache(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()

    def tearDown(self):
        gc.collect()
        shutil.rmtree(self.tmp_dir)

    def test_loaded_arrays_are_writable_and_outlive_the_cache(self):
        cache = Cache(self.tmp_dir, serializer=PickleOOBSerializer())
        var = fgm_like_day()
        cache.set("day", var)
        result = cache.get("day")
        del cache
        gc.collect()
        result.values[0, 0] = np.nan
        result.time[0] = np.datetime64("2000-01-01", "ns")
        np.testing.assert_array_equal(result.values[1:], var.values[1:])
        np.testing.assert_array_equal(result.time[1:], var.time[1:])

    def test_pickle_cache_upgrades_to_oob_and_keeps_old_entries(self):
        old = Cache(self.tmp_dir)
        old.set("old", fgm_like_day(100))
        del old
        cache = Cache(self.tmp_dir, serializer=PickleOOBSerializer())
        np.testing.assert_array_equal(cache.get("old").values.shape, (100, 4))
        cache.set("new", fgm_like_day(100))
        del cache
        reopened = Cache(self.tmp_dir)
        self.assertEqual(reopened.serializer.name, "pickle-oob")
        self.assertEqual(reopened.get("new").values.shape, (100, 4))

    def test_oob_cache_refuses_downgrade_to_pickle(self):
        Cache(self.tmp_dir, serializer=PickleOOBSerializer())
        gc.collect()
        with self.assertRaises(ValueError):
            Cache(self.tmp_dir, serializer=PickleSerializer())

    def test_index_upgrade_and_downgrade(self):
        old = Index(self.tmp_dir)
        old["old"] = np.arange(10)
        del old
        index = Index(self.tmp_dir, serializer=PickleOOBSerializer())
        np.testing.assert_array_equal(index["old"], np.arange(10))
        del index
        gc.collect()
        with self.assertRaises(ValueError):
            Index(self.tmp_dir, serializer=PickleSerializer())


if __name__ == "__main__":
    unittest.main()
