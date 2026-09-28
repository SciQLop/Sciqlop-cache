import gc
import multiprocessing
import os
import pickle
import shutil
import struct
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor

import numpy as np

from pysciqlop_cache import Cache, Index, PickleOOBSerializer, PickleSerializer
from pysciqlop_cache._pysciqlop_cache import BLOSC2_AVAILABLE


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

    def test_unknown_buffer_codec_raises(self):
        data = bytearray(PickleOOBSerializer(compress=False).dumps(fgm_like_day()))
        first_codec = len(PickleOOBSerializer.MAGIC) + 12
        self.assertEqual(data[first_codec], 0)
        data[first_codec] = 7
        with self.assertRaisesRegex(pickle.UnpicklingError, "codec 7"):
            self.ser.loads(bytes(data))

    def test_corrupt_sizes_raise_before_allocating(self):
        data = PickleOOBSerializer().dumps(fgm_like_day())
        entry = len(PickleOOBSerializer.MAGIC) + 12  # first (codec, stored, raw) entry
        count_at = len(PickleOOBSerializer.MAGIC)
        corruptions = {
            "raw size 32 TiB": (entry + 9, struct.pack("<Q", 1 << 45)),
            "raw size 2**64-1": (entry + 9, struct.pack("<Q", (1 << 64) - 1)),
            "stored size past the end": (entry + 1, struct.pack("<Q", 1 << 40)),
            "buffer count 2**32-1": (count_at, struct.pack("<I", (1 << 32) - 1)),
        }
        for name, (at, patch) in corruptions.items():
            corrupt = data[:at] + patch + data[at + len(patch) :]
            with self.subTest(name), self.assertRaises(pickle.UnpicklingError):
                self.ser.loads(corrupt)

    def test_truncated_value_raises(self):
        data = self.ser.dumps(fgm_like_day())
        with self.assertRaises(pickle.UnpicklingError):
            self.ser.loads(data[: len(data) // 2])

    def test_dumps_chunks_concatenate_to_dumps(self):
        # Uncompressed: multithreaded blosc2 lays blocks out in completion
        # order, so two compressions of the same data differ byte-wise.
        ser = PickleOOBSerializer(compress=False)
        for value in [42, fgm_like_day(100), fgm_like_day()]:
            chunks = ser.dumps_chunks(value)
            joined = chunks if isinstance(chunks, bytes) else b"".join(chunks)
            self.assertEqual(joined, ser.dumps(value))
        var = fgm_like_day()
        joined = b"".join(self.ser.dumps_chunks(var))
        np.testing.assert_array_equal(self.ser.loads(joined).time, var.time)

    def test_dumps_chunks_does_not_copy_arrays(self):
        var = fgm_like_day()
        chunks = self.ser.dumps_chunks(var)
        self.assertTrue(any(np.shares_memory(np.asarray(c), var.values) for c in chunks[1:]))

    def test_reads_plain_pickle_entries(self):
        var = fgm_like_day(100)
        result = self.ser.loads(PickleSerializer().dumps(var))
        np.testing.assert_array_equal(result.values, var.values)


def buffer_codecs(data):
    count, _ = struct.unpack_from("<IQ", data, 8)
    return [struct.unpack_from("<BQQ", data, 20 + 17 * i)[0] for i in range(count)]


@unittest.skipUnless(BLOSC2_AVAILABLE, "built without blosc2")
class TestBlosc2(unittest.TestCase):
    def setUp(self):
        self.ser = PickleOOBSerializer()

    def test_compressible_buffers_use_blosc2_and_roundtrip(self):
        var = fgm_like_day()
        data = self.ser.dumps(var)
        self.assertEqual(sorted(buffer_codecs(data)), [0, 1], "time axis blosc2, noisy values raw")
        self.assertLess(len(data), var.values.nbytes + var.time.nbytes // 4)
        result = self.ser.loads(data)
        np.testing.assert_array_equal(result.time, var.time)
        np.testing.assert_array_equal(result.values, var.values)
        result.time[0] = np.datetime64("2000-01-01", "ns")  # writable

    def test_compress_false_keeps_every_buffer_raw(self):
        data = PickleOOBSerializer(compress=False).dumps(fgm_like_day())
        self.assertEqual(buffer_codecs(data), [0, 0])

    def test_small_oob_buffers_are_not_compressed(self):
        data = self.ser.dumps({"t": np.arange(20_000, dtype=np.int64)})  # 160 KB
        self.assertEqual(buffer_codecs(data), [0])

    def test_min_ratio_is_respected(self):
        data = PickleOOBSerializer(min_ratio=1000.0).dumps(fgm_like_day())
        self.assertEqual(buffer_codecs(data), [0, 0])

    def test_corrupt_blosc2_buffer_raises(self):
        data = self.ser.dumps({"t": np.arange(1_000_000, dtype=np.int64)})
        self.assertEqual(buffer_codecs(data), [1])
        stored_at = len(data) - struct.unpack_from("<BQQ", data, 20)[1]
        huge_raw_size = data[:29] + struct.pack("<Q", 1 << 45) + data[37:]
        for corrupt in (data[:-10], data[:stored_at] + b"\xff" * 16 + data[stored_at + 16 :], huge_raw_size):
            with self.subTest(size=len(corrupt)), self.assertRaises(pickle.UnpicklingError):
                self.ser.loads(corrupt)

    def test_itemsizes_and_layouts_roundtrip(self):
        arrays = {
            "u8": np.arange(300_000, dtype=np.uint8),
            "i16": np.arange(300_000, dtype=np.int16),
            "c128": np.zeros(40_000, dtype=np.complex128),
            "f_order": np.asfortranarray(np.zeros((500, 400), dtype=np.float32)),
            "record": np.zeros(20_000, dtype=[("a", "f8"), ("b", "i4"), ("c", "S20")]),
        }
        result = self.ser.loads(self.ser.dumps(arrays))
        for name, arr in arrays.items():
            with self.subTest(name):
                self.assertEqual(result[name].dtype, arr.dtype)
                np.testing.assert_array_equal(result[name], arr)


def _os_thread_count():
    return len(os.listdir("/proc/self/task")) if os.path.isdir("/proc/self/task") else None


def _roundtrip_in_child(queue):
    ser = PickleOOBSerializer()
    var = fgm_like_day()
    result = ser.loads(ser.dumps(var))
    # Counted before queue.put(), which starts the queue's feeder thread.
    queue.put((bool(np.array_equal(result.time, var.time)), _os_thread_count()))


@unittest.skipUnless(BLOSC2_AVAILABLE, "built without blosc2")
class TestBlosc2Runtime(unittest.TestCase):
    def test_threads_compress_and_decode_concurrently(self):
        ser = PickleOOBSerializer()
        values = [{"t": np.arange(i, i + 500_000, dtype=np.int64)} for i in range(16)]

        def roundtrip(value):
            data = ser.dumps(value)
            return buffer_codecs(data) == [1] and np.array_equal(ser.loads(data)["t"], value["t"])

        with ThreadPoolExecutor(16) as pool:
            self.assertTrue(all(pool.map(roundtrip, values * 4)))

    @unittest.skipUnless(hasattr(os, "fork"), "needs fork")
    def test_forked_child_can_compress_after_parent_did(self):
        ser = PickleOOBSerializer()
        ser.loads(ser.dumps(fgm_like_day()))  # parent starts the worker pool
        ctx = multiprocessing.get_context("fork")
        queue = ctx.Queue()
        child = ctx.Process(target=_roundtrip_in_child, args=(queue,))
        child.start()
        child.join(timeout=60)
        if child.is_alive():
            child.kill()
            self.fail("forked child hung in blosc2")
        self.assertEqual(child.exitcode, 0)
        ok, threads = queue.get(timeout=5)
        self.assertTrue(ok)
        if threads is not None and (os.cpu_count() or 1) > 1:
            # The parent's workers don't exist in the child: it must start its
            # own pool instead of running every job on the calling thread
            # (or blocking on a mutex a dead worker held at fork time).
            self.assertGreater(threads, 1)


class TestErrorPaths(unittest.TestCase):
    """Bad input raises a clear error instead of misbehaving."""

    def setUp(self):
        self.ser = PickleOOBSerializer(compress=False)

    def test_unknown_header_after_the_nul_byte(self):
        with self.assertRaisesRegex(pickle.UnpicklingError, "unknown pickle-oob header"):
            self.ser.loads(b"\x00NOTOOB1" + b"\x00" * 32)

    def test_raw_buffer_whose_raw_size_disagrees_with_its_stored_size(self):
        data = bytearray(self.ser.dumps(fgm_like_day()))
        raw_size_at = len(PickleOOBSerializer.MAGIC) + 12 + 9
        stored = struct.unpack_from("<Q", data, raw_size_at)[0]
        struct.pack_into("<Q", data, raw_size_at, stored - 8)
        with self.assertRaisesRegex(pickle.UnpicklingError, "size mismatch"):
            self.ser.loads(bytes(data))

    def test_decode_buffers_rejects_an_alloc_of_the_wrong_size(self):
        from pysciqlop_cache._pysciqlop_cache import decode_buffers
        src = np.arange(10, dtype=np.uint8)
        with self.assertRaisesRegex(ValueError, "wrong size"):
            decode_buffers(src, [(0, 0, 10, 10)], lambda n: np.empty(n + 1, np.uint8))

    @unittest.skipUnless(BLOSC2_AVAILABLE, "built without blosc2")
    def test_compress_buffer_arguments(self):
        from pysciqlop_cache._pysciqlop_cache import compress_buffer
        data = np.arange(100_000, dtype=np.int64)
        for typesize in (0, 256):
            self.assertIsNone(compress_buffer(data, typesize, 2.0))
        self.assertIsNone(compress_buffer(np.zeros(10, np.uint8), 3, 2.0))  # not whole items
        self.assertIsNone(compress_buffer(np.empty(0, np.uint8), 1, 2.0))
        for bad_ratio in (0.0, -1.0):
            with self.assertRaisesRegex(ValueError, "min_ratio"):
                compress_buffer(data, 8, bad_ratio)

    @unittest.skipUnless(BLOSC2_AVAILABLE, "built without blosc2")
    def test_a_buffer_that_compresses_only_at_its_start_is_kept_raw(self):
        from pysciqlop_cache._pysciqlop_cache import compress_buffer
        data = np.concatenate([np.zeros(256 * 1024, np.uint8),
                               np.random.default_rng(0).integers(0, 256, 4 << 20, np.uint8)])
        self.assertIsNone(compress_buffer(data, 1, 2.0))

    @unittest.skipUnless(BLOSC2_AVAILABLE, "built without blosc2")
    def test_corrupt_chunk_body_raises(self):
        data = bytearray(PickleOOBSerializer().dumps({"t": np.arange(1_000_000, dtype=np.int64)}))
        self.assertEqual(buffer_codecs(data), [1])
        stored_at = len(data) - struct.unpack_from("<BQQ", data, 20)[1]
        data[stored_at + 40 : stored_at + 48] = b"\xff" * 8  # past the 32-byte chunk header
        with self.assertRaisesRegex(pickle.UnpicklingError, "could not decompress"):
            PickleOOBSerializer().loads(bytes(data))


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

    def test_raw_binding_rejects_non_buffer_values(self):
        cache = Cache(self.tmp_dir)
        raw_set = type(cache).__mro__[1].set
        with self.assertRaises(TypeError):
            raw_set(cache, "k", [b"ok", 42])
        with self.assertRaises(TypeError):
            raw_set(cache, "k", "not bytes")
        self.assertIsNone(cache.get("k"))
        del cache

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
