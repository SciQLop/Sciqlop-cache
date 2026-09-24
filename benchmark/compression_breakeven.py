#!/usr/bin/env python3
"""Raw blosc2 throughput and ratio per data class, and the disk speed below
which each codec beats storing the array uncompressed.

Break-even law (derivation in docs/compression-vs-pickle.md):
    compress iff disk_speed < codec_throughput * (1 - 1/ratio)
Usage (needs blosc2 and speasy):
    python benchmark/compression_breakeven.py
"""

import time

import blosc2
import numpy as np
import speasy as spz

SOURCES = {
    "MMS1 FGM srvy": ("cda/MMS1_FGM_SRVY_L2/mms1_fgm_b_gse_srvy_l2", "2020-01-15", "2020-01-16"),
    "THA FGM fgl": ("cda/THA_L2_FGM/tha_fgl_gse", "2020-01-10", "2020-01-12"),
    "MMS1 DIS spectro": ("cda/MMS1_FPI_FAST_L2_DIS-MOMS/mms1_dis_energyspectr_omni_fast", "2020-01-10", "2020-01-17"),
}

CODECS = {
    "lz4": (blosc2.Codec.LZ4, 5),
    "zstd1": (blosc2.Codec.ZSTD, 1),
    "zstd3": (blosc2.Codec.ZSTD, 3),
    "zstd6": (blosc2.Codec.ZSTD, 6),
}


def best_s(f, n=7):
    ts = []
    for _ in range(n):
        t0 = time.perf_counter()
        f()
        ts.append(time.perf_counter() - t0)
    return min(ts)


def measure(a, codec, clevel):
    cparams = {"codec": codec, "clevel": clevel, "filters": [blosc2.Filter.SHUFFLE]}
    blob = blosc2.pack_array2(a, cparams=cparams)
    compress = a.nbytes / best_s(lambda: blosc2.pack_array2(a, cparams=cparams))
    decompress = a.nbytes / best_s(lambda: blosc2.unpack_array2(blob))
    return a.nbytes / len(blob), compress, decompress


def data_classes():
    variables = [spz.get_data(*src) for src in SOURCES.values()]
    return {
        "time (int64)": np.concatenate([np.asarray(v.time).view("int64") for v in variables]),
        "values (float32)": np.concatenate(
            [np.asarray(v.values, dtype="float32").ravel() for v in variables]),
    }


def main():
    print("class,codec,ratio,compress_GBps,decompress_GBps,breakeven_write_GBps,breakeven_read_GBps")
    for cls, a in data_classes().items():
        for name, (codec, clevel) in CODECS.items():
            ratio, c, d = measure(a, codec, clevel)
            gain = 1 - 1 / ratio
            print(f"{cls},{name},{ratio:.2f},{c / 1e9:.2f},{d / 1e9:.2f},"
                  f"{c * gain / 1e9:.2f},{d * gain / 1e9:.2f}", flush=True)


if __name__ == "__main__":
    main()
