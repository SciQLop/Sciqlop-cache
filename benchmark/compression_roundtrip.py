#!/usr/bin/env python3
"""Pickle vs blosc2 through Cache.set/get, on synthetic or real Speasy arrays.

Report: docs/compression-vs-pickle.md
Usage (needs blosc2, plus speasy for --source speasy):
    TMPDIR=$HOME/some/real/disk python benchmark/compression_roundtrip.py --source synthetic
    TMPDIR=$HOME/some/real/disk python benchmark/compression_roundtrip.py --source speasy
TMPDIR matters: /tmp is often a quota-limited tmpfs, which fails big writes.
"""

import argparse
import pickle
import tempfile
import time

import blosc2
import numpy as np
from pysciqlop_cache import Cache

SPEASY_PRODUCTS = {
    "ACE IMF 16s": ("amda/imf", "2016-06-01", "2016-06-11"),
    "MMS1 FGM srvy": ("cda/MMS1_FGM_SRVY_L2/mms1_fgm_b_gse_srvy_l2", "2020-01-15", "2020-01-16"),
    "MMS1 DIS density": ("cda/MMS1_FPI_FAST_L2_DIS-MOMS/mms1_dis_numberdensity_fast", "2020-01-10", "2020-01-17"),
    "MMS1 DIS spectro": ("cda/MMS1_FPI_FAST_L2_DIS-MOMS/mms1_dis_energyspectr_omni_fast", "2020-01-10", "2020-01-17"),
    "Wind MFI 3s": ("cda/WI_H0_MFI/BGSE", "2020-01-01", "2020-01-11"),
    "THA FGM fgl": ("cda/THA_L2_FGM/tha_fgl_gse", "2020-01-10", "2020-01-12"),
}


def blosc(codec, clevel, filt=blosc2.Filter.SHUFFLE):
    cparams = {"codec": codec, "clevel": clevel, "filters": [filt]}
    return lambda a: blosc2.pack_array2(a, cparams=cparams)


ENCODERS = {
    "pickle": None,
    "lz4": blosc(blosc2.Codec.LZ4, 5),
    "zstd1": blosc(blosc2.Codec.ZSTD, 1),
    "zstd1-bitsh": blosc(blosc2.Codec.ZSTD, 1, blosc2.Filter.BITSHUFFLE),
}


def synthetic_arrays():
    n = 2_000_000
    rng = np.random.default_rng(0)
    yield "synthetic", {
        "time_ns": np.arange(n, dtype="int64") * 62_500_000 + 1_600_000_000_000_000_000,
        "B_smooth_f64": np.cumsum(rng.normal(0, 0.1, (n, 3)), axis=0),
        "noise_f64": rng.random((n, 3)),
        "spectro_f32": rng.random((200_000, 32)).astype("float32"),
    }


def speasy_arrays():
    import speasy as spz

    for name, (pid, start, stop) in SPEASY_PRODUCTS.items():
        v = spz.get_data(pid, start, stop)
        if v is not None:
            yield name, {
                "time": np.ascontiguousarray(v.time).view("int64"),
                "values": np.ascontiguousarray(v.values),
            }


def best_ms(f, n=5):
    ts = []
    for _ in range(n):
        t0 = time.perf_counter()
        f()
        ts.append(time.perf_counter() - t0)
    return min(ts) * 1e3


def ops(cache, key, a, enc):
    if enc is None:
        return (lambda: cache.set(key, a)), (lambda: cache.get(key)), len(pickle.dumps(a, 5))
    return ((lambda: cache.set(key, enc(a))),
            (lambda: blosc2.unpack_array2(cache.get(key))),
            len(enc(a)))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", choices=["synthetic", "speasy"], default="synthetic")
    sources = {"synthetic": synthetic_arrays, "speasy": speasy_arrays}[parser.parse_args().source]
    with tempfile.TemporaryDirectory() as d:
        cache = Cache(d)
        print("product,part,dtype,shape,MB,codec,ratio,set_ms,get_ms", flush=True)
        for product, parts in sources():
            for part, a in parts.items():
                for cname, enc in ENCODERS.items():
                    put, fetch, size = ops(cache, f"{product}/{part}/{cname}", a, enc)
                    s, g = best_ms(put), best_ms(fetch)
                    assert np.array_equal(fetch(), a, equal_nan=a.dtype.kind == "f")
                    print(f"{product},{part},{a.dtype},{'x'.join(map(str, a.shape))},"
                          f"{a.nbytes / 1e6:.1f},{cname},{a.nbytes / size:.2f},{s:.1f},{g:.1f}",
                          flush=True)


if __name__ == "__main__":
    main()
