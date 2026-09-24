# Compressing numpy values: blosc2 vs pickle

Measured 2026-09-24. Goal: decide whether compressing numpy arrays speeds up
sciqlop-cache, and draw strategies for Speasy and other apps built on it.

## TL;DR

- **Time axes: always compress them with lz4.** They shrink 3–35x, and it costs almost nothing.
- **Float values: keep them as plain pickle on SSDs.** They shrink only 1.2–1.5x, and compressing them makes set/get 3–6x slower.
- **Compress float values only on slow storage**: HDDs, network shares, or cold reads from a SATA SSD.
- The rule behind all of this: **compress when disk speed < codec speed × (1 − 1/ratio).**
- On real Speasy data, compressing the time axis alone makes a cached variable 30–40% smaller.

## Setup

- Machine: 16 cores, NVMe SSD (ext4 under `$HOME`), Python 3.14, numpy 2, blosc2 4.13.1, speasy 1.8.2.
- Baseline: `PickleSerializer` (protocol 5), which is what `Cache` uses by default.
- Compressed: `blosc2.pack_array2` with the byte-shuffle filter, stored as bytes, and `unpack_array2` on read. blosc2 uses every core.
- Each timing is the best of 5 runs, so it reflects warm caches.
- Scripts:
  - `benchmark/compression_roundtrip.py`: set/get through `Cache`, on synthetic or real Speasy data.
  - `benchmark/compression_breakeven.py`: raw codec speed and the break-even disk speeds.
- Run them with `TMPDIR` on a real disk, see [Pitfalls](#pitfalls-met-while-measuring).

## Results

### Synthetic arrays (2M samples)

This was the first probe. It is a pessimistic case: float64 noise has no structure at all.

| array | MB | codec | ratio | set ms | get ms |
|---|---|---|---|---|---|
| int64 time axis, regular | 16 | pickle | 1.00 | 9.0 | 1.1 |
| | | lz4 | 28.1 | **1.7** | 1.4 |
| float64 B-field, random walk | 48 | pickle | 1.00 | 45.7 | 24.3 |
| | | lz4 | 1.29 | 126.7 | 43.2 |
| float64 noise | 48 | pickle | 1.00 | 44.9 | 24.2 |
| | | lz4 | 1.14 | 140.2 | 41.6 |
| float32 random spectrogram | 26 | pickle | 1.00 | 23.8 | 1.3 |
| | | lz4 | 1.10 | 45.7 | 7.2 |

### Real Speasy products

| product | part | MB | pickle set / get ms | best ratio | lz4 set / get ms |
|---|---|---|---|---|---|
| MMS1 FGM srvy, 1 day | time | 11.1 | 1.6 / 0.2 | 16.4 (lz4) | 1.6 / 1.1 |
| | values f32 (N,4) | 22.1 | 3.6 / 1.2 | 1.42 (zstd1 + bitshuffle) | 12.5 / 4.2 |
| THA FGM fgl, 2 days | time | 5.1 | 0.8 / 0.1 | 35.8 (zstd1) | 1.1 / 0.4 |
| | values f32 (N,3) | 7.7 | 1.0 / 0.2 | 1.51 (zstd1 + bitshuffle) | 3.9 / 2.1 |
| MMS1 DIS spectro, 7 days | time | 0.4 | 0.2 / 0.0 | 4.1 (zstd1) | 1.0 / 0.1 |
| | values f32 (N,32) | 5.6 | 0.8 / 0.1 | 1.27 (zstd1) | 2.8 / 0.9 |
| ACE IMF 16 s, 10 days | time | 0.4 | 0.3 / 0.0 | 25.6 (zstd1) | 1.0 / 0.1 |
| | values f32 (N,3) | 0.6 | 0.3 / 0.0 | 1.34 (zstd1) | 1.3 / 0.1 |
| Wind MFI 3 s, 10 days | time | 0.1 | 0.2 / 0.0 | 24.7 (zstd1) | 0.8 / 0.0 |
| | values f32 (N,3) | 0.2 | 0.2 / 0.0 | 1.21 (zstd1) | 0.9 / 0.1 |

A whole variable (time axis + values) gets smaller like this:

| MMS1 FGM srvy, 1 day | size | set ms | get ms |
|---|---|---|---|
| pickle (today) | 33.2 MB | 5.2 | 1.4 |
| lz4 on the time axis only | 22.8 MB (−31%) | 5.2 | 2.3 |
| lz4 on both | 17.6 MB (−47%) | 14.1 | 5.3 |

On THEMIS FGM, compressing the time axis alone saves 38%, and zstd1 on both saves 58%.

### Why real data compresses this way

- **Time axes are nearly regular.** Neighbouring timestamps share most of their bytes. After byte-shuffle, lz4 sees long runs of identical bytes. FPI's time axis is less regular (its sampling jitters), so it only reaches about 4x.
- **Values are float32 measurements.** The low mantissa bits are instrument noise, and no lossless codec can shrink noise. Only the sign, exponent and high mantissa bytes compress, which caps the ratio around 1.3–1.5x.
- float64 noise (synthetic) compresses even less, 1.1–1.2x. Speasy's real data is float32, so it does slightly better than that.

## The break-even law

Consider one array of N bytes and a codec with:

- r: its compression ratio,
- C: its compression speed, in raw bytes per second,
- D: its decompression speed, in raw bytes per second.

The disk moves B bytes per second.

- Writing uncompressed takes N / B.
- Writing compressed takes N / C + N / (r·B).

Compression wins when N / C + N / (r·B) < N / B. That simplifies to:

> **Compress for writes when B < C · (1 − 1/r).**
> **Compress for reads when B < D · (1 − 1/r).**

Two consequences:

1. **The speedup can never exceed r.** Even on an infinitely slow disk, 1.35x-compressible floats only get 1.35x faster. A 17x-compressible time axis can get up to 17x faster.
2. **Picking between codecs is a lower-envelope problem.** The cost per raw byte is 1/C + (1/r)·(1/B), which is a straight line in 1/B. At any disk speed, the best choice is the lowest line, and "no compression" is the line 1/B.

### Measured break-even speeds

These come from `benchmark/compression_breakeven.py`, on MMS FGM, THEMIS FGM and MMS FPI data together. Ranges show the spread between two runs.

| data | codec | ratio | compress GB/s | decompress GB/s | break-even, writes | break-even, reads |
|---|---|---|---|---|---|---|
| time (int64) | **lz4** | 17.1 | 11.3–11.5 | 8.6–9.1 | **10.6–10.9 GB/s** | **8.1–8.5 GB/s** |
| time (int64) | zstd1 | 16.0 | 5.5–5.8 | 2.6–2.7 | 5.2–5.5 | 2.5 |
| time (int64) | zstd6 | 46.5 | 1.2–1.4 | 3.7–4.4 | 1.2–1.4 | 3.6–4.3 |
| values (float32) | lz4 | 1.31 | 0.7–1.2 | 4.3–6.0 | 0.17–0.29 | **1.0–1.4** |
| values (float32) | **zstd1** | 1.38 | 0.9–1.4 | 2.3–2.4 | **0.23–0.38** | 0.62–0.66 |
| values (float32) | zstd3 | 1.40 | 0.6–0.8 | 2.2–2.6 | 0.18–0.23 | 0.64–0.75 |

- **Time axes:** lz4 breaks even at about 10 GB/s, which is about the speed of a memory copy. Compressing them wins, or ties, on every storage.
- **Float values, writes:** zstd1 beats lz4. It compresses both faster and smaller. It only beats plain writes below about 0.3 GB/s.
- **Float values, reads:** lz4 wins below about 1 GB/s. zstd1 only overtakes it below about 0.2 GB/s.

### What that means per storage type

| storage | typical speed | time axis | float values |
|---|---|---|---|
| RAM / page cache (warm reads) | ~10 GB/s | lz4, a wash | plain |
| NVMe | 2–7 GB/s | **lz4** | plain |
| SATA SSD | ~0.5 GB/s | **lz4** | plain writes, lz4 on cold reads (small win) |
| HDD | ~0.15 GB/s | **lz4** | **zstd1** (≤1.35x faster) |
| NFS / SMB over 1 GbE | ~0.1 GB/s | **lz4** | **zstd1** (≤1.35x faster) |

### Caveats on "disk speed"

- **B is the effective speed, not the device's rated speed.** `set()` writes into the OS page cache, and the kernel flushes it later. A slow device only slows `set()` down when writes are sustained long enough to hit the kernel's dirty-page limit.
- **Warm reads never touch the disk.** Big values are read through an mmap, so a repeated `get()` runs at RAM speed, and there compression only costs time. Only cold reads (after a reboot, or with a working set larger than RAM) see the device's speed.
- **C and D grow with the core count.** blosc2 splits the work across all cores. On a 4-core laptop, expect the break-even speeds to be about 4x lower. There, float values should stay plain even on an HDD.
- **Unexplained:** synthetic float64 pickle writes ran at about 1 GB/s (48 MB in 45 ms). Real float32 pickle writes ran at about 6 GB/s (22 MB in 3.6 ms), on the same disk. This doesn't change the conclusions, but it is worth understanding before tuning the write path further.

## Strategies

Ordered by value for effort.

1. **Compress time axes with lz4 in Speasy's codec layer** (recommended first step).
   - It's a clear win: about a third of the bytes of a typical variable disappear, `set()` gets no slower, and `get()` pays under 1 ms per day of 16 Hz data.
   - It belongs in Speasy, not in sciqlop-cache. Speasy knows which array is the time axis, while the cache only sees opaque bytes.
   - Mind compatibility: the cache format changes, so old entries must still load. For example, tag new entries with a version marker, and treat old ones as plain pickle.

2. **Make float-value compression an opt-in setting.**
   - It's worth it on HDDs, network shares, and shared cluster storage. Default it to off.
   - Use zstd1 + shuffle; bitshuffle is sometimes 3–5% smaller.
   - A static config option is enough. Auto-detecting disk speed isn't worth the complexity, because the win is capped at about 1.35x anyway.

3. **Optimize for cache hit rate, not milliseconds.**
   - For Speasy, a cache miss means a network download that takes seconds. A cache hit takes milliseconds, with or without compression.
   - Under a size limit (`max_size`), 30–60% smaller entries mean more data stays cached, and fewer downloads happen. That can outweigh any per-`get()` cost, and it holds even on NVMe.
   - How much it helps depends on how users actually access data, so it should be measured on a real Speasy workload before being turned on.

4. **Zero-copy reads for uncompressed arrays** (sciqlop-cache side, not measured here).
   - For big plain arrays, the remaining `get()` cost is copying the pickled bytes out of the mmap.
   - A serializer that returns an array built directly on top of the mapped `Buffer` could make big-array `get()` roughly constant-time.
   - The catch: the array must keep the mapping alive, and it must be read-only. That's why this is a design question, not a quick win.

5. **Lossy compression** (not measured, needs scientific sign-off).
   - Zeroing low mantissa bits (blosc2's `TRUNC_PREC` filter) would push float32 ratios well above 1.5x.
   - It changes the data, though. It's only acceptable where the dropped precision is below instrument resolution, and that is a decision for each instrument, not a cache default.

## Pitfalls met while measuring

- **`/tmp` is a quota-limited tmpfs here.** Big writes failed with `EDQUOT` ("Disk quota exceeded"). Run benchmarks with `TMPDIR` pointing to a real disk.
- **`set()` reports success when the value file can't be written.** The quota failure above showed that a failed big-value write is silently lost. See [issue #16](https://github.com/SciQLop/Sciqlop-cache/issues/16).
