#pragma once

// Buffer codecs of the pickle-oob value format. Plain C++ on spans, so the
// bindings can run all of it with the GIL released.

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#ifdef SCIQLOP_CACHE_WITH_BLOSC2
#include "blosc2_runtime.hpp"
#include <blosc2.h>
#endif

// Codec ids are persisted in pickle-oob values: never reuse one, only add.
enum class BufferCodec : int
{
    raw = 0,
    // A sequence of blosc2 chunks (each header records its sizes), so values
    // over blosc2's 2 GB chunk limit work and no extra framing is needed.
    blosc2 = 1,
};

inline constexpr bool blosc2_available =
#ifdef SCIQLOP_CACHE_WITH_BLOSC2
    true;
#else
    false;
#endif

#ifdef SCIQLOP_CACHE_WITH_BLOSC2

namespace oob_blosc2
{

// Largest blosc2 chunk we write (blosc2's own limit is 2 GB). Each chunk is
// split into blocks that blosc2 spreads over its threads.
inline constexpr std::size_t max_chunk = std::size_t { 256 } << 20;
// Buffers at least twice this size are first tested on a sample of it, so
// one that won't compress (noisy floats) costs ~6 us, not a full pass.
inline constexpr std::size_t sample_size = std::size_t { 64 } << 10;

// Chunks hold whole items, so shuffle sees aligned records.
inline std::size_t chunk_size(int typesize)
{
    return max_chunk - max_chunk % static_cast<std::size_t>(typesize);
}

struct DecompressionCtx
{
    blosc2_context* ctx;
    DecompressionCtx()
    {
        blosc2_dparams params = BLOSC2_DPARAMS_DEFAULTS;
        params.nthreads = static_cast<int16_t>(
            std::min<unsigned>(blosc2_runtime::thread_count(), INT16_MAX));
        ctx = blosc2_create_dctx(params);
        if (!ctx)
            throw std::runtime_error("blosc2: could not create a decompression context");
    }
    DecompressionCtx(const DecompressionCtx&) = delete;
    DecompressionCtx& operator=(const DecompressionCtx&) = delete;
    ~DecompressionCtx() { blosc2_free_ctx(ctx); }
};

// Compresses `src` into `out` (appended) one chunk at a time; returns false
// as soon as the output would exceed `budget` bytes.
inline bool compress_into(blosc2_context* ctx, std::span<const char> src, int typesize,
                          std::size_t budget, std::vector<char>& out)
{
    const auto step = chunk_size(typesize);
    for (std::size_t at = 0; at < src.size(); at += step)
    {
        auto piece = src.subspan(at, std::min(step, src.size() - at));
        auto start = out.size();
        if (start + BLOSC2_MAX_OVERHEAD > budget)
            return false;
        auto room = std::min(budget - start, piece.size() + BLOSC2_MAX_OVERHEAD);
        out.resize(start + room);
        int n = blosc2_compress_ctx(ctx, piece.data(), static_cast<int32_t>(piece.size()),
                                    out.data() + start, static_cast<int32_t>(room));
        if (n <= 0)
            return false; // 0: does not fit the budget, < 0: error
        out.resize(start + static_cast<std::size_t>(n));
    }
    return true;
}

} // namespace oob_blosc2

// Compresses `src` if that makes it at least `min_ratio` times smaller.
// `typesize` is the array item size, used by the shuffle filter.
[[nodiscard]] inline std::optional<std::vector<char>> blosc2_compress(
    std::span<const char> src, int typesize, double min_ratio)
{
    using namespace oob_blosc2;
    if (src.empty() || typesize < 1 || typesize > BLOSC_MAX_TYPESIZE
        || src.size() % static_cast<std::size_t>(typesize) != 0)
        return std::nullopt;
    blosc2_runtime::CompressionLease ctx(typesize);
    auto budget_for = [&](std::size_t n) { return static_cast<std::size_t>(n / min_ratio); };
    std::vector<char> out;
    if (src.size() >= 2 * sample_size)
    {
        auto sample = src.first(sample_size - sample_size % static_cast<std::size_t>(typesize));
        if (!compress_into(ctx.get(), sample, typesize, budget_for(sample.size()), out))
            return std::nullopt;
        out.clear();
    }
    if (!compress_into(ctx.get(), src, typesize, budget_for(src.size()), out))
        return std::nullopt;
    return out;
}

inline std::optional<std::string> _check_blosc2(std::span<const char> stored, std::size_t dst_size)
{
    std::size_t filled = 0;
    while (!stored.empty())
    {
        if (stored.size() < BLOSC_MIN_HEADER_LENGTH)
            return "truncated blosc2 chunk header";
        int32_t nbytes = 0, cbytes = 0, blocksize = 0;
        if (blosc2_cbuffer_sizes(stored.data(), &nbytes, &cbytes, &blocksize) < 0 || cbytes <= 0
            || nbytes < 0)
            return "invalid blosc2 chunk header";
        if (static_cast<std::size_t>(cbytes) > stored.size())
            return "blosc2 chunk runs past the stored buffer";
        if (static_cast<std::size_t>(nbytes) > dst_size - filled)
            return "blosc2 chunks hold more bytes than the buffer";
        filled += static_cast<std::size_t>(nbytes);
        stored = stored.subspan(static_cast<std::size_t>(cbytes));
    }
    if (filled != dst_size)
        return "blosc2 chunks hold fewer bytes than the buffer";
    return std::nullopt;
}

inline std::optional<std::string> _decode_blosc2(std::span<const char> stored, std::span<char> dst)
{
    oob_blosc2::DecompressionCtx ctx;
    while (!stored.empty())
    {
        int32_t nbytes = 0, cbytes = 0, blocksize = 0;
        blosc2_cbuffer_sizes(stored.data(), &nbytes, &cbytes, &blocksize);
        int n = blosc2_decompress_ctx(ctx.ctx, stored.data(), cbytes, dst.data(), nbytes);
        if (n != nbytes)
            return "blosc2 could not decompress a chunk";
        dst = dst.subspan(static_cast<std::size_t>(nbytes));
        stored = stored.subspan(static_cast<std::size_t>(cbytes));
    }
    return std::nullopt;
}

#endif // SCIQLOP_CACHE_WITH_BLOSC2

// Returns an error message, or nothing if `stored` can fill `dst_size` bytes.
inline std::optional<std::string> check_stored_buffer(
    BufferCodec codec, std::span<const char> stored, std::size_t dst_size)
{
    switch (codec)
    {
        case BufferCodec::raw:
            if (stored.size() != dst_size)
                return "raw buffer size mismatch";
            return std::nullopt;
        case BufferCodec::blosc2:
#ifdef SCIQLOP_CACHE_WITH_BLOSC2
            return _check_blosc2(stored, dst_size);
#else
            return "this pysciqlop_cache build has no blosc2 support";
#endif
    }
    return "codec " + std::to_string(static_cast<int>(codec))
        + " is not supported by this pysciqlop_cache version";
}

// Fills `dst` from `stored`; only for inputs that passed check_stored_buffer.
inline std::optional<std::string> decode_buffer(
    BufferCodec codec, std::span<const char> stored, std::span<char> dst)
{
    switch (codec)
    {
        case BufferCodec::raw:
            std::memcpy(dst.data(), stored.data(), stored.size());
            return std::nullopt;
        case BufferCodec::blosc2:
#ifdef SCIQLOP_CACHE_WITH_BLOSC2
            return _decode_blosc2(stored, dst);
#else
            break;
#endif
    }
    return "unsupported codec";
}
