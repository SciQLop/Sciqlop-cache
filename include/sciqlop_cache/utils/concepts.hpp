#pragma once

#include <string>
#include <vector>
#include <array>
#include <chrono>
#include <concepts>
#include <span>
#include <type_traits>

template <typename T>
concept DurationConcept = requires(T t) {
    { t.count() } -> std::convertible_to<long long>;
    {
        std::chrono::duration_cast<std::chrono::seconds>(t)
    } -> std::convertible_to<std::chrono::seconds>;
    { std::chrono::floor<T>(t) } -> std::convertible_to<T>;
    std::is_same_v<T, std::chrono::duration<typename T::rep, typename T::period>>;
};

template <typename T>
concept TimePoint = requires(T t) {
    { t.time_since_epoch() } -> std::convertible_to<std::chrono::nanoseconds>;
};

template <typename T>
concept Bytes = requires(T t) {
    { std::size(t) } -> std::convertible_to<std::size_t>;
    { std::data(t) } -> std::convertible_to<const char*>;
};

// A value made of several non-contiguous pieces (e.g. a pickle header plus
// out-of-band array buffers), stored as their concatenation without first
// gluing them into one allocation.
struct ByteChunks
{
    std::vector<std::span<const char>> parts;

    [[nodiscard]] std::size_t size() const
    {
        std::size_t total = 0;
        for (const auto& p : parts)
            total += p.size();
        return total;
    }

    [[nodiscard]] std::vector<char> flatten() const
    {
        std::vector<char> out;
        out.reserve(size());
        for (const auto& p : parts)
            out.insert(out.end(), p.begin(), p.end());
        return out;
    }
};

template <typename T>
concept Payload = Bytes<T> || std::same_as<std::remove_cvref_t<T>, ByteChunks>;

[[nodiscard]] inline std::span<const std::span<const char>> chunks_of(const ByteChunks& value)
{
    return value.parts;
}

[[nodiscard]] inline std::array<std::span<const char>, 1> chunks_of(const Bytes auto& value)
{
    return { std::span<const char>(std::data(value), std::size(value)) };
}
