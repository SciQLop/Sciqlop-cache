#pragma once

#include <string>
#include <vector>
#include <chrono>

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
