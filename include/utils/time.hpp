/*
** CNRS LPP PROJECT, 2025
** Cache
** File description:
** time related functions
*/

#pragma once

#include <chrono>
#include <ctime>


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

inline double time_point_to_epoch(const auto time_)
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(time_.time_since_epoch()).count()
        / 1e9;
}

inline auto epoch_to_time_point(double epoch_)
{
    return std::chrono::time_point<std::chrono::system_clock, std::chrono::nanoseconds>(
        std::chrono::nanoseconds(static_cast<long long>(epoch_ * 1e9)));
}
