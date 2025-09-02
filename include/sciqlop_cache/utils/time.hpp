/*
** CNRS LPP PROJECT, 2025
** Cache
** File description:
** time related functions
*/

#pragma once

#include <chrono>
#include <ctime>

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
