/*
** CNRS LPP PROJECT, 2025
** Cache
** File description:
** class Cache used to store and retrieve data
** often used by the user
*/

#pragma once

#include <chrono>

double time_to_double(std::chrono::system_clock::time_point time_) {
    using namespace std::chrono;
    auto converter = time_.time_since_epoch();

    return duration_cast<duration<double>>(converter).count();
}

std::chrono::system_clock::time_point double_to_time(double double_) {
    using namespace std::chrono;
    auto converter = duration<double>(double_);

    return std::chrono::system_clock::time_point(duration_cast<system_clock::duration>(converter));
}
