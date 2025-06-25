/*
** CNRS LPP PROJECT, 2025
** Cache
** File description:
** time related functions
*/

#pragma once

#include <chrono>
#include <ctime>

std::time_t time_to_epoch(std::chrono::system_clock::time_point time_) {
    return std::chrono::system_clock::to_time_t(time_);
}

std::chrono::system_clock::time_point epoch_to_time(std::time_t epoch_) {
    return std::chrono::system_clock::from_time_t(epoch_);
}

double epoch_to_double(std::time_t epoch_) {
    return static_cast<double>(epoch_);
}

std::time_t double_to_epoch(double double_) {
    return static_cast<std::time_t>(double_);
}
