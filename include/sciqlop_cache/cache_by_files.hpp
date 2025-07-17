/*
** CNRS LPP PROJECT, 2025
** Cache
** File description:
** time related functions
*/

#pragma once

#include "utils/time.hpp"
#include <cpp_utils/lifetime/scope_leaving_guards.hpp>
#include <iostream>
#include <fstream>
#include <filesystem>
#include <unordered_map>
#include <vector>
#include <string>
#include <cstdint>
#include <cstdio> // for std::remove

struct Data {
    std::string path;
    float expireTime;
    int accessCount;
};

bool storeBytes(const std::string &path, const std::vector<std::uint8_t> &bytes, bool append = false)
{
    std::ios_base::openmode mode;

    if (replace || !std::filesystem::exists(path))
        mode = std::ios::binary | std::ios::out | std::ios::trunc;
    else
        mode = std::ios::binary | std::ios::out | std::ios::app;

    std::ofstream file(path, mode);
    if (!file)
        return false;

    file.write(reinterpret_cast<const char*>(bytes.data()), bytes.size());
    return file.good();
}

std::vector<std::uint8_t> getBytes(const std::string &path)
{
    std::ifstream file(path, std::ios::binary | std::ios::ate);

    if (!file) {
        std::cerr << "Failed to open file: " << path << std::endl;
        return {};
    }
    std::streamsize size = file.tellg();
    file.seekg(0, std::ios::beg);
    std::vector<std::uint8_t> buffer(size);
    if (!file.read(reinterpret_cast<char*>(buffer.data()), size)) {
        std::cerr << "Failed to read file: " << path << std::endl;
        return {};
    }
    return buffer;
}
