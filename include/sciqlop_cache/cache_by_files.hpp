/*
** CNRS LPP PROJECT, 2025
** Cache
** File description:
** time related functions
*/

#pragma once

#include "database.hpp"
#include <iostream>
#include <fstream>
#include <filesystem>
#include <unordered_map>
#include <vector>
#include <string>
#include <cstdint>
#include <sys/stat.h>
#include <uuid.h>
#include <format>

/*struct Data {
    std::string path;
    float expireTime;
    int accessCount;
};*/

template <Bytes T>
bool storeBytes(const std::filesystem::path &path, const T &bytes)
{
    std::filesystem::create_directories(path.parent_path());
    std::ofstream out(path, std::ios::binary);
    if (!out) {
        std::cerr << "Failed to open file for writing: " << path << std::endl;
        return false;
    }
    out.write(reinterpret_cast<const char*>(std::data(bytes)),
              static_cast<std::streamsize>(std::size(bytes)));
    bool res = out && !out.fail() && !out.bad();
    if (!res)
        std::cerr << "Failed to write to file: " << path << std::endl;
    out.close();
    return res;
}

std::string generate_random_filename()
{
    static std::random_device rd;
    static std::mt19937 gen(rd());

    uuids::uuid_random_generator generator{gen};
    uuids::uuid id = generator();
    std::string uuid_str = uuids::to_string(id);

    return uuid_str;
}
