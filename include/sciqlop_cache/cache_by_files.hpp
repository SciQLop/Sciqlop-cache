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

template <Bytes Buffer = std::vector<char>>
std::optional<Buffer> getBytes(const std::filesystem::path &path)
{
    if(std::ifstream file{path, std::ios::binary | std::ios::ate})
    {
        std::streamsize size = file.tellg();
        file.seekg(0, std::ios::beg);
        Buffer buffer(static_cast<size_t>(size));
        if (!file.read(std::data(buffer), size)) {
            std::cerr << "Failed to read file: " << path << std::endl;
            return std::nullopt;
        }
        return buffer;
    }
    std::cerr << "Failed to open file for reading: " << path << std::endl;
    return std::nullopt;
}

bool fileExists(const std::string& path) {
    struct stat buffer;
    return (stat(path.c_str(), &buffer) == 0);
}

bool deleteFile(const std::string &path)
{
    if (std::remove(path.c_str()) != 0) {
        std::cerr << "Error deleting file: " << path << std::endl;
        return false;
    }
    return true;
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
