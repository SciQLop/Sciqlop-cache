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

/*struct Data {
    std::string path;
    float expireTime;
    int accessCount;
};*/

template <Bytes T>
bool storeBytes(const std::string &path, const T &bytes)
{
    std::filesystem::create_directories(std::filesystem::path(path).parent_path());
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
Buffer getBytes(const std::string &path)
{
    std::ifstream file(path, std::ios::binary | std::ios::ate);

    if (!file) {
        std::cerr << "Failed to open file for reading: " << path << std::endl;
        return {};
    }
    std::streamsize size = file.tellg();
    file.seekg(0, std::ios::beg);
    Buffer buffer(static_cast<size_t>(size));
    if (!file.read(std::data(buffer), size)) {
        std::cerr << "Failed to read file: " << path << std::endl;
        return {};
    }
    return buffer;
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
/*
void saveData(const std::unordered_map<std::string, Data> &dataList, const std::string &filename)
{
    std::ofstream out(filename, std::ios::binary);
    if (!out) return;

    size_t mapSize = dataList.size();
    out.write(reinterpret_cast<const char*>(&mapSize), sizeof(mapSize));

    for (const auto& [key, data] : dataList) {
        size_t keyLen = key.size();
        out.write(reinterpret_cast<const char*>(&keyLen), sizeof(keyLen));
        out.write(key.data(), keyLen);

        size_t pathLen = data.path.size();
        out.write(reinterpret_cast<const char*>(&pathLen), sizeof(pathLen));
        out.write(data.path.data(), pathLen);

        out.write(reinterpret_cast<const char*>(&data.expireTime), sizeof(data.expireTime));
        out.write(reinterpret_cast<const char*>(&data.accessCount), sizeof(data.accessCount));
    }
}

std::unordered_map<std::string, Data> loadData(const std::string &filename)
{
    std::unordered_map<std::string, Data> dataList;
    std::ifstream in(filename, std::ios::binary);
    if (!in) return dataList;

    size_t mapSize;
    in.read(reinterpret_cast<char*>(&mapSize), sizeof(mapSize));

    for (size_t i = 0; i < mapSize; ++i) {
        size_t keyLen;
        in.read(reinterpret_cast<char*>(&keyLen), sizeof(keyLen));
        std::string key(keyLen, '\0');
        in.read(&key[0], keyLen);

        size_t pathLen;
        in.read(reinterpret_cast<char*>(&pathLen), sizeof(pathLen));
        std::string path(pathLen, '\0');
        in.read(&path[0], pathLen);

        float expireTime;
        int accessCount;
        in.read(reinterpret_cast<char*>(&expireTime), sizeof(expireTime));
        in.read(reinterpret_cast<char*>(&accessCount), sizeof(accessCount));

        dataList[key] = Data{path, expireTime, accessCount};
    }
    return dataList;
}
*/