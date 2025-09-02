#pragma once

#include <cpp_utils/io/memory_mapped_file.hpp>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <optional>
#include <random>
#include <sqlite3.h>
#include <string>
#include <uuid.h>
#include "sciqlop_cache/utils/concepts.hpp"
#include "sciqlop_cache/utils/buffer.hpp"

class DiskStorage
{
    std::random_device rd;
    std::mt19937 gen;
    uuids::uuid_random_generator uuid_generator;
    std::filesystem::path _path;

    [[nodiscard]] inline bool _write(const std::filesystem::path& file_path,
                                    const Bytes auto & value)
    {
        try
        {
            std::filesystem::path parent_dir = file_path.parent_path();
            if (!std::filesystem::exists(parent_dir))
            {
                std::filesystem::create_directories(parent_dir);
            }
            std::ofstream ofs(file_path, std::ios::binary);
            if (!ofs)
                return false;
            ofs.write(value.data(), value.size());
            return ofs.good();
        }
        catch (const std::exception& e)
        {
            std::cerr << "Error storing bytes to file: " << e.what() << std::endl;
            return false;
        }
    }

public:
    DiskStorage(const std::filesystem::path& path)
            : gen(rd()), uuid_generator { gen }, _path(path)
    {
        if (!std::filesystem::exists(path))
        {
            std::filesystem::create_directories(path);
        }
    }

    DiskStorage()
            : gen(rd()), uuid_generator { gen }, _path(".")
    {
        if (!std::filesystem::exists(_path))
        {
            std::filesystem::create_directories(_path);
        }
    }

    [[nodiscard]] inline std::filesystem::path path() const { return _path; }

    [[nodiscard]] inline std::string generate_random_filename()
    {
        return uuids::to_string(uuid_generator());
    }

    inline bool remove(const std::filesystem::path& file_path , bool recursive = false)
    {
        try
        {
            if (std::filesystem::exists(file_path))
            {
                if (recursive && std::filesystem::is_directory(file_path))
                    return std::filesystem::remove_all(file_path) > 0;
                else
                    return std::filesystem::remove(file_path);
            }
            return false;
        }
        catch (const std::exception& e)
        {
            std::cerr << "Error deleting file: " << e.what() << std::endl;
            return false;
        }
    }

    [[nodiscard]] inline std::optional<Buffer> load(const std::filesystem::path& file_path)
    {
        try
        {
            if (!std::filesystem::exists(file_path))
            {
                return std::nullopt;
            }
            return Buffer(file_path);
        }
        catch (const std::exception& e)
        {
            std::cerr << "Error reading bytes from file: " << e.what() << std::endl;
            return std::nullopt;
        }
    }


     [[nodiscard]] inline  std::optional<std::filesystem::path> store(const Bytes auto & value)
    {
        auto filename = generate_random_filename();
        auto file_path = _path / filename.substr(0, 2) / filename.substr(2, 2) / filename;
        if (_write(file_path, value))
            return file_path;
        return {};
    }
};
