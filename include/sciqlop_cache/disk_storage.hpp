#pragma once

#include <cpp_utils/io/memory_mapped_file.hpp>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <list>
#include <optional>
#include <random>
#include <sqlite3.h>
#include <string>
#include <unordered_map>
#include <uuid.h>
#include "sciqlop_cache/utils/concepts.hpp"
#include "sciqlop_cache/utils/buffer.hpp"

class DiskStorage
{
    std::random_device rd;
    std::mt19937 gen;
    uuids::uuid_random_generator uuid_generator;
    std::filesystem::path _path;

    // LRU mmap handle cache: path_string → shared_ptr<MemoryMappedFile>
    std::size_t _mmap_cache_capacity;
    std::list<std::string> _lru_order;
    std::unordered_map<std::string,
        std::pair<std::shared_ptr<MemoryMappedFile>,
                  std::list<std::string>::iterator>> _mmap_cache;

    void _evict_lru()
    {
        if (_lru_order.empty()) return;
        _mmap_cache.erase(_lru_order.back());
        _lru_order.pop_back();
    }

    void _cache_evict(const std::string& key)
    {
        auto it = _mmap_cache.find(key);
        if (it != _mmap_cache.end())
        {
            _lru_order.erase(it->second.second);
            _mmap_cache.erase(it);
        }
    }

    std::shared_ptr<MemoryMappedFile> _cache_get(const std::string& key)
    {
        auto it = _mmap_cache.find(key);
        if (it == _mmap_cache.end()) return nullptr;
        // Move to front (most recently used)
        _lru_order.splice(_lru_order.begin(), _lru_order, it->second.second);
        return it->second.first;
    }

    void _cache_put(const std::string& key, std::shared_ptr<MemoryMappedFile> mmf)
    {
        if (_mmap_cache_capacity == 0) return;
        _cache_evict(key); // remove old entry if exists
        while (_mmap_cache.size() >= _mmap_cache_capacity)
            _evict_lru();
        _lru_order.push_front(key);
        _mmap_cache[key] = { std::move(mmf), _lru_order.begin() };
    }

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
            throw std::runtime_error(
                std::string("Failed to write file: ") + e.what());
        }
    }

public:
    DiskStorage(const std::filesystem::path& path, std::size_t mmap_cache_capacity = 128)
            : gen(rd()), uuid_generator { gen }, _path(path)
            , _mmap_cache_capacity(mmap_cache_capacity)
    {
        if (!std::filesystem::exists(path))
        {
            std::filesystem::create_directories(path);
        }
    }

    DiskStorage()
            : gen(rd()), uuid_generator { gen }, _path(".")
            , _mmap_cache_capacity(128)
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
        _cache_evict(file_path.string());
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
            auto key = file_path.string();

            // Check mmap cache first
            if (auto cached = _cache_get(key))
                return Buffer(std::static_pointer_cast<IMemoryView>(cached));

            if (!std::filesystem::exists(file_path))
                return std::nullopt;

            auto mmf = std::make_shared<MemoryMappedFile>(key);
            _cache_put(key, mmf);
            return Buffer(std::static_pointer_cast<IMemoryView>(mmf));
        }
        catch (const std::exception& e)
        {
            std::cerr << "Error reading bytes from file: " << e.what() << std::endl;
            return std::nullopt;
        }
    }

    void clear_mmap_cache() { _mmap_cache.clear(); _lru_order.clear(); }


     [[nodiscard]] inline  std::optional<std::filesystem::path> store(const Bytes auto & value)
    {
        auto filename = generate_random_filename();
        auto file_path = _path / filename.substr(0, 2) / filename.substr(2, 2) / filename;
        if (_write(file_path, value))
            return file_path;
        return {};
    }
};
