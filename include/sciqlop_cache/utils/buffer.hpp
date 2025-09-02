#pragma once

#include <cpp_utils/io/memory_mapped_file.hpp>
#include <cstdio>
#include <filesystem>
#include <memory>
#include <sqlite3.h>
#include <string>
#include <uuid.h>
#include <vector>

using namespace cpp_utils::io;

class IMemoryView
{
public:
    virtual const char* data() const noexcept = 0;
    virtual size_t size() const noexcept = 0;
    virtual operator bool() const noexcept = 0;
    virtual std::vector<char> to_vector() const = 0;
};

class MemoryMappedFile:public IMemoryView
{
    memory_mapped_file mmf;
public:
    MemoryMappedFile(const std::string& path)
            : mmf(path)
    {
        if (!mmf.is_valid())
            throw std::runtime_error("Failed to open memory-mapped file: " + path);
    }

    ~MemoryMappedFile() = default;

    [[nodiscard]] inline operator bool() const noexcept { return  mmf.is_valid(); }

    [[nodiscard]] inline const char* data() const noexcept { return mmf.data(); }

    [[nodiscard]] inline size_t size() const noexcept { return mmf.size(); }

    [[nodiscard]] inline std::vector<char> to_vector() const
    {
        return std::vector<char>(mmf.data(), mmf.data() + mmf.size());
    }
};

class VectorMemoryView:public IMemoryView
{
    std::vector<char> vec;
public:
    VectorMemoryView(std::vector<char>&& v) : vec(std::move(v)) {}
    ~VectorMemoryView() = default;

    [[nodiscard]] inline operator bool() const noexcept { return !vec.empty(); }

    [[nodiscard]] inline const char* data() const noexcept { return vec.data(); }

    [[nodiscard]] inline size_t size() const noexcept { return vec.size(); }

    [[nodiscard]] inline std::vector<char> to_vector() const { return vec; }
};

class Buffer
{
    std::shared_ptr<IMemoryView> _data;

public:
    Buffer(const std::filesystem::path& path)
            : _data(std::make_shared<MemoryMappedFile>(path.string()))
    {
    }

    Buffer(std::vector<char>&& vec) : _data(std::make_shared<VectorMemoryView>(std::move(vec))) {}

    Buffer(IMemoryView* view) : _data(view) {}

    Buffer(const Buffer& other) : _data(other._data) { }

    Buffer(Buffer&& other) noexcept
            : _data(std::move(other._data))
    {
        other._data = nullptr;
    }

    ~Buffer() = default;

    Buffer& operator=(const Buffer& other)
    {
        if (this != &other)
        {
            _data = other._data;
        }
        return *this;
    }

    Buffer& operator=(Buffer&& other) noexcept
    {
        if (this != &other)
        {
            _data = std::move(other._data);
            other._data = nullptr;
        }
        return *this;
    }

    [[nodiscard]] inline operator bool() const noexcept { return _data && bool(*_data); }

    [[nodiscard]] inline const char* data() const noexcept { return _data->data(); }

    [[nodiscard]] inline size_t size() const noexcept { return _data->size(); }

    [[nodiscard]] inline std::vector<char> to_vector() const
    {
        return _data->to_vector();
    }
};
