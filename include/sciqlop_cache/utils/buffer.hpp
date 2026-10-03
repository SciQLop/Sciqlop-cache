#pragma once

#include <cpp_utils/io/memory_mapped_file.hpp>
#include <cerrno>
#include <cstdio>
#include <filesystem>
#include <memory>
#include <sqlite3.h>
#include <string>
#include <system_error>
#include <uuid.h>
#include <vector>
#ifndef _WIN32
#include <sys/mman.h>
#endif

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

#ifndef _WIN32
// Maps an already-open file, so the caller's open + fstat are the only path
// lookups. The mapping outlives the fd: the caller closes it right after.
class FdMappedFile:public IMemoryView
{
    char* _data;
    std::size_t _size;
public:
    FdMappedFile(int fd, std::size_t size)
            : _data(static_cast<char*>(::mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0)))
            , _size(size)
    {
        if (_data == MAP_FAILED)
            throw std::system_error(errno, std::generic_category(), "mmap failed");
    }

    FdMappedFile(const FdMappedFile&) = delete;
    FdMappedFile& operator=(const FdMappedFile&) = delete;

    ~FdMappedFile() { ::munmap(_data, _size); }

    [[nodiscard]] inline operator bool() const noexcept { return true; }

    [[nodiscard]] inline const char* data() const noexcept { return _data; }

    [[nodiscard]] inline size_t size() const noexcept { return _size; }

    [[nodiscard]] inline std::vector<char> to_vector() const
    {
        return std::vector<char>(_data, _data + _size);
    }
};
#endif

// Uninitialised heap bytes: a std::vector<char> would zero them just before
// read() overwrites them.
class HeapMemoryView:public IMemoryView
{
    std::unique_ptr<char[]> _bytes;
    std::size_t _size;
public:
    explicit HeapMemoryView(std::size_t size)
            : _bytes(std::make_unique_for_overwrite<char[]>(size)), _size(size)
    {
    }

    [[nodiscard]] inline char* mutable_data() noexcept { return _bytes.get(); }

    [[nodiscard]] inline operator bool() const noexcept { return _size != 0; }

    [[nodiscard]] inline const char* data() const noexcept { return _bytes.get(); }

    [[nodiscard]] inline size_t size() const noexcept { return _size; }

    [[nodiscard]] inline std::vector<char> to_vector() const
    {
        return std::vector<char>(_bytes.get(), _bytes.get() + _size);
    }
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

    Buffer(std::shared_ptr<IMemoryView> view) : _data(std::move(view)) {}

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

    // Null guard so a moved-from Buffer is still safe to inspect — accessors
    // return zero/empty rather than dereferencing a null _data pointer.
    [[nodiscard]] inline const char* data() const noexcept { return _data ? _data->data() : nullptr; }

    [[nodiscard]] inline size_t size() const noexcept { return _data ? _data->size() : 0; }

    [[nodiscard]] inline std::vector<char> to_vector() const
    {
        return _data ? _data->to_vector() : std::vector<char>{};
    }
};
