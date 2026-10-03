#pragma once

#include <cpp_utils/io/memory_mapped_file.hpp>
#include <algorithm>
#include <array>
#include <cerrno>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <list>
#include <mutex>
#include <optional>
#include <random>
#include <sqlite3.h>
#include <string>
#include <system_error>
#include <unordered_map>
#include <uuid.h>
#include "sciqlop_cache/utils/concepts.hpp"
#include "sciqlop_cache/utils/buffer.hpp"

#ifdef _WIN32
#include <fcntl.h>
#include <io.h>
#include <process.h>
inline int _ds_pid() { return _getpid(); }
#else
#include <climits>
#include <fcntl.h>
#include <sys/uio.h>
#include <unistd.h>
inline pid_t _ds_pid() { return getpid(); }
#endif

class DiskStorage
{
    std::random_device rd;
    std::mt19937 gen;
    uuids::uuid_random_generator uuid_generator;
    std::filesystem::path _path;

    // Full-state seed (the stduuid README pattern): seeding mt19937 with a
    // single 32-bit word would collapse the whole filename sequence space to
    // 2^32 possibilities.
    void _seed_engine()
    {
        std::array<unsigned int, std::mt19937::state_size> seed_data;
        std::generate(seed_data.begin(), seed_data.end(), std::ref(rd));
        std::seed_seq seq(seed_data.begin(), seed_data.end());
        gen.seed(seq);
    }

    // LRU mmap handle cache: path_string → shared_ptr<MemoryMappedFile>.
    // The user-facing path (set/get/del) calls into DiskStorage under the
    // store's _mtx, but the background checkpoint thread also calls
    // remove() lock-free during eviction (see _Store::_bg_evict). So this
    // cache needs its own mutex.
    mutable std::mutex _cache_mutex;
    std::size_t _mmap_cache_capacity;
    std::list<std::string> _lru_order;
    std::unordered_map<std::string,
        std::pair<std::shared_ptr<MemoryMappedFile>,
                  std::list<std::string>::iterator>> _mmap_cache;

    void _evict_lru_locked()
    {
        if (_lru_order.empty()) return;
        _mmap_cache.erase(_lru_order.back());
        _lru_order.pop_back();
    }

    void _cache_evict_locked(const std::string& key)
    {
        auto it = _mmap_cache.find(key);
        if (it != _mmap_cache.end())
        {
            _lru_order.erase(it->second.second);
            _mmap_cache.erase(it);
        }
    }

    std::shared_ptr<MemoryMappedFile> _cache_get_locked(const std::string& key)
    {
        auto it = _mmap_cache.find(key);
        if (it == _mmap_cache.end()) return nullptr;
        // Move to front (most recently used)
        _lru_order.splice(_lru_order.begin(), _lru_order, it->second.second);
        return it->second.first;
    }

    void _cache_put_locked(const std::string& key, std::shared_ptr<MemoryMappedFile> mmf)
    {
        if (_mmap_cache_capacity == 0) return;
        _cache_evict_locked(key); // remove old entry if exists
        while (_mmap_cache.size() >= _mmap_cache_capacity)
            _evict_lru_locked();
        _lru_order.push_front(key);
        _mmap_cache[key] = { std::move(mmf), _lru_order.begin() };
    }

    [[noreturn]] static void _throw_write_error(int err, const std::filesystem::path& file_path)
    {
        throw std::system_error(err ? err : EIO, std::generic_category(),
                                "Failed to write file " + file_path.string());
    }

    inline void _write(const std::filesystem::path& file_path, const Payload auto& value)
    {
        errno = 0;
        std::ofstream ofs(file_path, std::ios::binary);
        if (ofs)
        {
            for (const auto& chunk : chunks_of(value))
                ofs.write(chunk.data(), static_cast<std::streamsize>(chunk.size()));
            // close() runs the final flush; without it the destructor flushes
            // AFTER good() is evaluated, so a failed last flush (ENOSPC,
            // quota) silently produced a truncated file under a committed row.
            ofs.close();
        }
        if (!ofs.good())
            _throw_write_error(errno, file_path);
    }

    // Returns 0 or the errno of the failed write.
    [[nodiscard]] static int _write_fd(int fd, std::span<const char> chunk)
    {
        const char* p = chunk.data();
        std::size_t left = chunk.size();
        while (left > 0)
        {
#ifdef _WIN32
            auto n = ::_write(fd, p,
                static_cast<unsigned int>(std::min(left, std::size_t { 1u << 30 })));
#else
            auto n = ::write(fd, p, left);
            // A signal interrupting write() is spurious, not a failure.
            if (n < 0 && errno == EINTR)
                continue;
#endif
            if (n <= 0)
                return n < 0 ? errno : EIO;
            p += n;
            left -= static_cast<std::size_t>(n);
        }
        return 0;
    }

#ifdef _WIN32
    // simplify: one _write per chunk. Windows has no buffered writev
    // (WriteFileGather needs unbuffered, page-aligned I/O); values have a few
    // chunks at most, so this is a couple of extra syscalls.
    [[nodiscard]] static int _write_chunks(int fd, std::span<const std::span<const char>> chunks)
    {
        for (const auto& chunk : chunks)
            if (int err = _write_fd(fd, chunk))
                return err;
        return 0;
    }
#else
    // One writev instead of a write per chunk: on btrfs every extra append
    // to a new file costs ~15 us, as much as writing 64 KB.
    [[nodiscard]] static int _write_chunks(int fd, std::span<const std::span<const char>> chunks)
    {
        std::vector<iovec> iov;
        for (const auto& chunk : chunks)
            if (!chunk.empty())
                iov.push_back({ const_cast<char*>(chunk.data()), chunk.size() });
        std::size_t next = 0;
        while (next < iov.size())
        {
            auto count = static_cast<int>(std::min<std::size_t>(iov.size() - next, IOV_MAX));
            auto n = ::writev(fd, iov.data() + next, count);
            if (n < 0 && errno == EINTR)
                continue;
            if (n <= 0)
                return n < 0 ? errno : EIO;
            for (auto left = static_cast<std::size_t>(n); left > 0;)
            {
                auto step = std::min(left, iov[next].iov_len);
                iov[next].iov_base = static_cast<char*>(iov[next].iov_base) + step;
                iov[next].iov_len -= step;
                left -= step;
                if (iov[next].iov_len == 0)
                    ++next;
            }
        }
        return 0;
    }
#endif

    // Exclusive-create variant of _write: returns false instead of
    // truncating when the file is already there, so a duplicate blob name can
    // never silently overwrite another value's file. Uses raw fds because
    // std::ofstream has no portable create-exclusive mode pre-C++23.
    [[nodiscard]] static int _open_exclusive(const std::filesystem::path& file_path)
    {
#ifdef _WIN32
        return ::_open(file_path.string().c_str(), _O_WRONLY | _O_CREAT | _O_EXCL | _O_BINARY,
                       _S_IREAD | _S_IWRITE);
#else
        return ::open(file_path.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0644);
#endif
    }

    [[nodiscard]] inline bool _write_exclusive(
        const std::filesystem::path& file_path, const Payload auto& value)
    {
        // The directories almost always exist: create them only when the open
        // says they don't. macOS's libc++ create_directories() calls mkdir()
        // before checking, and a failing mkdir costs ~20 us on APFS.
        int fd = _open_exclusive(file_path);
        if (fd < 0 && errno == ENOENT)
        {
            std::filesystem::create_directories(file_path.parent_path());
            fd = _open_exclusive(file_path);
        }
        if (fd < 0)
        {
            if (errno == EEXIST)
                return false;
            _throw_write_error(errno, file_path);
        }
        int err = _write_chunks(fd, chunks_of(value));
#ifdef _WIN32
        if (::_close(fd) != 0 && !err)
#else
        if (::close(fd) != 0 && !err)
#endif
            err = errno;
        if (err)
        {
            // Don't leave a truncated blob behind (ENOSPC, quota, ...).
            std::error_code ignored;
            std::filesystem::remove(file_path, ignored);
            _throw_write_error(err, file_path);
        }
        return true;
    }

public:
    DiskStorage(const std::filesystem::path& path, std::size_t mmap_cache_capacity = 128)
            : uuid_generator { gen }, _path(path)
            , _mmap_cache_capacity(mmap_cache_capacity)
    {
        _seed_engine();
        if (!std::filesystem::exists(path))
        {
            std::filesystem::create_directories(path);
        }
    }

    DiskStorage()
            : uuid_generator { gen }, _path(".")
            , _mmap_cache_capacity(128)
    {
        _seed_engine();
        if (!std::filesystem::exists(_path))
        {
            std::filesystem::create_directories(_path);
        }
    }

    // Draw a fresh full-state seed. Called from _Store::_fork_child() because
    // fork() clones the engine state verbatim: without this, a parent and its
    // children generate lockstep-identical UUID sequences and file-backed
    // values for different keys land on the SAME on-disk path (silent
    // cross-contamination — see tests/fork_safety).
    void reseed() { _seed_engine(); }

    [[nodiscard]] inline std::filesystem::path path() const { return _path; }

    // Paths are stored in the DB relative to the cache root so the whole
    // cache directory can be moved/copied and still resolve. Resolve a stored
    // path back to an absolute one against the current root. Absolute inputs
    // (legacy DBs written before this change, or the root itself) pass through
    // unchanged.
    [[nodiscard]] inline std::filesystem::path abs_path(const std::filesystem::path& stored) const
    {
        return stored.is_absolute() ? stored : _path / stored;
    }

    // Used by callers to tell a genuinely-missing file from a transient open
    // failure (permissions, fd exhaustion, ...) on a file that is still
    // present on disk.
    [[nodiscard]] inline bool file_exists(const std::filesystem::path& stored) const
    {
        return std::filesystem::exists(abs_path(stored));
    }

    [[nodiscard]] inline std::string generate_random_filename()
    {
        // The pid suffix is structural uniqueness that does not depend on the
        // atfork hook firing or on seed quality: two processes can NEVER
        // produce the same name, even with identical residual RNG state. The
        // uuid stays the prefix so the 2-level fanout below keeps spreading
        // files uniformly across directories.
        return uuids::to_string(uuid_generator()) + "-" + std::to_string(_ds_pid());
    }

    inline bool remove(const std::filesystem::path& stored , bool recursive = false)
    {
        auto file_path = abs_path(stored);
        {
            std::lock_guard lk { _cache_mutex };
            _cache_evict_locked(file_path.string());
        }
        // Failure is reported through the return value, not logged: on Windows
        // a file mapped by a live Buffer can't be deleted, which is expected
        // and handled by the caller (_Store::_remove_file retries via trash).
        std::error_code ec;
        if (recursive && std::filesystem::is_directory(file_path, ec))
            return std::filesystem::remove_all(file_path, ec) > 0 && !ec;
        return std::filesystem::remove(file_path, ec) && !ec;
    }

    [[nodiscard]] inline std::optional<Buffer> load(const std::filesystem::path& stored)
    {
        try
        {
            auto file_path = abs_path(stored);
            auto key = file_path.string();

            {
                std::lock_guard lk { _cache_mutex };
                if (auto cached = _cache_get_locked(key))
                    return Buffer(std::static_pointer_cast<IMemoryView>(cached));
            }

            if (!std::filesystem::exists(file_path))
                return std::nullopt;

            auto mmf = std::make_shared<MemoryMappedFile>(key);
            {
                std::lock_guard lk { _cache_mutex };
                _cache_put_locked(key, mmf);
            }
            return Buffer(std::static_pointer_cast<IMemoryView>(mmf));
        }
        catch (const std::exception& e)
        {
            std::cerr << "Error reading bytes from file: " << e.what() << std::endl;
            return std::nullopt;
        }
    }

    void clear_mmap_cache()
    {
        std::lock_guard lk { _cache_mutex };
        _mmap_cache.clear();
        _lru_order.clear();
    }

    [[nodiscard]] std::size_t mmap_cache_size() const
    {
        std::lock_guard lk { _cache_mutex };
        return _mmap_cache.size();
    }


    // Writes `value` to a fresh random blob file and returns its path relative
    // to the storage root. The happy path is a single exclusive create — no
    // callback invocation, no extra work. On EEXIST (astronomically rare with
    // a uuid+pid name) the collision is healed: `is_referenced(rel_path)`
    // tells whether a live cache row owns that path — if yes (genuine
    // collision) a fresh name is generated and retried; if no (orphaned
    // leftover file) it is overwritten in place and reused. The default
    // callback behaves as "always referenced" (always regenerate), keeping
    // DiskStorage usable standalone, DB-agnostic. A write failure throws
    // std::system_error with the real errno (issue #16: returning "no path"
    // dropped it, and callers turned that into a silent data loss).
    [[nodiscard]] inline std::filesystem::path store(
        const Payload auto& value,
        std::function<bool(const std::filesystem::path&)> is_referenced = {})
    {
        for (int attempt = 0; attempt < 16; ++attempt)
        {
            auto filename = generate_random_filename();
            auto rel_path = std::filesystem::path(filename.substr(0, 2))
                / filename.substr(2, 2) / filename;
            if (_write_exclusive(_path / rel_path, value))
                return rel_path;
            if (!is_referenced || is_referenced(rel_path))
                continue; // genuine collision: regenerate a fresh name
            _write(_path / rel_path, value); // orphaned leftover: reuse it
            return rel_path;
        }
        throw std::runtime_error("Failed to find a free blob file name in " + _path.string());
    }
};
