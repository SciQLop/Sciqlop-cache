#pragma once

#include <cstddef>
#include <filesystem>
#include <fmt/format.h>
#include <functional>
#include <memory>
#include <string>
#include <vector>

template <typename StoreType>
class FanoutStore
{
    std::vector<std::unique_ptr<StoreType>> _shards;

    std::size_t _shard_for(const std::string& key) const
    {
        return std::hash<std::string> {}(key) % _shards.size();
    }

    StoreType& _shard(const std::string& key) { return *_shards[_shard_for(key)]; }

    template <typename F>
    void _for_each_shard(F&& f)
    {
        for (auto& s : _shards)
            f(*s);
    }

public:
    FanoutStore(const FanoutStore&) = delete;
    FanoutStore& operator=(const FanoutStore&) = delete;
    FanoutStore(FanoutStore&&) = default;
    FanoutStore& operator=(FanoutStore&&) = default;

    explicit FanoutStore(const std::filesystem::path& path,
                         std::size_t shard_count = 8,
                         std::size_t max_size = 0)
    {
        _shards.reserve(shard_count);
        for (std::size_t i = 0; i < shard_count; ++i)
        {
            auto shard_path = path / fmt::format("{:02d}", i);
            std::filesystem::create_directories(shard_path);
            _shards.push_back(std::make_unique<StoreType>(shard_path, max_size));
        }
    }

    [[nodiscard]] std::size_t shard_count() const { return _shards.size(); }

    [[nodiscard]] bool exists(const std::string& key) { return _shard(key).exists(key); }

    inline bool del(const std::string& key) { return _shard(key).del(key); }

    [[nodiscard]] inline std::optional<Buffer> get(const std::string& key)
    {
        return _shard(key).get(key);
    }

    [[nodiscard]] inline std::optional<Buffer> pop(const std::string& key)
    {
        return _shard(key).pop(key);
    }

    // --- set() overloads ---

    inline bool set(const std::string& key, const Bytes auto& value)
    {
        return _shard(key).set(key, value);
    }

    inline bool set(const std::string& key, const Bytes auto& value, DurationConcept auto expire)
        requires requires(StoreType& s, const std::string& k, const decltype(value)& v, decltype(expire) e) { s.set(k, v, e); }
    {
        return _shard(key).set(key, value, expire);
    }

    inline bool set(const std::string& key, const Bytes auto& value, const std::string& tag)
        requires requires(StoreType& s, const std::string& k, const decltype(value)& v, const std::string& t) { s.set(k, v, t); }
    {
        return _shard(key).set(key, value, tag);
    }

    inline bool set(const std::string& key, const Bytes auto& value,
                    DurationConcept auto expire, const std::string& tag)
        requires requires(StoreType& s, const std::string& k, const decltype(value)& v, decltype(expire) e, const std::string& t) { s.set(k, v, e, t); }
    {
        return _shard(key).set(key, value, expire, tag);
    }

    // --- add() overloads ---

    inline bool add(const std::string& key, const Bytes auto& value)
    {
        return _shard(key).add(key, value);
    }

    inline bool add(const std::string& key, const Bytes auto& value, DurationConcept auto expire)
        requires requires(StoreType& s, const std::string& k, const decltype(value)& v, decltype(expire) e) { s.add(k, v, e); }
    {
        return _shard(key).add(key, value, expire);
    }

    inline bool add(const std::string& key, const Bytes auto& value, const std::string& tag)
        requires requires(StoreType& s, const std::string& k, const decltype(value)& v, const std::string& t) { s.add(k, v, t); }
    {
        return _shard(key).add(key, value, tag);
    }

    inline bool add(const std::string& key, const Bytes auto& value,
                    DurationConcept auto expire, const std::string& tag)
        requires requires(StoreType& s, const std::string& k, const decltype(value)& v, decltype(expire) e, const std::string& t) { s.add(k, v, e, t); }
    {
        return _shard(key).add(key, value, expire, tag);
    }

    // --- Aggregated operations ---

    [[nodiscard]] std::size_t count()
    {
        std::size_t total = 0;
        _for_each_shard([&](auto& s) { total += s.count(); });
        return total;
    }

    [[nodiscard]] std::size_t size()
    {
        std::size_t total = 0;
        _for_each_shard([&](auto& s) { total += s.size(); });
        return total;
    }

    [[nodiscard]] std::vector<std::string> keys()
    {
        std::vector<std::string> all;
        _for_each_shard([&](auto& s) {
            auto k = s.keys();
            all.insert(all.end(), std::make_move_iterator(k.begin()),
                       std::make_move_iterator(k.end()));
        });
        return all;
    }

    class KeyCursor
    {
        std::vector<std::unique_ptr<StoreType>>& _shards;
        std::size_t _shard_idx = 0;
        std::optional<typename StoreType::KeyCursor> _cursor;

        void _advance_shard()
        {
            _cursor.reset();
            while (_shard_idx < _shards.size())
            {
                _cursor.emplace(_shards[_shard_idx]->iterkeys());
                auto val = _cursor->next();
                if (val)
                {
                    _pending = std::move(*val);
                    return;
                }
                _cursor.reset();
                ++_shard_idx;
            }
        }

        std::optional<std::string> _pending;

    public:
        explicit KeyCursor(std::vector<std::unique_ptr<StoreType>>& shards)
            : _shards(shards)
        {
            _advance_shard();
        }

        KeyCursor(const KeyCursor&) = delete;
        KeyCursor& operator=(const KeyCursor&) = delete;
        KeyCursor(KeyCursor&&) = default;

        std::optional<std::string> next()
        {
            if (!_pending) return std::nullopt;
            auto result = std::move(*_pending);
            _pending.reset();
            if (_cursor)
            {
                auto val = _cursor->next();
                if (val)
                    _pending = std::move(*val);
                else
                {
                    _cursor.reset();
                    ++_shard_idx;
                    _advance_shard();
                }
            }
            return result;
        }
    };

    [[nodiscard]] KeyCursor iterkeys()
    {
        return KeyCursor(_shards);
    }

    void clear()
    {
        _for_each_shard([](auto& s) { s.clear(); });
    }

    using CheckResult = typename StoreType::CheckResult;

    CheckResult check(bool fix = false)
    {
        CheckResult combined;
        _for_each_shard([&](auto& s) {
            auto r = s.check(fix);
            combined.orphaned_files += r.orphaned_files;
            combined.dangling_rows += r.dangling_rows;
            combined.size_mismatches += r.size_mismatches;
            if (!r.counters_consistent) combined.counters_consistent = false;
            if (!r.sqlite_integrity_ok) combined.sqlite_integrity_ok = false;
        });
        combined.ok = combined.sqlite_integrity_ok
                   && combined.dangling_rows == 0
                   && combined.size_mismatches == 0
                   && combined.orphaned_files == 0
                   && combined.counters_consistent;
        return combined;
    }

    [[nodiscard]] std::filesystem::path path() const
    {
        return _shards[0]->path().parent_path();
    }

    // --- Expiration ---

    inline bool touch(const std::string& key, DurationConcept auto expire)
        requires requires(StoreType& s, const std::string& k, decltype(expire) e) { s.touch(k, e); }
    {
        return _shard(key).touch(key, expire);
    }

    inline void expire()
        requires requires(StoreType& s) { s.expire(); }
    {
        _for_each_shard([](auto& s) { s.expire(); });
    }

    // --- Eviction ---

    inline std::size_t evict()
        requires requires(StoreType& s) { s.evict(); }
    {
        std::size_t total = 0;
        _for_each_shard([&](auto& s) { total += s.evict(); });
        return total;
    }

    inline void set_max_cache_size(std::size_t value)
        requires requires(StoreType& s) { s.set_max_cache_size(value); }
    {
        _for_each_shard([value](auto& s) { s.set_max_cache_size(value); });
    }

    [[nodiscard]] inline std::size_t max_cache_size()
        requires requires(StoreType& s) { s.max_cache_size(); }
    {
        return _shards[0]->max_cache_size();
    }

    // --- Tags ---

    inline std::size_t evict_tag(const std::string& tag)
        requires requires(StoreType& s) { s.evict_tag(tag); }
    {
        std::size_t total = 0;
        _for_each_shard([&](auto& s) { total += s.evict_tag(tag); });
        return total;
    }

    // --- Stats ---

    struct Stats { uint64_t hits; uint64_t misses; };

    auto stats()
        requires requires(StoreType& s) { s.stats(); }
    {
        uint64_t h = 0, m = 0;
        _for_each_shard([&](auto& s) {
            auto st = s.stats();
            h += st.hits;
            m += st.misses;
        });
        return Stats { h, m };
    }

    void reset_stats()
        requires requires(StoreType& s) { s.reset_stats(); }
    {
        _for_each_shard([](auto& s) { s.reset_stats(); });
    }

    // --- incr / decr ---

    inline int64_t incr(const std::string& key, int64_t delta = 1, int64_t default_value = 0)
    {
        return _shard(key).incr(key, delta, default_value);
    }

    inline int64_t decr(const std::string& key, int64_t delta = 1, int64_t default_value = 0)
    {
        return _shard(key).decr(key, delta, default_value);
    }

    // --- meta (per-shard, keyed) ---

    inline void set_meta(const std::string& key, const std::string& value)
    {
        _shard(key).set_meta(key, value);
    }

    [[nodiscard]] inline std::optional<std::string> get_meta(const std::string& key)
    {
        return _shard(key).get_meta(key);
    }

    // --- transact (scoped to shard for key) ---

    auto begin_user_transaction(const std::string& key)
    {
        return _shard(key).begin_user_transaction();
    }
};
