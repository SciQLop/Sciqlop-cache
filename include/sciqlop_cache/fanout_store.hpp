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
    std::size_t _shard_count;

    std::size_t _shard_for(const std::string& key) const
    {
        return std::hash<std::string> {}(key) % _shard_count;
    }

    StoreType& _shard(const std::string& key) { return *_shards[_shard_for(key)]; }

    template <typename F>
    void _for_each_shard(F&& f)
    {
        for (auto& s : _shards)
            f(*s);
    }

public:
    explicit FanoutStore(const std::filesystem::path& path,
                         std::size_t shard_count = 8,
                         std::size_t max_size = 0)
        : _shard_count(shard_count)
    {
        _shards.reserve(_shard_count);
        for (std::size_t i = 0; i < _shard_count; ++i)
        {
            auto shard_path = path / fmt::format("{:02d}", i);
            std::filesystem::create_directories(shard_path);
            _shards.push_back(std::make_unique<StoreType>(shard_path, max_size));
        }
    }

    [[nodiscard]] std::size_t shard_count() const { return _shard_count; }

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

    void clear()
    {
        _for_each_shard([](auto& s) { s.clear(); });
    }

    bool check()
    {
        bool ok = true;
        _for_each_shard([&](auto& s) { ok &= s.check(); });
        return ok;
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
