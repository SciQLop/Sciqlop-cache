// Reproducer for the DiskStorage data race identified in the pre-Speasy-migration audit.
//
// Bug: DiskStorage's mmap LRU cache (_mmap_cache + _lru_order) is mutated from
// the user-facing path (set/get/del) under _Store::_mtx, AND from the
// background checkpoint thread's _bg_evict path (which calls storage->remove
// directly, without acquiring _mtx). Concurrent mutation of std::unordered_map
// and std::list without synchronization is undefined behaviour.
//
// Run with TSan to detect the race:
//     meson setup build-tsan -Dwith_tests=true -Db_sanitize=thread
//     meson test -C build-tsan concurrency_bugs
//
// The test sets max_size low and writes many large (>8KB) values, so the bg
// thread's eviction path is exercised while user threads continuously read.
// TSan should flag the race on _mmap_cache or _lru_order.

#include <atomic>
#include <chrono>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include <catch2/catch_all.hpp>
#include <catch2/catch_test_macros.hpp>

#include "../common.hpp"
#include "sciqlop_cache/sciqlop_cache.hpp"

SCENARIO("DiskStorage mmap cache races with background eviction thread", "[concurrency][tsan]")
{
    GIVEN("a Cache with very low max_size and aggressive churn of large values")
    {
        AutoCleanDirectory dir { "diskstorage_race" };
        // max_size=256KB → eviction fires almost every bg tick.
        Cache cache(dir.path(), 256 * 1024);

        // 16KB values: file-backed (>8KB threshold), routed through mmap cache.
        constexpr std::size_t value_size = 16 * 1024;
        std::vector<char> payload(value_size, 'x');

        WHEN("user threads churn large values while bg thread evicts")
        {
            std::atomic<bool> stop { false };
            std::atomic<int> ops { 0 };
            std::atomic<int> next_id { 0 };

            // Writer: continuously sets unique large values → grows _total_size
            // past max_size → bg thread's _bg_evict picks them up and calls
            // storage->remove (no _mtx) while reader threads call storage->load.
            auto writer = [&]
            {
                while (!stop.load(std::memory_order_relaxed))
                {
                    int id = next_id.fetch_add(1, std::memory_order_relaxed);
                    cache.set("k" + std::to_string(id), payload);
                    ops.fetch_add(1, std::memory_order_relaxed);
                }
            };

            // Reader: hits storage->load on file-backed values, mutates mmap cache.
            auto reader = [&]
            {
                std::mt19937 rng { std::random_device{}() };
                while (!stop.load(std::memory_order_relaxed))
                {
                    int top = next_id.load(std::memory_order_relaxed);
                    if (top == 0) continue;
                    std::uniform_int_distribution<int> dist(0, top - 1);
                    cache.get("k" + std::to_string(dist(rng)));
                    ops.fetch_add(1, std::memory_order_relaxed);
                }
            };

            // Deleter: triggers storage->remove from user-thread side too — most
            // likely race partner with bg's storage->remove on same DiskStorage.
            auto deleter = [&]
            {
                std::mt19937 rng { std::random_device{}() };
                while (!stop.load(std::memory_order_relaxed))
                {
                    int top = next_id.load(std::memory_order_relaxed);
                    if (top == 0) continue;
                    std::uniform_int_distribution<int> dist(0, top - 1);
                    cache.del("k" + std::to_string(dist(rng)));
                    ops.fetch_add(1, std::memory_order_relaxed);
                }
            };

            std::vector<std::thread> threads;
            for (int i = 0; i < 4; ++i) threads.emplace_back(writer);
            for (int i = 0; i < 4; ++i) threads.emplace_back(reader);
            for (int i = 0; i < 2; ++i) threads.emplace_back(deleter);

            // Bg thread ticks every ~1s. Run 8s for several eviction passes.
            std::this_thread::sleep_for(std::chrono::seconds(8));
            stop.store(true);
            for (auto& t : threads) t.join();

            // The TSan-detectable failure is the race itself, reported by the
            // runtime. The op count is incidental — its value is non-zero only
            // to confirm the test actually exercised the cache.
            REQUIRE(ops.load() > 0);
        }
    }
}
