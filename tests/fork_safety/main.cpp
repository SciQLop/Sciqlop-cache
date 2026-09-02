#include <catch2/catch_all.hpp>
#include <catch2/catch_test_macros.hpp>

#ifdef _WIN32

// Windows has no fork(); multiprocessing uses spawn, so the inherited-mutex
// hazard this test guards against cannot occur. Keep a placeholder so the
// suite is uniform across platforms.
TEST_CASE("fork safety (POSIX only)")
{
    SKIP("fork() is unavailable on Windows; multiprocessing uses spawn");
}

#else

#include <atomic>
#include <cstdlib>
#include <string>
#include <thread>
#include <vector>

#include <sys/wait.h>
#include <unistd.h>

#include "../common.hpp"
#include "sciqlop_cache/sciqlop_cache.hpp"


// A live Cache runs a background checkpoint thread that periodically holds the
// store mutex. Forking while another thread holds it used to deadlock the
// child on its first cache op; the pthread_atfork handlers must quiesce and
// rebuild the store so the child can keep using the inherited cache.
TEST_CASE("forked child can use the inherited cache without deadlocking")
{
    AutoCleanDirectory dir("fork_safety");
    Cache cache(dir.path() / "c");
    cache.set("k", std::string("0"));

    std::atomic<bool> stop { false };
    std::vector<std::thread> hammer;
    for (int i = 0; i < 4; ++i)
        hammer.emplace_back([&] { while (!stop.load()) (void)cache.get("k"); });

    for (int round = 0; round < 20; ++round)
    {
        pid_t pid = fork();
        REQUIRE(pid >= 0);
        if (pid == 0) // child: must reach this and not block on the inherited mutex
        {
            cache.set("k", std::string("child"));
            const bool ok = cache.get("k").has_value();
            // exit() (not _exit()) so libgcov's atexit handler flushes this
            // child's counters, recording coverage of the child-side fork
            // recovery path. The inherited Cache is a stack object, so exit()
            // does not run its destructor.
            std::exit(ok ? 0 : 2);
        }

        int status = 0;
        for (int waited = 0; waited < 1500; ++waited) // up to 15 s
        {
            if (waitpid(pid, &status, WNOHANG) == pid)
                break;
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            if (waited == 1499)
            {
                kill(pid, SIGKILL);
                waitpid(pid, &status, 0);
                FAIL("child deadlocked using the fork-inherited cache");
            }
        }
        REQUIRE(WIFEXITED(status));
        REQUIRE(WEXITSTATUS(status) == 0);
    }

    stop.store(true);
    for (auto& t : hammer)
        t.join();
}

// Regression test for the cross-contamination root cause: DiskStorage's
// mt19937 is seeded once at construction, so fork() clones its exact state
// into every child. Two children storing file-backed (>8KB) values then
// generate the IDENTICAL UUID sequence and write different keys' values to
// the SAME on-disk path — last writer wins, both rows point at it, and one
// key reads back the other key's bytes. The fix (full-state reseed in
// _fork_child + getpid() in the blob filename + exclusive-create) must make
// the two rows land in distinct files with intact values.
TEST_CASE("forked children storing file-backed values never share an on-disk path")
{
    AutoCleanDirectory dir("fork_uuid_collision");
    Cache cache(dir.path() / "c");

    // Distinct sizes so a contaminated read is unambiguous.
    const std::string payload_a(16 * 1024, 'A');
    const std::string payload_b(20 * 1024, 'B');

    for (int child = 0; child < 2; ++child)
    {
        pid_t pid = fork();
        REQUIRE(pid >= 0);
        if (pid == 0) // child: inherits the parent's PRNG state pre-fix
        {
            if (child == 0)
                cache.set("key_a", payload_a);
            else
                cache.set("key_b", payload_b);
            std::exit(0);
        }
        int status = 0;
        waitpid(pid, &status, 0); // sequential: deterministic shared PRNG state
        REQUIRE(WIFEXITED(status));
        REQUIRE(WEXITSTATUS(status) == 0);
    }

    // Each key must read back its OWN payload, not the sibling's bytes.
    auto a = cache.get("key_a");
    auto b = cache.get("key_b");
    REQUIRE(a.has_value());
    REQUIRE(b.has_value());
    CHECK(a->to_vector() == std::vector<char>(payload_a.begin(), payload_a.end()));
    CHECK(b->to_vector() == std::vector<char>(payload_b.begin(), payload_b.end()));

    // And the two file-backed rows must live in DISTINCT on-disk files.
    std::size_t blob_files = 0;
    for (const auto& entry : std::filesystem::recursive_directory_iterator(dir.path() / "c"))
    {
        auto fname = entry.path().filename().string();
        if (entry.is_regular_file() && fname != Cache::db_fname
            && !fname.starts_with(std::string(Cache::db_fname)))
            ++blob_files;
    }
    CHECK(blob_files == 2);
}

#endif // _WIN32
