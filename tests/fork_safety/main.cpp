#include <atomic>
#include <cstdlib>
#include <string>
#include <thread>
#include <vector>

#include <sys/wait.h>
#include <unistd.h>

#include <catch2/catch_all.hpp>
#include <catch2/catch_test_macros.hpp>

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
