#include "core/server_builder.h"
#include "engine/hasher.h"
#include "core/command.h"
#include "core/worker_pool.h"
#include "network/pipeline.h"
#include "network/reactor.h"
#include "network/rate_limiter.h"
#include "daemons/aof_logger.h"
#include "daemons/timing_wheel.h"

#include <algorithm>
#include <cctype>
#include <csignal>
#include <fstream>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <thread>

namespace kvstore {

static std::atomic<bool> g_running{true};
static void signal_handler(int) { g_running = false; }

ServerBuilder& ServerBuilder::setPort(uint16_t port) { port_ = port; return *this; }
ServerBuilder& ServerBuilder::setWorkerThreads(size_t count) { worker_threads_ = count; return *this; }
ServerBuilder& ServerBuilder::setMaxMemoryLimit(const std::string& limit) {
    max_memory_ = parse_memory_limit(limit);
    return *this;
}
ServerBuilder& ServerBuilder::setShardCount(size_t count) { shard_count_ = count; return *this; }
ServerBuilder& ServerBuilder::setAofPath(const std::string& path) { aof_path_ = path; return *this; }

void ServerBuilder::build_and_run() {
    std::cout << "=== InMemoryDB Server ===\n"
              << "  Port:           " << port_ << "\n"
              << "  Workers:        " << worker_threads_ << "\n"
              << "  Max Memory:     " << (max_memory_ / (1024 * 1024)) << " MB\n"
              << "  Shards:         " << shard_count_ << "\n"
              << "  AOF Path:       " << aof_path_ << "\n\n";

    ShardRouter router(max_memory_, shard_count_);
    AofLogger aof(aof_path_);

    // B7: replay AOF before starting the reactor
    {
        std::ifstream aof_file(aof_path_);
        if (aof_file.is_open()) {
            aof.set_replaying(true);
            std::string line;
            size_t replayed = 0;
            while (std::getline(aof_file, line)) {
                if (line.empty()) continue;
                std::istringstream iss(line);
                std::vector<std::string> tokens;
                std::string token;
                while (iss >> token) tokens.push_back(std::move(token));
                auto cmd = CommandFactory::parse(tokens);
                if (cmd) { cmd->execute(router, &aof); replayed++; }
            }
            aof.set_replaying(false);
            if (replayed > 0)
                std::cout << "[AOF] Replayed " << replayed << " commands\n";
        }
    }

    aof.start();
    auto wheel = std::make_unique<TimingWheel>(router, &aof);
    wheel->start();

    WorkerPool pool(router, &aof, worker_threads_);

    RateLimiter limiter;
    Pipeline pipeline(pool, limiter);
    Reactor reactor(port_, pipeline, pool);

    std::signal(SIGINT, signal_handler);
    std::cout << "[Server] Starting...\n";

    std::thread reactor_thread([&reactor]() { reactor.start(); });

    while (g_running)
        std::this_thread::sleep_for(std::chrono::milliseconds(200));

    std::cout << "\n[Server] Shutting down...\n";
    reactor.stop();
    if (reactor_thread.joinable()) reactor_thread.join();
    pool.shutdown();
    aof.shutdown();
    wheel->stop();
    std::cout << "[Server] Shutdown complete.\n";
}

size_t ServerBuilder::parse_memory_limit(const std::string& limit) {
    std::string upper = limit;
    std::transform(upper.begin(), upper.end(), upper.begin(),
                   [](unsigned char c) { return std::toupper(c); });

    size_t multiplier = 1;
    std::string numeric = upper;
    if (upper.ends_with("GB")) {
        multiplier = 1024ULL * 1024 * 1024;
        numeric = upper.substr(0, upper.size() - 2);
    } else if (upper.ends_with("MB")) {
        multiplier = 1024ULL * 1024;
        numeric = upper.substr(0, upper.size() - 2);
    } else if (upper.ends_with("KB")) {
        multiplier = 1024;
        numeric = upper.substr(0, upper.size() - 2);
    }
    try { return std::stoull(numeric) * multiplier; }
    catch (...) { throw std::invalid_argument("Invalid memory limit: " + limit); }
}

} // namespace kvstore
