#pragma once

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <winsock2.h>

#include "core/command.h"
#include "engine/hasher.h"
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <vector>

namespace kvstore {

class AofLogger;

// B2: unit of work — no callback, workers post results to completion queue
struct Task {
    std::unique_ptr<ICommand> command;
    SOCKET client_fd;
    uint64_t sequence_id;
};

// B2: worker result posted to the reactor's completion queue
struct TaskResult {
    SOCKET client_fd;
    uint64_t sequence_id;
    std::string response;
};

// B2: thread-safe queue between workers and reactor
class CompletionQueue {
public:
    void push(TaskResult result);
    void drain(std::vector<TaskResult>& out);

private:
    std::mutex mutex_;
    std::queue<TaskResult> queue_;
};

// SPMC worker pool — executes commands, posts results to completion queue
class WorkerPool {
public:
    WorkerPool(ShardRouter& router, AofLogger* aof,
               size_t thread_count, size_t max_queue_size = 10000);
    ~WorkerPool();

    WorkerPool(const WorkerPool&) = delete;
    WorkerPool& operator=(const WorkerPool&) = delete;

    bool submit(Task task);
    size_t queue_size() const;
    size_t max_queue_size() const;
    bool is_full() const;
    CompletionQueue& completion_queue();
    void shutdown();

private:
    void worker_loop();

    ShardRouter& router_;
    AofLogger* aof_;
    size_t max_queue_size_;
    std::atomic<bool> running_{true};

    mutable std::mutex mutex_;
    std::condition_variable cv_;
    std::queue<Task> queue_;
    std::vector<std::thread> workers_;
    CompletionQueue completion_queue_;
};

} // namespace kvstore
