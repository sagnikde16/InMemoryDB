#include "core/worker_pool.h"

namespace kvstore {

// --- CompletionQueue ---

void CompletionQueue::push(TaskResult result) {
    std::lock_guard lock(mutex_);
    queue_.push(std::move(result));
}

void CompletionQueue::drain(std::vector<TaskResult>& out) {
    std::lock_guard lock(mutex_);
    while (!queue_.empty()) {
        out.push_back(std::move(queue_.front()));
        queue_.pop();
    }
}

// --- WorkerPool ---

WorkerPool::WorkerPool(ShardRouter& router, AofLogger* aof,
                       size_t thread_count, size_t max_queue_size)
    : router_(router), aof_(aof), max_queue_size_(max_queue_size) {
    workers_.reserve(thread_count);
    for (size_t i = 0; i < thread_count; i++)
        workers_.emplace_back(&WorkerPool::worker_loop, this);
}

WorkerPool::~WorkerPool() { shutdown(); }

bool WorkerPool::submit(Task task) {
    {
        std::lock_guard lock(mutex_);
        if (!running_ || queue_.size() >= max_queue_size_) return false;
        queue_.push(std::move(task));
    }
    cv_.notify_one();
    return true;
}

size_t WorkerPool::queue_size() const {
    std::lock_guard lock(mutex_);
    return queue_.size();
}

size_t WorkerPool::max_queue_size() const { return max_queue_size_; }

bool WorkerPool::is_full() const {
    std::lock_guard lock(mutex_);
    return queue_.size() >= max_queue_size_;
}

CompletionQueue& WorkerPool::completion_queue() { return completion_queue_; }

void WorkerPool::shutdown() {
    {
        std::lock_guard lock(mutex_);
        if (!running_) return;
        running_ = false;
    }
    cv_.notify_all();
    for (auto& w : workers_)
        if (w.joinable()) w.join();
}

// B2: workers never touch Connection objects — post results to completion queue
void WorkerPool::worker_loop() {
    while (true) {
        Task task;
        {
            std::unique_lock lock(mutex_);
            cv_.wait(lock, [this] { return !queue_.empty() || !running_; });
            if (!running_ && queue_.empty()) return;
            if (queue_.empty()) continue;
            task = std::move(queue_.front());
            queue_.pop();
        }

        std::string response = task.command->execute(router_, aof_);
        completion_queue_.push({task.client_fd, task.sequence_id, std::move(response)});
    }
}

} // namespace kvstore
