#pragma once

#include <atomic>
#include <condition_variable>
#include <fstream>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace kvstore {

class ShardRouter; // forward declaration for replay

// Dual-buffered AOF logger with 1s fsync and replay support.
class AofLogger {
public:
    explicit AofLogger(const std::string& filepath);
    ~AofLogger();

    AofLogger(const AofLogger&) = delete;
    AofLogger& operator=(const AofLogger&) = delete;

    void log(const std::string& command);
    void flush();
    void start();
    void shutdown();

    // B7: replay flag — prevents re-logging during startup replay
    bool is_replaying() const noexcept;
    void set_replaying(bool v) noexcept;

    const std::string& filepath() const noexcept;

private:
    void flush_loop();
    void write_buffer_to_disk(std::vector<std::string>& buffer);

    std::string filepath_;
    std::ofstream file_;

    std::vector<std::string> buffer_a_;
    std::vector<std::string> buffer_b_;
    std::vector<std::string>* active_buffer_;
    std::vector<std::string>* flush_buffer_;

    mutable std::mutex mutex_;
    std::condition_variable cv_;
    std::thread flush_thread_;
    std::atomic<bool> running_{false};
    std::atomic<bool> is_replaying_{false};
};

} // namespace kvstore
