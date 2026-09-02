#include "daemons/aof_logger.h"
#include <iostream>

namespace kvstore {

AofLogger::AofLogger(const std::string& filepath)
    : filepath_(filepath)
    , active_buffer_(&buffer_a_)
    , flush_buffer_(&buffer_b_) {}

AofLogger::~AofLogger() { shutdown(); }

void AofLogger::log(const std::string& command) {
    std::lock_guard lock(mutex_);
    if (running_) active_buffer_->push_back(command);
}

void AofLogger::start() {
    file_.open(filepath_, std::ios::app | std::ios::binary);
    if (!file_.is_open())
        throw std::runtime_error("Failed to open AOF file: " + filepath_);
    running_ = true;
    flush_thread_ = std::thread(&AofLogger::flush_loop, this);
}

void AofLogger::shutdown() {
    if (!running_.exchange(false)) return;
    cv_.notify_all();
    if (flush_thread_.joinable()) flush_thread_.join();
    flush();
    file_.close();
}

void AofLogger::flush() {
    std::lock_guard lock(mutex_);
    write_buffer_to_disk(*active_buffer_);
}

void AofLogger::flush_loop() {
    while (running_) {
        {
            std::unique_lock lock(mutex_);
            cv_.wait_for(lock, std::chrono::seconds(1),
                         [this] { return !running_.load(); });
        }
        {
            std::lock_guard lock(mutex_);
            std::swap(active_buffer_, flush_buffer_);
        }
        write_buffer_to_disk(*flush_buffer_);
    }
}

void AofLogger::write_buffer_to_disk(std::vector<std::string>& buffer) {
    if (buffer.empty()) return;
    for (const auto& cmd : buffer) file_ << cmd << "\n";
    file_.flush();
    buffer.clear();
}

bool AofLogger::is_replaying() const noexcept { return is_replaying_; }
void AofLogger::set_replaying(bool v) noexcept { is_replaying_ = v; }
const std::string& AofLogger::filepath() const noexcept { return filepath_; }

} // namespace kvstore
