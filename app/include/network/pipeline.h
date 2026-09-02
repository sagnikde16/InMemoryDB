#pragma once

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <winsock2.h>

#include "core/worker_pool.h"
#include "network/rate_limiter.h"
#include <string>
#include <vector>
#include <memory>

namespace kvstore {

class ICommand;

struct PipelineContext {
    SOCKET client;
    std::string line;
    uint64_t seq_id;
    std::string client_ip;
    std::vector<std::string> tokens;
    std::unique_ptr<ICommand> command;
    WorkerPool& pool;
};

class PipelineHandler {
public:
    virtual ~PipelineHandler() = default;
    
    void set_next(std::shared_ptr<PipelineHandler> next) {
        next_ = next;
    }
    
    virtual void handle(PipelineContext& ctx) {
        if (next_) {
            next_->handle(ctx);
        }
    }
    
protected:
    std::shared_ptr<PipelineHandler> next_;
};

class RateLimitHandler : public PipelineHandler {
public:
    RateLimitHandler(RateLimiter& limiter) : limiter_(limiter) {}
    void handle(PipelineContext& ctx) override;
private:
    RateLimiter& limiter_;
};

class TokenizeHandler : public PipelineHandler {
public:
    void handle(PipelineContext& ctx) override;
};

class CommandParseHandler : public PipelineHandler {
public:
    void handle(PipelineContext& ctx) override;
};

class DispatchHandler : public PipelineHandler {
public:
    void handle(PipelineContext& ctx) override;
};

// Simplified pipeline: rate limit check → tokenize → command factory → submit.
// Connection/pipeline limiting is handled by the Reactor.
class Pipeline {
public:
    Pipeline(WorkerPool& pool, RateLimiter& limiter);

    // Parse line into a command and submit to worker pool
    void process(SOCKET client, const std::string& line,
                 uint64_t seq_id, const std::string& client_ip);

private:
    WorkerPool& pool_;
    RateLimiter& limiter_;
    std::shared_ptr<PipelineHandler> head_;
};

} // namespace kvstore
