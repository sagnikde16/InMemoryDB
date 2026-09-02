#include "network/pipeline.h"
#include "core/command.h"
#include <sstream>

namespace kvstore {

Pipeline::Pipeline(WorkerPool& pool, RateLimiter& limiter)
    : pool_(pool), limiter_(limiter) {
    
    auto rate_limiter = std::make_shared<RateLimitHandler>(limiter_);
    auto tokenize = std::make_shared<TokenizeHandler>();
    auto command_parse = std::make_shared<CommandParseHandler>();
    auto dispatch = std::make_shared<DispatchHandler>();

    rate_limiter->set_next(tokenize);
    tokenize->set_next(command_parse);
    command_parse->set_next(dispatch);

    head_ = rate_limiter;
}

void RateLimitHandler::handle(PipelineContext& ctx) {
    // B6: rate limit check — bypass worker pool on rejection
    if (!limiter_.allow(ctx.client_ip)) {
        ctx.pool.completion_queue().push(
            {ctx.client, ctx.seq_id, "-ERR rate limit exceeded\r\n"});
        return;
    }
    PipelineHandler::handle(ctx);
}

void TokenizeHandler::handle(PipelineContext& ctx) {
    // Tokenize by whitespace
    std::istringstream iss(ctx.line);
    std::string token;
    while (iss >> token) ctx.tokens.push_back(std::move(token));
    if (ctx.tokens.empty()) {
        ctx.pool.completion_queue().push(
            {ctx.client, ctx.seq_id, "-ERR empty command\r\n"});
        return;
    }
    PipelineHandler::handle(ctx);
}

void CommandParseHandler::handle(PipelineContext& ctx) {
    ctx.command = CommandFactory::parse(ctx.tokens);
    if (!ctx.command) {
        ctx.pool.completion_queue().push(
            {ctx.client, ctx.seq_id, "-ERR unknown command\r\n"});
        return;
    }
    PipelineHandler::handle(ctx);
}

void DispatchHandler::handle(PipelineContext& ctx) {
    Task task;
    task.command = std::move(ctx.command);
    task.client_fd = ctx.client;
    task.sequence_id = ctx.seq_id;

    if (!ctx.pool.submit(std::move(task))) {
        ctx.pool.completion_queue().push(
            {ctx.client, ctx.seq_id, "-ERR server busy\r\n"});
    }
}

void Pipeline::process(SOCKET client, const std::string& line,
                       uint64_t seq_id, const std::string& client_ip) {
    PipelineContext ctx{
        client,
        line,
        seq_id,
        client_ip,
        {},
        nullptr,
        pool_
    };
    
    if (head_) {
        head_->handle(ctx);
    }
}

} // namespace kvstore
