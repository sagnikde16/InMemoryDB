#pragma once

#include "engine/hasher.h"
#include <memory>
#include <string>
#include <vector>

namespace kvstore {

class AofLogger; // forward declaration for B7

// Base command interface — each parsed network request becomes a command object
class ICommand {
public:
    virtual ~ICommand() = default;
    virtual std::string execute(ShardRouter& router, AofLogger* aof) = 0;
    virtual bool is_write() const { return false; }
    virtual std::string serialize() const = 0;
};

// SET key value [ttl]
class SetCommand : public ICommand {
public:
    SetCommand(std::string key, std::string value, int64_t ttl = 86400);
    std::string execute(ShardRouter& router, AofLogger* aof) override;
    bool is_write() const override { return true; }
    std::string serialize() const override;
private:
    std::string key_, value_;
    int64_t ttl_;
};

// GET key
class GetCommand : public ICommand {
public:
    explicit GetCommand(std::string key);
    std::string execute(ShardRouter& router, AofLogger* aof) override;
    std::string serialize() const override;
private:
    std::string key_;
};

// DEL key
class DelCommand : public ICommand {
public:
    explicit DelCommand(std::string key);
    std::string execute(ShardRouter& router, AofLogger* aof) override;
    bool is_write() const override { return true; }
    std::string serialize() const override;
private:
    std::string key_;
};

// EXISTS key
class ExistsCommand : public ICommand {
public:
    explicit ExistsCommand(std::string key);
    std::string execute(ShardRouter& router, AofLogger* aof) override;
    std::string serialize() const override;
private:
    std::string key_;
};

// PING
class PingCommand : public ICommand {
public:
    std::string execute(ShardRouter& router, AofLogger* aof) override;
    std::string serialize() const override;
};

// B1: LPUSH key value — prepend to list
class LPushCommand : public ICommand {
public:
    LPushCommand(std::string key, std::string value, int64_t ttl = 86400);
    std::string execute(ShardRouter& router, AofLogger* aof) override;
    bool is_write() const override { return true; }
    std::string serialize() const override;
private:
    std::string key_, value_;
    int64_t ttl_;
};

// B1: LRANGE key start stop — read list range
class LRangeCommand : public ICommand {
public:
    LRangeCommand(std::string key, int64_t start, int64_t stop);
    std::string execute(ShardRouter& router, AofLogger* aof) override;
    std::string serialize() const override;
private:
    std::string key_;
    int64_t start_, stop_;
};

// B1: HSET key field value — set hash field
class HSetCommand : public ICommand {
public:
    HSetCommand(std::string key, std::string field, std::string value, int64_t ttl = 86400);
    std::string execute(ShardRouter& router, AofLogger* aof) override;
    bool is_write() const override { return true; }
    std::string serialize() const override;
private:
    std::string key_, field_, value_;
    int64_t ttl_;
};

// B1: HGET key field — read hash field
class HGetCommand : public ICommand {
public:
    HGetCommand(std::string key, std::string field);
    std::string execute(ShardRouter& router, AofLogger* aof) override;
    std::string serialize() const override;
private:
    std::string key_, field_;
};

class CommandFactory {
public:
    static std::unique_ptr<ICommand> parse(const std::vector<std::string>& tokens);
};

} // namespace kvstore
