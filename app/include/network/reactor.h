#pragma once

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <winsock2.h>
#include <ws2tcpip.h>

#include "core/worker_pool.h"
#include <atomic>
#include <cstdint>
#include <map>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace kvstore {

class Pipeline;

// B3: per-connection state with sequence tracking and reassembly buffer
struct ConnectionState {
    std::string read_buffer;
    std::string write_buffer;
    bool write_pending = false;
    std::string client_ip;

    // B3: sequence stamping for ordered pipelined responses
    uint64_t next_seq_issue = 0;
    uint64_t next_seq_expected = 0;
    std::map<uint64_t, std::string> out_of_order_buf;

    // B5: in-flight pipeline depth counter
    uint64_t in_flight_requests = 0;
};

// Non-blocking Winsock2 WSAPoll reactor with completion queue drain,
// sequence reassembly, TCP backpressure, and connection limiting.
class Reactor {
public:
    static constexpr size_t MAX_CONNECTIONS = 10000;
    static constexpr size_t MAX_PIPELINE_DEPTH = 256;

    Reactor(uint16_t port, Pipeline& pipeline, WorkerPool& pool);
    ~Reactor();

    Reactor(const Reactor&) = delete;
    Reactor& operator=(const Reactor&) = delete;

    void start();
    void stop();

private:
    void init_winsock();
    void create_listen_socket();
    void accept_connection();
    void handle_read(SOCKET client);
    void handle_write(SOCKET client);
    void disconnect(SOCKET client);
    void rebuild_poll_set();

    // B2: drain worker results and B3: reassemble ordered responses
    void drain_completion_queue();

    uint16_t port_;
    SOCKET listen_socket_ = INVALID_SOCKET;
    std::atomic<bool> running_{false};

    Pipeline& pipeline_;
    WorkerPool& pool_;

    std::vector<WSAPOLLFD> poll_fds_;
    std::unordered_map<SOCKET, ConnectionState> connections_;

    // B4: sockets paused due to backpressure or pipeline depth
    std::unordered_set<SOCKET> paused_reads_;

    // B5: global connection counter
    std::atomic<size_t> total_connections_{0};
};

} // namespace kvstore
