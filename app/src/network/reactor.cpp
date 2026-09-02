#include "network/reactor.h"
#include "network/pipeline.h"
#include <iostream>

namespace kvstore {

Reactor::Reactor(uint16_t port, Pipeline& pipeline, WorkerPool& pool)
    : port_(port), pipeline_(pipeline), pool_(pool) {}

Reactor::~Reactor() {
    stop();
    if (listen_socket_ != INVALID_SOCKET) closesocket(listen_socket_);
    WSACleanup();
}

void Reactor::init_winsock() {
    WSADATA wsa_data;
    int result = WSAStartup(MAKEWORD(2, 2), &wsa_data);
    if (result != 0)
        throw std::runtime_error("WSAStartup failed: " + std::to_string(result));
}

void Reactor::create_listen_socket() {
    listen_socket_ = WSASocketW(AF_INET, SOCK_STREAM, IPPROTO_TCP,
                                nullptr, 0, WSA_FLAG_OVERLAPPED);
    if (listen_socket_ == INVALID_SOCKET)
        throw std::runtime_error("WSASocket failed: " + std::to_string(WSAGetLastError()));

    int opt = 1;
    setsockopt(listen_socket_, SOL_SOCKET, SO_REUSEADDR,
               reinterpret_cast<const char*>(&opt), sizeof(opt));

    u_long mode = 1;
    ioctlsocket(listen_socket_, FIONBIO, &mode);

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port_);

    if (bind(listen_socket_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == SOCKET_ERROR)
        throw std::runtime_error("Bind failed: " + std::to_string(WSAGetLastError()));
    if (listen(listen_socket_, SOMAXCONN) == SOCKET_ERROR)
        throw std::runtime_error("Listen failed: " + std::to_string(WSAGetLastError()));
}

void Reactor::start() {
    init_winsock();
    create_listen_socket();
    running_ = true;
    std::cout << "[Reactor] Listening on port " << port_ << "\n";

    while (running_) {
        // B2: drain worker results → B3: reassemble ordered responses
        drain_completion_queue();

        // B4: if worker queue below 70%, clear global backpressure
        size_t qsz = pool_.queue_size();
        size_t qmax = pool_.max_queue_size();
        if (qsz * 10 < qmax * 7) {
            paused_reads_.clear();
        } else {
            // B5: selectively unpause connections with room in their pipeline
            std::erase_if(paused_reads_, [this](SOCKET fd) {
                auto it = connections_.find(fd);
                return it != connections_.end() &&
                       it->second.in_flight_requests < MAX_PIPELINE_DEPTH;
            });
        }

        rebuild_poll_set();

        int result = WSAPoll(poll_fds_.data(),
                             static_cast<ULONG>(poll_fds_.size()), 100);
        if (result == SOCKET_ERROR) {
            std::cerr << "[Reactor] WSAPoll error: " << WSAGetLastError() << "\n";
            continue;
        }
        if (result == 0) continue;

        for (size_t i = 0; i < poll_fds_.size(); i++) {
            if (poll_fds_[i].revents == 0) continue;

            if (poll_fds_[i].fd == listen_socket_) {
                if (poll_fds_[i].revents & POLLRDNORM) accept_connection();
                continue;
            }

            SOCKET client = poll_fds_[i].fd;

            if (poll_fds_[i].revents & (POLLERR | POLLHUP)) {
                disconnect(client);
                continue;
            }

            if (poll_fds_[i].revents & POLLRDNORM) handle_read(client);
            if (poll_fds_[i].revents & POLLWRNORM) handle_write(client);
        }
    }

    for (auto& [sock, _] : connections_) closesocket(sock);
    connections_.clear();
}

void Reactor::stop() { running_ = false; }

// B5: reject connections beyond MAX_CONNECTIONS
void Reactor::accept_connection() {
    sockaddr_in client_addr{};
    int addr_len = sizeof(client_addr);
    SOCKET client = accept(listen_socket_,
                           reinterpret_cast<sockaddr*>(&client_addr), &addr_len);
    if (client == INVALID_SOCKET) return;

    if (total_connections_ >= MAX_CONNECTIONS) {
        const char* err = "-ERR max connections reached\r\n";
        send(client, err, static_cast<int>(strlen(err)), 0);
        closesocket(client);
        return;
    }

    u_long mode = 1;
    ioctlsocket(client, FIONBIO, &mode);

    char ip_buf[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &client_addr.sin_addr, ip_buf, sizeof(ip_buf));

    ConnectionState state;
    state.client_ip = ip_buf;
    connections_[client] = std::move(state);
    total_connections_++;
}

void Reactor::handle_read(SOCKET client) {
    char buf[4096];
    int bytes = recv(client, buf, sizeof(buf), 0);
    if (bytes <= 0) { disconnect(client); return; }

    auto conn_it = connections_.find(client);
    if (conn_it == connections_.end()) return;
    auto& state = conn_it->second;
    state.read_buffer.append(buf, bytes);

    size_t pos;
    while ((pos = state.read_buffer.find('\n')) != std::string::npos) {
        std::string line = state.read_buffer.substr(0, pos);
        state.read_buffer.erase(0, pos + 1);
        if (!line.empty() && line.back() == '\r') line.pop_back();
        if (line.empty()) continue;

        // B5: per-connection pipeline depth limit
        if (state.in_flight_requests >= MAX_PIPELINE_DEPTH) {
            paused_reads_.insert(client);
            return;
        }

        // B4: global backpressure — pause reads if queue > 90% full
        if (pool_.queue_size() * 10 > pool_.max_queue_size() * 9) {
            paused_reads_.insert(client);
            return;
        }

        uint64_t seq = state.next_seq_issue++;
        state.in_flight_requests++;
        pipeline_.process(client, line, seq, state.client_ip);
    }
}

void Reactor::handle_write(SOCKET client) {
    auto it = connections_.find(client);
    if (it == connections_.end()) return;
    auto& state = it->second;
    if (state.write_buffer.empty()) { state.write_pending = false; return; }

    int sent = send(client, state.write_buffer.data(),
                    static_cast<int>(state.write_buffer.size()), 0);
    if (sent == SOCKET_ERROR) { disconnect(client); return; }
    state.write_buffer.erase(0, sent);
    if (state.write_buffer.empty()) state.write_pending = false;
}

void Reactor::disconnect(SOCKET client) {
    closesocket(client);
    connections_.erase(client);
    paused_reads_.erase(client);
    if (total_connections_ > 0) total_connections_--;
}

// B2: drain completion queue; B3: reassemble ordered responses
void Reactor::drain_completion_queue() {
    std::vector<TaskResult> results;
    pool_.completion_queue().drain(results);

    for (auto& r : results) {
        auto it = connections_.find(r.client_fd);
        if (it == connections_.end()) continue; // connection dropped

        auto& state = it->second;
        state.out_of_order_buf[r.sequence_id] = std::move(r.response);

        // B3: flush contiguous sequence to the write buffer
        while (state.out_of_order_buf.count(state.next_seq_expected)) {
            state.write_buffer += state.out_of_order_buf[state.next_seq_expected];
            state.out_of_order_buf.erase(state.next_seq_expected);
            state.next_seq_expected++;
            if (state.in_flight_requests > 0) state.in_flight_requests--;
            state.write_pending = true;
        }
    }
}

// B4: paused sockets don't get POLLRDNORM
void Reactor::rebuild_poll_set() {
    poll_fds_.clear();

    WSAPOLLFD listen_pfd{};
    listen_pfd.fd = listen_socket_;
    listen_pfd.events = POLLRDNORM;
    poll_fds_.push_back(listen_pfd);

    for (auto& [sock, state] : connections_) {
        WSAPOLLFD pfd{};
        pfd.fd = sock;
        pfd.events = 0;
        if (paused_reads_.find(sock) == paused_reads_.end())
            pfd.events |= POLLRDNORM;
        if (state.write_pending)
            pfd.events |= POLLWRNORM;
        poll_fds_.push_back(pfd);
    }
}

} // namespace kvstore
