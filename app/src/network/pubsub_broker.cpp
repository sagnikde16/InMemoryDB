#include "network/pubsub_broker.h"

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <winsock2.h>

using namespace std;

namespace kvstore {

void PubSubBroker::subscribe(const string& channel, SOCKET client) {
    unique_lock lock(mutex_);
    channels_[channel].insert(client);
}

void PubSubBroker::unsubscribe(const string& channel, SOCKET client) {
    unique_lock lock(mutex_);
    auto it = channels_.find(channel);
    if (it != channels_.end()) {
        it->second.erase(client);
        if (it->second.empty()) {
            channels_.erase(it);
        }
    }
}

void PubSubBroker::unsubscribe_all(SOCKET client) {
    unique_lock lock(mutex_);
    for (auto it = channels_.begin(); it != channels_.end(); ) {
        it->second.erase(client);
        if (it->second.empty()) {
            it = channels_.erase(it);
        } else {
            ++it;
        }
    }
}

size_t PubSubBroker::publish(const string& channel, const string& message) {
    shared_lock lock(mutex_);
    auto it = channels_.find(channel);
    if (it == channels_.end()) return 0;

    string formatted = format_message(channel, message);
    size_t delivered = 0;

    for (SOCKET sock : it->second) {
        int result = send(sock, formatted.data(),
                          static_cast<int>(formatted.size()), 0);
        if (result != SOCKET_ERROR) {
            delivered++;
        }
    }
    return delivered;
}

vector<string> PubSubBroker::list_channels() const {
    shared_lock lock(mutex_);
    vector<string> result;
    result.reserve(channels_.size());
    for (const auto& [name, _] : channels_) {
        result.push_back(name);
    }
    return result;
}

size_t PubSubBroker::subscriber_count(const string& channel) const {
    shared_lock lock(mutex_);
    auto it = channels_.find(channel);
    return (it != channels_.end()) ? it->second.size() : 0;
}

string PubSubBroker::format_message(const string& channel,
                                         const string& message) {
    // Simple push format: *3\r\n$7\r\nmessage\r\n$<channel_len>\r\n<channel>\r\n$<msg_len>\r\n<msg>\r\n
    return "*3\r\n$7\r\nmessage\r\n$" +
           to_string(channel.size()) + "\r\n" + channel + "\r\n$" +
           to_string(message.size()) + "\r\n" + message + "\r\n";
}

} // namespace kvstore
