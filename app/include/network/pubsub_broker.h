#pragma once

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <winsock2.h>

#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

using namespace std;

namespace kvstore {

// Observer-pattern Pub/Sub broker. Channels map to sets of subscriber sockets.
// Operates entirely independently of the shard engine — no shared locks.
class PubSubBroker {
public:
    void subscribe(const string& channel, SOCKET client);
    void unsubscribe(const string& channel, SOCKET client);
    void unsubscribe_all(SOCKET client);

    // Returns the number of subscribers that received the message
    size_t publish(const string& channel, const string& message);

    vector<string> list_channels() const;
    size_t subscriber_count(const string& channel) const;

private:
    // Format a pub/sub push message for the wire
    static string format_message(const string& channel,
                                      const string& message);

    mutable shared_mutex mutex_;
    unordered_map<string, unordered_set<SOCKET>> channels_;
};

} // namespace kvstore
