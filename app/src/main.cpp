#include "core/server_builder.h"
#include <iostream>
#include <thread>

using namespace std;

int main() {
    try {
        kvstore::ServerBuilder()
            .setPort(8080)
            .setWorkerThreads(thread::hardware_concurrency())
            .setMaxMemoryLimit("1GB")
            .setShardCount(64)
            .setAofPath("appendonly.aof")
            .build_and_run();
    } catch (const exception& e) {
        cerr << "[Fatal] " << e.what() << "\n";
        return 1;
    }
    return 0;
}
