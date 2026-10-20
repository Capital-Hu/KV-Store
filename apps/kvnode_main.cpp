#include "kvstore/rpc/node_server.h"

#include <atomic>
#include <csignal>
#include <iostream>
#include <set>
#include <stdexcept>
#include <thread>

namespace {
static_assert(std::atomic<bool>::is_always_lock_free, "Signal flag must not acquire a hidden lock");
std::atomic<bool> stop_requested{false};
extern "C" void StopSignal(int) { stop_requested.store(true, std::memory_order_relaxed); }
constexpr auto kUsage = "kvnode --id ID --directory PATH --mode new|existing --members MEMBERS [--raft-routes MEMBERS]";
}

int main(int argc, char** argv) {
    try {
        if (argc == 2 && std::string(argv[1]) == "--help") { std::cout << kUsage << '\n'; return 0; }
        kvstore::rpc::ServerOptions options; std::set<std::string> seen;
        for (int i = 1; i < argc; i += 2) {
            const std::string argument = argv[i];
            if (i + 1 == argc || !seen.insert(argument).second) { throw std::invalid_argument(kUsage); }
            const std::string value = argv[i + 1];
            if (argument == "--id") { options.id = kvstore::rpc::ParseUnsigned(value); }
            else if (argument == "--directory") { options.directory = value; }
            else if (argument == "--members") { options.members = kvstore::rpc::ParseMembers(value); }
            else if (argument == "--raft-routes") { options.raft_routes = kvstore::rpc::ParseMembers(value); }
            else if (argument == "--mode") {
                if (value == "new") { options.mode = kvstore::raft::StorageOpenMode::CreateNew; }
                else if (value == "existing") { options.mode = kvstore::raft::StorageOpenMode::OpenExisting; }
                else { throw std::invalid_argument(kUsage); }
            } else { throw std::invalid_argument(kUsage); }
        }
        for (const auto* required : {"--id", "--directory", "--mode", "--members"}) {
            if (seen.count(required) == 0) { throw std::invalid_argument(kUsage); }
        }
        std::signal(SIGINT, StopSignal); std::signal(SIGTERM, StopSignal);
        kvstore::rpc::NodeServer node(options);
        std::cout << "Node " << options.id << " started. Ctrl+C stops it; reopen with --mode existing.\n" << std::flush;
        while (!stop_requested.load(std::memory_order_relaxed) && !node.failed()) { std::this_thread::sleep_for(std::chrono::milliseconds{25}); }
        node.Stop(); return node.failed() ? 1 : 0;
    } catch (const std::invalid_argument& error) {
        std::cerr << error.what() << '\n'; return 2;
    } catch (const std::exception& error) {
        std::cerr << "Node startup failed: " << error.what() << '\n'; return 1;
    }
}
