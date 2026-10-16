#include "kvstore/raft/node.h"
#include "kvstore/raft/storage.h"

#include <chrono>
#include <cstdlib>
#include <deque>
#include <filesystem>
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>

#include <unistd.h>

namespace {
namespace raft = kvstore::raft;
using namespace std::chrono_literals;

class DemoDirectory {
public:
    DemoDirectory() {
        char pattern[] = "/tmp/kvstore-raft-demo-XXXXXX";
        const auto path = ::mkdtemp(pattern);
        if (!path) { throw std::runtime_error("Cannot create demo directory"); }
        path_ = path;
    }
    ~DemoDirectory() { std::error_code ignored; std::filesystem::remove_all(path_, ignored); }
    std::string Path(raft::NodeId id) const { return (path_ / (std::to_string(id) + ".raft")).string(); }
private:
    std::filesystem::path path_;
};

const char* Name(raft::Role role) {
    switch (role) {
        case raft::Role::Follower: return "Follower";
        case raft::Role::Candidate: return "Candidate";
        case raft::Role::Leader: return "Leader";
    }
    return "Unknown";
}
}  // namespace

int main() {
    try {
        DemoDirectory directory;
        std::map<raft::NodeId, std::unique_ptr<raft::Node>> nodes;
        std::deque<raft::Message> messages;
        std::optional<raft::NodeId> offline;
        const auto open = [&](raft::NodeId id, raft::StorageOpenMode mode) {
            raft::NodeConfig config{id, {}};
            for (raft::NodeId peer = 1; peer <= 3; ++peer) { if (peer != id) { config.peers.push_back(peer); } }
            // Fixed demonstration timeouts make each displayed transition predictable.
            const auto timeout = std::chrono::milliseconds{150 + static_cast<std::chrono::milliseconds::rep>(id - 1) * 70};
            return std::make_unique<raft::Node>(config, std::make_unique<raft::Storage>(directory.Path(id), config, mode),
                                                raft::ElectionOptions{50ms, timeout, timeout, id});
        };
        const auto collect = [&](raft::NodeId id) {
            for (auto& message : nodes.at(id)->DrainMessages()) { messages.push_back(std::move(message)); }
        };
        const auto deliver = [&] {
            std::size_t count = 0;
            while (!messages.empty()) {
                if (++count > 1000) { throw std::runtime_error("Demo message loop did not reach idle"); }
                auto message = std::move(messages.front()); messages.pop_front();
                if (offline == message.from || offline == message.to) { continue; }
                nodes.at(message.to)->Step(message); collect(message.to);
            }
        };
        const auto show = [&](const char* title) {
            std::cout << title << '\n';
            for (const auto& pair : nodes) {
                const auto state = pair.second->status();
                std::cout << "  Node " << state.id << ": " << (offline == state.id ? "OFFLINE, last local state: " : "")
                          << Name(state.role) << ", term=" << state.term << ", leader=";
                if (state.leader_id) { std::cout << *state.leader_id; } else { std::cout << "unknown"; }
                std::cout << '\n';
            }
        };
        for (raft::NodeId id = 1; id <= 3; ++id) { nodes[id] = open(id, raft::StorageOpenMode::CreateNew); }
        std::cout << "In-process Raft election demo; logical time, temporary durable files, no sockets.\n";
        show("1. All nodes start as followers.");
        nodes[1]->AdvanceTime(150ms); collect(1); deliver();
        show("2. Node 1 times out, wins votes and sends heartbeats.");
        for (int round = 0; round < 3; ++round) {
            nodes[2]->AdvanceTime(50ms); nodes[3]->AdvanceTime(50ms);
            nodes[1]->AdvanceTime(50ms); collect(1); deliver();
        }
        show("3. Heartbeats keep the same leader and term.");
        offline = 1;
        nodes[2]->AdvanceTime(220ms); collect(2);
        nodes[3]->AdvanceTime(220ms); collect(3); deliver();
        show("4. Node 1 is isolated; nodes 2 and 3 elect node 2 in term 2.");
        nodes[1].reset();
        nodes[1] = open(1, raft::StorageOpenMode::OpenExisting);
        offline.reset();
        show("5. Node 1 reopens its durable state as a follower in term 1.");
        nodes[2]->AdvanceTime(50ms); collect(2); deliver();
        show("6. The higher-term heartbeat brings node 1 into term 2.");
    } catch (const std::exception& error) {
        std::cerr << "Raft demo failed: " << error.what() << '\n';
        return 1;
    }
    return 0;
}
