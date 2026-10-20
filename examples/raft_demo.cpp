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
                std::cout << ", last_log_index=" << state.last_log_index << ", commit_index=" << state.commit_index;
                const auto& log = pair.second->persistent_state().log;
                if (!log.empty()) {
                    const auto& last = log.back();
                    std::cout << ", last_entry=(" << last.index << ", term " << last.term << ", ";
                    if (last.command.type == raft::CommandType::NoOp) { std::cout << "NoOp"; }
                    else if (last.command.type == raft::CommandType::Delete) { std::cout << "Delete " << last.command.key; }
                    else { std::cout << "Put " << last.command.key << '=' << last.command.value; }
                    std::cout << ')';
                }
                std::cout << '\n';
            }
        };
        for (raft::NodeId id = 1; id <= 3; ++id) { nodes[id] = open(id, raft::StorageOpenMode::CreateNew); }
        std::cout << "In-process Raft replication demo; logical time, temporary durable files, no Store or sockets.\n";
        show("1. All nodes start as followers.");
        nodes[1]->AdvanceTime(150ms); collect(1); deliver();
        show("2. Node 1 wins term 1; its NoOp is replicated and committed.");
        for (int round = 0; round < 3; ++round) {
            nodes[2]->AdvanceTime(50ms); nodes[3]->AdvanceTime(50ms);
            nodes[1]->AdvanceTime(50ms); collect(1); deliver();
        }
        nodes[1]->Propose({raft::CommandType::Put, "name", "Alice"}); collect(1);
        show("3. Put name=Alice is durable on node 1, awaiting majority confirmation.");
        deliver();
        show("4. Delivery replicates Put and advances commitment on all three nodes.");
        offline = 1;
        nodes[1]->Propose({raft::CommandType::Put, "name", "isolated"}); collect(1); deliver();
        show("5. Isolated node 1 appends another Put but cannot commit it.");
        nodes[2]->AdvanceTime(220ms); collect(2);
        nodes[3]->AdvanceTime(220ms); collect(3); deliver();
        nodes[2]->Propose({raft::CommandType::Delete, "name", {}}); collect(2); deliver();
        show("6. Nodes 2 and 3 elect node 2, commit its NoOp and then Delete name.");
        offline.reset();
        nodes[2]->AdvanceTime(50ms); collect(2); deliver();
        show("7. Rejoining node 1 discards its uncommitted conflict and catches up.");
        nodes[1].reset();
        nodes[1] = open(1, raft::StorageOpenMode::OpenExisting);
        show("8. Node 1 reopens its durable log; volatile commit_index starts at zero.");
        nodes[2]->AdvanceTime(50ms); collect(2); deliver();
        show("9. The matching heartbeat restores node 1's confirmed commit position.");
    } catch (const std::exception& error) {
        std::cerr << "Raft demo failed: " << error.what() << '\n';
        return 1;
    }
    return 0;
}
