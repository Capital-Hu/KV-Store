#include "kvstore/replicated_store.h"

#include <cstdlib>
#include <deque>
#include <filesystem>
#include <iostream>
#include <map>
#include <stdexcept>
#include <unistd.h>

namespace {
namespace raft = kvstore::raft;
using namespace std::chrono_literals;
class Directory {
public:
    Directory() {
        char pattern[] = "/tmp/kvstore-replica-demo-XXXXXX";
        const auto path = ::mkdtemp(pattern);
        if (!path) { throw std::runtime_error("Cannot create demo directory"); }
        path_ = path;
    }
    ~Directory() { std::error_code ignored; std::filesystem::remove_all(path_, ignored); }
    std::string Path(raft::NodeId id) const { return (path_ / std::to_string(id)).string(); }
private:
    std::filesystem::path path_;
};
}  // namespace

int main() {
    try {
        Directory directory;
        std::map<raft::NodeId, std::unique_ptr<kvstore::ReplicatedStore>> nodes;
        std::deque<raft::Message> messages;
        bool isolated = false;
        const auto open = [&](raft::NodeId id, raft::StorageOpenMode mode) {
            raft::NodeConfig config{id, {}};
            for (raft::NodeId peer = 1; peer <= 3; ++peer) { if (peer != id) { config.peers.push_back(peer); } }
            kvstore::ReplicatedStoreOptions options;
            const auto timeout = std::chrono::milliseconds{150 + static_cast<int>(id - 1) * 70};
            options.election = {50ms, timeout, timeout, id};
            return std::make_unique<kvstore::ReplicatedStore>(directory.Path(id), config, mode, options);
        };
        const auto collect = [&](raft::NodeId id) {
            for (auto& message : nodes.at(id)->DrainMessages()) { messages.push_back(std::move(message)); }
        };
        const auto deliver = [&] {
            std::size_t count = 0;
            while (!messages.empty()) {
                if (++count > 5000) { throw std::runtime_error("Demo message delivery did not settle"); }
                auto message = std::move(messages.front()); messages.pop_front();
                if (isolated && (message.from == 1 || message.to == 1)) { continue; }
                nodes.at(message.to)->Step(message); collect(message.to);
            }
        };
        const auto show = [&](const char* title) {
            std::cout << title << '\n';
            for (const auto& pair : nodes) {
                const auto status = pair.second->status(); const auto value = pair.second->LocalGet("name");
                std::cout << "  Node " << status.id << ": term=" << status.term << ", log=" << status.last_log_index
                          << ", commit=" << status.commit_index << ", applied=" << status.last_applied
                          << ", LocalGet(name)=" << (value ? *value : "<not found>")
                          << ", SSTables=" << pair.second->sstable_count() << '\n';
            }
        };
        for (raft::NodeId id = 1; id <= 3; ++id) { nodes[id] = open(id, raft::StorageOpenMode::CreateNew); }
        std::cout << "In-process Raft + Store demo; temporary files, local reads, no sockets.\n";
        show("1. Each node owns separate Raft and Store files.");
        nodes[1]->AdvanceTime(150ms); collect(1); deliver();
        show("2. Node 1 is elected; committed NoOp advances application without writing a key.");
        nodes[1]->Propose({raft::CommandType::Put, "name", "Alice"}); collect(1);
        show("3. Put is in node 1's Raft log; Store is unchanged before confirmation.");
        deliver();
        show("4. Majority confirmation makes all three Stores return Alice.");
        for (const auto id : {1, 2, 3}) { nodes[id]->Flush(); }
        show("5. Local Flush moves the applied data into SSTables.");
        isolated = true;
        nodes[1]->Propose({raft::CommandType::Put, "name", "uncommitted"}); collect(1); deliver();
        nodes[2]->AdvanceTime(220ms); collect(2); deliver();
        nodes[2]->Propose({raft::CommandType::Put, "name", "Bob"}); collect(2); deliver();
        show("6. New leader commits Bob; isolated node 1 still reads Alice locally.");
        isolated = false; nodes[2]->AdvanceTime(50ms); collect(2); deliver();
        show("7. Node 1 repairs its uncommitted suffix and applies Bob.");
        messages.clear(); nodes.clear();
        for (raft::NodeId id = 1; id <= 3; ++id) { nodes[id] = open(id, raft::StorageOpenMode::OpenExisting); }
        show("8. All nodes reopen and replay saved committed prefixes before any network delivery.");
        nodes[1]->AdvanceTime(150ms); collect(1); deliver();
        nodes[1]->Propose({raft::CommandType::Delete, "name", {}}); collect(1); deliver();
        show("9. A newly elected leader commits Delete; all Stores now report not found.");
    } catch (const std::exception& error) {
        std::cerr << "Replica demo failed: " << error.what() << '\n'; return 1;
    }
}
