#include "kvstore/replicated_store.h"
#include "raft/storage_codec.h"
#include "replica/replica_checkpoint.h"
#include "common/coding.h"

#include <algorithm>
#include <cstdlib>
#include <deque>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <stdexcept>
#include <unistd.h>

namespace {
namespace raft = kvstore::raft;
namespace detail = kvstore::detail;
using Replica = kvstore::ReplicatedStore;
using namespace std::chrono_literals;
constexpr auto kNew = raft::StorageOpenMode::CreateNew;
constexpr auto kExisting = raft::StorageOpenMode::OpenExisting;
void Check(bool condition, const std::string& message) { if (!condition) { throw std::runtime_error(message); } }
template<class Action> void Reject(Action action, const std::string& message) {
    try { action(); } catch (const std::exception&) { return; }
    throw std::runtime_error(message);
}
class Directory {
public:
    Directory() {
        char pattern[] = "/tmp/kvstore-odd-members-XXXXXX";
        const auto created = ::mkdtemp(pattern); Check(created != nullptr, "Create odd-member directory"); path_ = created;
    }
    ~Directory() { std::error_code ignored; std::filesystem::remove_all(path_, ignored); }
    std::string Path(const std::string& name) const { return (path_ / name).string(); }
private:
    std::filesystem::path path_;
};
raft::NodeConfig Config(std::size_t count, std::size_t node = 0) {
    raft::NodeConfig result{(node + 1) * 10, {}};
    for (std::size_t i = 0; i < count; ++i) { if (i != node) { result.peers.push_back((i + 1) * 10); } }
    return result;
}
std::string Read(const std::string& path) {
    std::ifstream input(path, std::ios::binary);
    Check(input.is_open(), "Read membership fixture");
    return {std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
}
void Write(const std::string& path, const std::string& bytes) {
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    output.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    Check(static_cast<bool>(output), "Write membership fixture");
}

class Cluster {
public:
    explicit Cluster(std::size_t count) : nodes(count), available(count, true) {
        for (std::size_t i = 0; i < count; ++i) { Open(i, kNew); }
    }
    void Open(std::size_t node, raft::StorageOpenMode mode) {
        auto config = Config(nodes.size(), node);
        std::reverse(config.peers.begin(), config.peers.end());
        kvstore::ReplicatedStoreOptions options;
        options.election = {20ms, 100ms, 100ms, config.id}; options.flush_threshold_bytes = 0;
        nodes[node] = std::make_unique<Replica>(directory.Path(std::to_string(node)), config, mode, options);
    }
    void Pump() {
        for (auto& node : nodes) {
            if (!node) { continue; }
            for (auto& message : node->DrainMessages()) { messages.push_back(std::move(message)); }
        }
        std::size_t delivered = 0;
        while (!messages.empty()) {
            Check(++delivered < 10000, "Message delivery terminates");
            auto message = std::move(messages.front()); messages.pop_front();
            const auto from = static_cast<std::size_t>(message.from / 10 - 1);
            const auto to = static_cast<std::size_t>(message.to / 10 - 1);
            if (!available[from] || !available[to] || !nodes[to]) { continue; }
            nodes[to]->Step(message);
            for (auto& reply : nodes[to]->DrainMessages()) { messages.push_back(std::move(reply)); }
        }
    }
    void Elect(std::size_t node) {
        nodes[node]->AdvanceTime(100ms); Pump();
        Check(nodes[node]->status().role == raft::Role::Leader, "Available majority elects candidate");
    }
    void Heartbeat(std::size_t leader) { nodes[leader]->AdvanceTime(20ms); Pump(); }
    Directory directory;
    std::vector<std::unique_ptr<Replica>> nodes;
    std::vector<bool> available;
    std::deque<raft::Message> messages;
};

void TestSingleNode() {
    Cluster cluster(1); cluster.Elect(0);
    Check(cluster.nodes[0]->status().last_applied == 1, "Singleton commits leader NoOp without a reply");
    const auto put = cluster.nodes[0]->Propose({raft::CommandType::Put, "name", "Alice"});
    Check(cluster.nodes[0]->IsApplied(put) && cluster.nodes[0]->LocalGet("name") == "Alice", "Singleton writes apply immediately");
    const auto barrier = cluster.nodes[0]->ProposeReadBarrier();
    Check(cluster.nodes[0]->IsApplied(barrier), "Singleton fresh read barrier applies");
    cluster.nodes[0].reset(); cluster.Open(0, kExisting);
    Check(cluster.nodes[0]->LocalGet("name") == "Alice", "Singleton committed state survives reopen");
    cluster.Elect(0);
    const auto deletion = cluster.nodes[0]->Propose({raft::CommandType::Delete, "name", {}});
    Check(cluster.nodes[0]->IsApplied(deletion) && !cluster.nodes[0]->LocalGet("name"), "Singleton delete applies");
}

void TestQuorumAndRecovery(std::size_t count) {
    Cluster cluster(count); cluster.Elect(0);
    auto& leader = *cluster.nodes[0];
    const auto seed = leader.Propose({raft::CommandType::Put, "name", "Alice"}); cluster.Pump();
    for (auto& node : cluster.nodes) { Check(node->IsApplied(seed), "Seed applied on every member"); }
    const auto quorum = count / 2 + 1;
    for (std::size_t i = quorum - 1; i < count; ++i) { cluster.available[i] = false; }
    const auto pending = leader.Propose({raft::CommandType::Put, "name", "Bob"}); cluster.Pump();
    Check(!leader.IsApplied(pending) && leader.LocalGet("name") == "Alice", "Below quorum cannot commit a new write");
    cluster.available[quorum - 1] = true; cluster.Heartbeat(0);
    Check(leader.IsApplied(pending) && leader.LocalGet("name") == "Bob", "Exactly quorum can commit");
    const auto previous_term = leader.status().term;
    cluster.available[0] = false;
    cluster.available[quorum] = true;
    cluster.Elect(1);
    Check(cluster.nodes[1]->status().term > previous_term, "New leader advances term");
    const auto replacement = cluster.nodes[1]->Propose({raft::CommandType::Put, "name", "Carol"}); cluster.Pump();
    Check(cluster.nodes[1]->IsApplied(replacement), "Majority remains writable after leader loss");
    std::fill(cluster.available.begin(), cluster.available.end(), true); cluster.Heartbeat(1);
    for (auto& node : cluster.nodes) {
        Check(node->IsApplied(replacement) && node->LocalGet("name") == "Carol", "Healed members catch up");
        Check(node->status().commit_index == node->status().last_applied, "All local applications caught up");
    }
    for (auto& node : cluster.nodes) { node.reset(); }
    for (std::size_t i = 0; i < count; ++i) {
        cluster.Open(i, kExisting);
        Check(cluster.nodes[i]->LocalGet("name") == "Carol", "All committed values survive full restart");
    }
}

void TestFormatsAndCorruption() {
    for (const auto count : {1u, 3u, 5u, 7u}) {
        auto config = Config(count);
        const auto header = raft::detail::EncodeFileHeader(config);
        const auto checkpoint = detail::EncodeReplicaCheckpoint(config, {3, 2, 42});
        Check(header.size() == raft::detail::FileHeaderBytes(config), "Raft metadata length matches configuration");
        Check(checkpoint.size() == detail::ReplicaCheckpointBytes(config), "COMMIT metadata length matches configuration");
        Check(header.substr(0, 4) == (count == 3 ? "KVR1" : "KVR2"), "Three-node legacy Raft format retained");
        Check(checkpoint.substr(0, 4) == (count == 3 ? "KVC1" : "KVC2"), "Three-node legacy COMMIT format retained");
        std::reverse(config.peers.begin(), config.peers.end());
        raft::detail::ValidateFileHeader(header, config);
        Check(detail::DecodeReplicaCheckpoint(checkpoint, config).index == 3, "Member ordering does not change identity");
        for (std::size_t offset = 0; offset < header.size(); ++offset) {
            auto corrupt = header; corrupt[offset] ^= 1;
            Reject([&] { raft::detail::ValidateFileHeader(corrupt, config); }, "Every modified header byte rejected");
        }
        for (std::size_t offset = 0; offset < checkpoint.size(); ++offset) {
            auto corrupt = checkpoint; corrupt[offset] ^= 1;
            Reject([&] { detail::DecodeReplicaCheckpoint(corrupt, config); }, "Every modified checkpoint byte rejected");
            Reject([&] { detail::DecodeReplicaCheckpoint(checkpoint.substr(0, offset), config); }, "Every truncated checkpoint rejected");
        }
        if (!config.peers.empty()) {
            config.peers.front() += 1;
            Reject([&] { raft::detail::ValidateFileHeader(header, config); }, "Same count with different member identity rejected");
            Reject([&] { detail::DecodeReplicaCheckpoint(checkpoint, config); }, "COMMIT rejects different member identity");
        }
    }
    const auto legacy_config = Config(3);
    std::string header("KVR1");
    for (auto value : {10ULL, 20ULL, 30ULL}) { detail::AppendU64(header, value); }
    detail::AppendU32(header, detail::Crc32(header));
    Check(header == raft::detail::EncodeFileHeader(legacy_config) && header.size() == 32, "Legacy Raft bytes are unchanged");
    std::string checkpoint("KVC1");
    for (auto value : {10ULL, 20ULL, 30ULL, 3ULL, 2ULL}) { detail::AppendU64(checkpoint, value); }
    detail::AppendU32(checkpoint, 42); detail::AppendU32(checkpoint, detail::Crc32(checkpoint));
    Check(checkpoint == detail::EncodeReplicaCheckpoint(legacy_config, {3, 2, 42}) && checkpoint.size() == 52,
          "Legacy COMMIT bytes are unchanged");
}

void TestJournalBoundariesAndMembershipChangeRejection() {
    for (const auto count : {1u, 5u}) {
        Directory directory; const auto path = directory.Path("raft"); const auto config = Config(count);
        { raft::Storage storage(path, config, kNew); storage.SetTermAndVote(1, config.id); }
        const auto original = Read(path);
        for (std::size_t size = 0; size < raft::detail::FileHeaderBytes(config); ++size) {
            Write(path, original.substr(0, size));
            Reject([&] { raft::Storage storage(path, config, kExisting, raft::StorageRecoveryMode::TruncateIncompleteTail); }, "Incomplete identity is never repaired as a journal tail");
            Check(Read(path) == original.substr(0, size), "Rejected header does not mutate disk");
        }
        Write(path, original + "R");
        { raft::Storage storage(path, config, kExisting, raft::StorageRecoveryMode::TruncateIncompleteTail);
          Check(storage.truncated_tail_bytes() == 1 && storage.state().voted_for == config.id, "Tail repair uses variable header boundary"); }
        Check(Read(path) == original, "Repair preserves complete journal prefix");
        Reject([&] { raft::Storage storage(path, Config(count == 1 ? 5 : 3), kExisting); }, "Existing cluster cannot change membership count");
        Check(Read(path) == original, "Wrong membership leaves old journal untouched");
    }
    for (const auto count : {0u, 2u, 4u, 6u}) { Check(!raft::ValidMemberCount(count), "Even or empty membership rejected"); }
    for (const auto& config : std::vector<raft::NodeConfig>{{1,{2}}, {1,{2,3,4}}, {1,{2,3,4,4}}, {1,{0,2,3,4}}}) {
        Reject([&] { raft::Node node(config); }, "Invalid member count or identity rejected");
    }
}
}

int main(int argc, char** argv) {
    try {
        if (argc == 2) {
            Replica replica(argv[1], {1, {2, 3}}, kExisting);
            for (std::size_t i = 0; i < 8; ++i) {
                const auto digits = std::to_string(i);
                Check(replica.LocalGet("key-" + std::string(12 - digits.size(), '0') + digits) == std::string(16, 'v'),
                      "Pre-upgrade three-node data reopens with every original value");
            }
            std::cout << "Pre-upgrade three-node replica compatibility passed\n"; return 0;
        }
        TestSingleNode();
        for (const auto count : {3u, 5u, 7u}) { TestQuorumAndRecovery(count); }
        TestFormatsAndCorruption(); TestJournalBoundariesAndMembershipChangeRejection();
        std::cout << "Odd membership: singleton, 3/5/7 quorums, failover, formats and recovery passed\n";
        return 0;
    } catch (const std::exception& error) { std::cerr << "Odd membership failed: " << error.what() << '\n'; return 1; }
}
