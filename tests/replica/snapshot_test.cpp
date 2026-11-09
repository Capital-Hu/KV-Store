#include "kvstore/replicated_store.h"

#include <filesystem>
#include <iostream>
#include <map>
#include <stdexcept>
#include <unistd.h>

namespace kvstore {
struct ReplicatedStoreTestAccess {
    using Stage = ReplicatedStore::ApplyStage;
    static void FailAt(ReplicatedStore& replica, Stage stage) {
        replica.hook_ = [stage](Stage current, raft::LogIndex) {
            if (current == stage) { throw std::runtime_error("Injected crash"); }
        };
    }
};
}

namespace {
namespace raft = kvstore::raft;
namespace fs = std::filesystem;
using Replica = kvstore::ReplicatedStore;
using Access = kvstore::ReplicatedStoreTestAccess;
using namespace std::chrono_literals;
constexpr auto kNew = raft::StorageOpenMode::CreateNew;
constexpr auto kExisting = raft::StorageOpenMode::OpenExisting;

void Check(bool condition, const std::string& message) { if (!condition) { throw std::runtime_error(message); } }

class Directory {
public:
    Directory() {
        char pattern[] = "/tmp/kvstore-snapshot-XXXXXX";
        const auto created = ::mkdtemp(pattern); Check(created != nullptr, "Create snapshot directory"); path_ = created;
    }
    ~Directory() { std::error_code ignored; fs::remove_all(path_, ignored); }
    std::string Path(const std::string& name) const { return (path_ / name).string(); }
private:
    fs::path path_;
};

kvstore::ReplicatedStoreOptions Options(std::size_t threshold = 20, std::size_t retain = 5) {
    kvstore::ReplicatedStoreOptions options;
    options.election.random_seed = 7;
    options.snapshot_threshold_entries = threshold;
    options.snapshot_retain_entries = retain;
    return options;
}
raft::Command Put(const std::string& key, const std::string& value) { return {raft::CommandType::Put, key, value}; }

void TestSingleNodeTruncatesAndRestarts() {
    Directory directory; const auto path = directory.Path("node");
    const raft::NodeConfig config{1, {}};
    std::uintmax_t size_before_compaction = 0;
    {
        Replica replica(path, config, kNew, Options());
        replica.AdvanceTime(2s);
        for (int i = 0; i < 18; ++i) { replica.Propose(Put("k" + std::to_string(i), "v" + std::to_string(i))); }
        Check(replica.status().log_base_index == 0, "Below the threshold nothing is truncated");
        size_before_compaction = fs::file_size(path + "/raft");
        replica.Propose({raft::CommandType::Delete, "k3", ""});
        replica.Propose(Put("k4", "newest"));
        for (int i = 0; i < 100; ++i) { replica.ProposeReadBarrier(); }
        const auto status = replica.status();
        Check(status.log_base_index > 0 && status.last_log_index - status.log_base_index < 25, "Entries and read barriers both trigger truncation");
        Check(fs::file_size(path + "/raft") <= size_before_compaction, "The journal file shrinks with the log");
        Check(replica.LatestSnapshot()->info.index >= status.log_base_index, "A snapshot covers every dropped entry");
    }
    Replica reopened(path, config, kExisting, Options());
    const auto status = reopened.status();
    Check(status.log_base_index > 0 && status.last_applied == status.commit_index, "Restart resumes after the base");
    Check(reopened.LocalGet("k0") == "v0" && !reopened.LocalGet("k3") && reopened.LocalGet("k4") == "newest", "Truncated writes survive restart");
    reopened.AdvanceTime(2s);
    Check(reopened.status().role == raft::Role::Leader, "A node with a truncated log can win an election");
}

void TestLogNeverFillsUp() {
    Directory directory; const auto path = directory.Path("node");
    const raft::NodeConfig config{1, {}};
    const std::string large(1024 * 1024 - 8, 'x');
    {
        kvstore::ReplicatedStoreOptions options;
        options.election.random_seed = 7;
        Replica replica(path, config, kNew, options);
        replica.AdvanceTime(2s);
        for (int i = 0; i < 80; ++i) { replica.Propose(Put("big" + std::to_string(i % 4), large)); }
        Check(replica.status().log_base_index > 0, "The byte threshold truncates large values");
    }
    Replica reopened(path, config, kExisting);
    reopened.AdvanceTime(2s);
    Check(reopened.status().role == raft::Role::Leader && reopened.LocalGet("big3") == large, "More than the old 64 MiB limit stays writable across restart");
}

void TestDisabledSnapshotsAndBadOptions() {
    Directory directory; const raft::NodeConfig config{1, {}};
    {
        Replica replica(directory.Path("off"), config, kNew, Options(0, 0));
        replica.AdvanceTime(2s);
        for (int i = 0; i < 30; ++i) { replica.Propose(Put("k", "v")); }
        Check(replica.status().log_base_index == 0 && !replica.LatestSnapshot(), "Threshold zero disables snapshots");
    }
    bool rejected = false;
    try { Replica replica(directory.Path("bad"), config, kNew, Options(10, 10)); }
    catch (const std::invalid_argument&) { rejected = true; }
    Check(rejected && !fs::exists(directory.Path("bad")), "A retained tail must be smaller than the threshold");
}

// Three in-process replicas with hand-delivered messages; a transfer is a local file copy.
class Cluster {
public:
    Cluster() { for (raft::NodeId id = 1; id <= 3; ++id) { Open(id, kNew); } }
    void Open(raft::NodeId id, raft::StorageOpenMode mode) {
        raft::NodeConfig config{id, {}};
        for (raft::NodeId peer = 1; peer <= 3; ++peer) { if (peer != id) { config.peers.push_back(peer); } }
        auto options = Options();
        options.election.random_seed = id;
        nodes[id] = std::make_unique<Replica>(directory.Path("n" + std::to_string(id)), config, mode, options);
    }
    void Close(raft::NodeId id) { nodes[id].reset(); }
    // Returns true when a message asked a follower for a snapshot it does not have.
    void Pump(bool transfer = true) {
        for (bool moved = true; moved;) {
            moved = false;
            for (auto& [id, node] : nodes) {
                if (!node || !node->usable()) { continue; }
                for (const auto& message : node->DrainMessages()) {
                    const auto target = nodes.find(message.to);
                    if (target == nodes.end() || !target->second || down.count(message.to) || down.count(message.from)) { continue; }
                    moved = true;
                    if (transfer) {
                        if (const auto* request = std::get_if<raft::InstallSnapshotRequest>(&message.body)) { Copy(*node, *target->second, request->snapshot); }
                    }
                    target->second->Step(message);
                }
            }
        }
    }
    void Copy(Replica& from, Replica& to, const raft::SnapshotInfo& info) {
        if (!to.ShouldReceiveSnapshot(info)) { return; }
        const auto files = from.LatestSnapshot();
        Check(files && files->info.index == info.index, "Leader offers its newest snapshot");
        const auto& incoming = to.incoming_snapshot_directory();
        fs::remove_all(incoming); fs::create_directory(incoming);
        for (std::size_t i = 0; i < files->paths.size(); ++i) { fs::copy_file(files->paths[i], incoming + "/" + Replica::SnapshotFileName(i)); }
        to.PublishIncomingSnapshot(info, static_cast<std::uint32_t>(files->paths.size()));
    }
    raft::NodeId Elect(raft::NodeId id) {
        nodes[id]->AdvanceTime(2s); Pump();
        Check(nodes[id]->status().role == raft::Role::Leader, "Chosen node wins the election");
        return id;
    }
    void Heartbeat(raft::NodeId leader, bool transfer = true) { nodes[leader]->AdvanceTime(200ms); Pump(transfer); }
    Directory directory;
    std::map<raft::NodeId, std::unique_ptr<Replica>> nodes;
    std::map<raft::NodeId, bool> down;
};

void WriteMany(Cluster& cluster, raft::NodeId leader, int count, const std::string& tag) {
    for (int i = 0; i < count; ++i) {
        cluster.nodes[leader]->Propose(Put("k" + std::to_string(i % 40), tag + std::to_string(i)));
        cluster.Pump();
    }
}

void TestLaggingFollowerInstallsSnapshot() {
    Cluster cluster;
    const auto leader = cluster.Elect(1);
    WriteMany(cluster, leader, 5, "early");
    cluster.down[3] = true;
    WriteMany(cluster, leader, 120, "late");
    cluster.nodes[leader]->Propose({raft::CommandType::Delete, "k7", ""}); cluster.Pump();
    Check(cluster.nodes[leader]->status().log_base_index > 10, "Leader truncated past the lagging follower");
    Check(cluster.nodes[3]->status().last_applied < 10, "Follower really lags");

    cluster.down.erase(3);
    cluster.Heartbeat(leader, false);
    cluster.Heartbeat(leader, false);
    const auto waiting = cluster.nodes[3]->status();
    Check(waiting.log_base_index == 0 && waiting.role == raft::Role::Follower, "Without files the follower refuses and keeps its state");
    for (int i = 0; i < 3; ++i) { cluster.Heartbeat(leader); }
    const auto follower = cluster.nodes[3]->status(), head = cluster.nodes[leader]->status();
    Check(follower.log_base_index > 10 && follower.last_applied == head.last_applied, "Follower caught up through the snapshot");
    for (int i = 0; i < 40; ++i) {
        const auto key = "k" + std::to_string(i);
        Check(cluster.nodes[3]->LocalGet(key) == cluster.nodes[leader]->LocalGet(key), "Snapshot state matches the leader for " + key);
    }
    Check(!cluster.nodes[3]->LocalGet("k7"), "A snapshot preserves deletion");

    cluster.Close(3); cluster.Open(3, kExisting);
    Check(cluster.nodes[3]->status().last_applied == head.last_applied, "Installed snapshot survives restart");
    cluster.Pump();
    const auto second = cluster.Elect(3);
    WriteMany(cluster, second, 30, "second");
    cluster.Heartbeat(second);
    Check(cluster.nodes[1]->LocalGet("k1") == cluster.nodes[3]->LocalGet("k1"), "Snapshot-installed node can lead");
}

void TestInstallCrashRecovery() {
    using Stage = Access::Stage;
    for (const auto stage : {Stage::InstallMarked, Stage::InstallStoreReset, Stage::InstallRaftReset, Stage::InstallCommitSaved}) {
        Cluster cluster;
        const auto leader = cluster.Elect(1);
        WriteMany(cluster, leader, 5, "early");
        cluster.down[3] = true;
        WriteMany(cluster, leader, 120, "late");
        cluster.down.erase(3);
        Access::FailAt(*cluster.nodes[3], stage);
        bool crashed = false;
        try { cluster.Heartbeat(leader); cluster.Heartbeat(leader); }
        catch (const std::runtime_error& error) { crashed = std::string(error.what()) == "Injected crash"; }
        Check(crashed, "Install reaches the injected crash");
        cluster.Close(3); cluster.Open(3, kExisting);
        const auto status = cluster.nodes[3]->status();
        Check(status.log_base_index > 10 && status.last_applied == status.log_base_index, "Reopen finishes the interrupted install");
        Check(!fs::exists(cluster.directory.Path("n3/snapshot-install")), "The install marker is consumed");
        for (int i = 0; i < 3; ++i) { cluster.Heartbeat(leader); }
        Check(cluster.nodes[3]->status().last_applied == cluster.nodes[leader]->status().last_applied, "Recovered follower resumes replication");
        for (int i = 0; i < 40; ++i) {
            const auto key = "k" + std::to_string(i);
            Check(cluster.nodes[3]->LocalGet(key) == cluster.nodes[leader]->LocalGet(key), "Recovered state matches the leader");
        }
    }
}

raft::AppendEntriesRequest Append(raft::Term term, raft::LogIndex prev, raft::Term prev_term, std::vector<raft::LogEntry> entries, raft::LogIndex commit) {
    return {term, 2, prev, prev_term, std::move(entries), commit};
}
raft::LogEntry Entry(raft::LogIndex index, raft::Term term) { return {index, term, {raft::CommandType::Put, "k", std::to_string(index)}}; }

void TestProtocolEdges() {
    Directory directory; const auto path = directory.Path("raft");
    const raft::NodeConfig config{1, {2, 3}};
    auto node = std::make_unique<raft::Node>(config, std::make_unique<raft::Storage>(path, config, kNew));
    std::uint64_t rpc = 0;
    const auto step = [&](auto body) {
        node->Step({2, 1, ++rpc, std::move(body)});
        return node->DrainMessages();
    };
    const auto succeeded = [](const std::vector<raft::Message>& messages) {
        return messages.size() == 1 && std::get<raft::InstallSnapshotResponse>(messages[0].body).success;
    };
    step(Append(1, 0, 0, {Entry(1, 1), Entry(2, 1), Entry(3, 1)}, 0));
    Check(succeeded(step(raft::InstallSnapshotRequest{1, 2, {2, 1, 0}})) && !node->PendingSnapshotInstall(), "A matching prefix needs no state replacement");
    Check(node->status().commit_index == 2 && node->status().last_log_index == 3, "Acknowledged entries after a matching prefix are kept");
    Check(succeeded(step(raft::InstallSnapshotRequest{1, 2, {1, 1, 0}})), "An older snapshot is already satisfied");

    Check(step(raft::InstallSnapshotRequest{5, 2, {3, 5, 9}}).empty() && node->PendingSnapshotInstall(), "A conflicting log asks for a state replacement");
    node->FinishSnapshotInstall(false);
    Check(!succeeded(node->DrainMessages()) && node->status().last_log_index == 3, "Missing files leave the log alone");

    step(raft::InstallSnapshotRequest{5, 2, {10, 5, 9}});
    node->FinishSnapshotInstall(true);
    Check(succeeded(node->DrainMessages()), "Install acknowledges");
    auto status = node->status();
    Check(status.log_base_index == 10 && status.last_log_index == 10 && status.last_log_term == 5 && status.commit_index == 10 && status.last_applied == 10, "Install restarts the log after the snapshot");

    const auto ok = [](const std::vector<raft::Message>& messages) { return std::get<raft::AppendEntriesResponse>(messages.at(0).body).success; };
    Check(ok(step(Append(5, 8, 5, {Entry(9, 5), Entry(10, 5), Entry(11, 5)}, 11))), "Entries below the base are skipped");
    Check(node->status().last_log_index == 11 && node->status().commit_index == 11, "Only the new entry is appended");
    Check(ok(step(Append(5, 3, 1, {Entry(4, 5)}, 0))) && node->status().last_log_index == 11, "A request wholly below the base is harmless");
    Check(!ok(step(Append(5, 13, 5, {}, 0))), "A gap above the log is still rejected");

    node.reset();
    raft::Storage storage(path, config, kExisting);
    Check(storage.state().base.index == 10 && storage.state().base.fingerprint == 9 && storage.state().last_index() == 11 && storage.state().current_term == 5, "Base and suffix persist through reopen");
}
}  // namespace

int main() {
    try {
        TestSingleNodeTruncatesAndRestarts();
        TestLogNeverFillsUp();
        TestDisabledSnapshotsAndBadOptions();
        TestProtocolEdges();
        TestLaggingFollowerInstallsSnapshot();
        TestInstallCrashRecovery();
        std::cout << "All snapshot tests passed\n";
        return 0;
    } catch (const std::exception& error) { std::cerr << "Snapshot test failed: " << error.what() << '\n'; return 1; }
}
