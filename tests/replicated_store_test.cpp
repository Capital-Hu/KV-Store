#include "kvstore/replicated_store.h"
#include "../src/replica_checkpoint.h"

#include <algorithm>
#include <cerrno>
#include <csignal>
#include <cstdlib>
#include <deque>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <map>
#include <random>
#include <set>
#include <stdexcept>
#include <sys/resource.h>
#include <sys/wait.h>
#include <unistd.h>

namespace kvstore {
struct ReplicatedStoreTestAccess {
    using Stage = ReplicatedStore::ApplyStage;
    static void Hook(ReplicatedStore& replica, std::function<void(Stage, raft::LogIndex)> hook) {
        replica.hook_ = std::move(hook);
    }
    static Store& Engine(ReplicatedStore& replica) { return *replica.store_; }
    static const std::vector<raft::LogEntry>& Log(const ReplicatedStore& replica) { return replica.node_->persistent_state().log; }
};
struct StoreTestAccess {
    using Stage = Store::FreezeStage;
    static void Hook(Store& store, std::function<void(Stage)> hook) {
        std::lock_guard<std::mutex> lock(store.mutex_); store.freeze_hook_ = std::move(hook);
    }
};
}  // namespace kvstore

namespace {
namespace raft = kvstore::raft;
using Replica = kvstore::ReplicatedStore;
using Access = kvstore::ReplicatedStoreTestAccess;
using Stage = Access::Stage;
using namespace std::chrono_literals;
constexpr auto kNew = raft::StorageOpenMode::CreateNew;
constexpr auto kExisting = raft::StorageOpenMode::OpenExisting;

void Check(bool condition, const std::string& message) { if (!condition) { throw std::runtime_error(message); } }
template <typename Action>
void Reject(Action action, const std::string& message) {
    try { action(); } catch (const std::exception&) { return; }
    throw std::runtime_error(message);
}
class Directory {
public:
    Directory() {
        char pattern[] = "/tmp/kvstore-replica-test-XXXXXX";
        const auto path = ::mkdtemp(pattern); Check(path != nullptr, "Create test directory"); path_ = path;
    }
    ~Directory() { std::error_code ignored; std::filesystem::remove_all(path_, ignored); }
    std::string Path(raft::NodeId id = 1) const { return (path_ / std::to_string(id)).string(); }
private:
    std::filesystem::path path_;
};
raft::NodeConfig Config(raft::NodeId id = 1) {
    raft::NodeConfig result{id, {}};
    for (raft::NodeId peer = 1; peer <= 3; ++peer) { if (peer != id) { result.peers.push_back(peer); } }
    return result;
}
kvstore::ReplicatedStoreOptions Options(raft::NodeId id = 1, std::size_t flush = 0) {
    kvstore::ReplicatedStoreOptions result;
    const auto timeout = std::chrono::milliseconds{100 + static_cast<int>(id - 1) * 50};
    result.election = {20ms, timeout, timeout, id}; result.flush_threshold_bytes = flush;
    result.compaction_threshold_tables = 3;
    return result;
}
std::unique_ptr<Replica> Open(const std::string& path, raft::NodeId id = 1, raft::StorageOpenMode mode = kNew,
                            std::size_t flush = 0) {
    return std::make_unique<Replica>(path, Config(id), mode, Options(id, flush));
}
raft::LogEntry Put(raft::LogIndex index, std::string key, std::string value, raft::Term term = 1) {
    return {index, term, {raft::CommandType::Put, std::move(key), std::move(value)}};
}
raft::LogEntry Delete(raft::LogIndex index, std::string key, raft::Term term = 1) {
    return {index, term, {raft::CommandType::Delete, std::move(key), {}}};
}
raft::Message Append(std::vector<raft::LogEntry> entries, raft::LogIndex commit,
                     raft::LogIndex previous = 0, raft::Term previous_term = 0, raft::Term term = 1) {
    return {2, 1, 99, raft::AppendEntriesRequest{term, 2, previous, previous_term, std::move(entries), commit}};
}
void Accept(Replica& replica, const raft::Message& request) {
    replica.Step(request); const auto responses = replica.DrainMessages();
    Check(responses.size() == 1 && std::get<raft::AppendEntriesResponse>(responses[0].body).success, "Follower accepts valid append");
}
std::string Read(const std::string& path) {
    std::ifstream input(path, std::ios::binary); Check(input.is_open(), "Read fixture");
    return {std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
}
void Write(const std::string& path, const std::string& bytes) {
    std::ofstream output(path, std::ios::binary | std::ios::trunc); output.write(bytes.data(), bytes.size());
    Check(static_cast<bool>(output), "Write fixture");
}
void Reply(Replica& replica, const raft::Message& request) {
    replica.Step({request.to, request.from, request.rpc_id,
        raft::AppendEntriesResponse{std::get<raft::AppendEntriesRequest>(request.body).term, true}});
}
void Elect(Replica& replica) {
    replica.AdvanceTime(100ms); const auto votes = replica.DrainMessages();
    replica.Step({2, 1, votes.at(0).rpc_id, raft::RequestVoteResponse{1, true}});
    const auto initial = replica.DrainMessages();
    Reply(replica, initial.at(0));
    // Finish the commit notification to peer 2, leaving it ready for the next proposal.
    const auto notifications = replica.DrainMessages();
    for (const auto& message : notifications) { if (message.to == 2) { Reply(replica, message); } }
    replica.DrainMessages();
    Check(replica.status().last_applied == 1, "Leader applies its committed no-op");
}
raft::Message PendingPut(Replica& replica) {
    Elect(replica); replica.Propose({raft::CommandType::Put, "k", "value"});
    const auto messages = replica.DrainMessages();
    const auto found = std::find_if(messages.begin(), messages.end(), [](const auto& message) { return message.to == 2; });
    Check(found != messages.end(), "Proposal sends to available peer"); return *found;
}

class Cluster {
public:
    explicit Cluster(std::size_t flush = 0) : flush_(flush) {
        for (raft::NodeId id = 1; id <= 3; ++id) { nodes[id] = Open(directory.Path(id), id, kNew, flush_); }
    }
    void Collect(raft::NodeId id) {
        for (auto& message : nodes.at(id)->DrainMessages()) { messages.push_back(std::move(message)); }
    }
    void Advance(raft::NodeId id, std::chrono::milliseconds delta) { nodes.at(id)->AdvanceTime(delta); Collect(id); }
    void Deliver(std::size_t index = 0) {
        auto at = messages.begin() + static_cast<std::ptrdiff_t>(index);
        auto message = std::move(*at); messages.erase(at);
        if (isolated.count(message.from) || isolated.count(message.to)) { return; }
        nodes.at(message.to)->Step(message); Collect(message.to);
    }
    void DeliverAll() {
        std::size_t count = 0;
        while (!messages.empty()) { Check(++count < 5000, "Cluster reaches idle"); Deliver(); }
    }
    void Start() { Advance(1, 100ms); DeliverAll(); }
    raft::Proposal Propose(raft::NodeId id, raft::Command command) {
        const auto proposal = nodes.at(id)->Propose(std::move(command)); Collect(id); return proposal;
    }
    raft::Proposal PutValue(raft::NodeId id, std::string key, std::string value) {
        return Propose(id, {raft::CommandType::Put, std::move(key), std::move(value)});
    }
    void Restart(raft::NodeId id) {
        const auto applied = nodes.at(id)->status().last_applied;
        nodes.at(id).reset(); nodes.at(id) = Open(directory.Path(id), id, kExisting, flush_);
        Check(nodes.at(id)->status().last_applied >= applied, "Restart retains all previously completed application");
    }
    Directory directory;
    std::map<raft::NodeId, std::unique_ptr<Replica>> nodes;
    std::deque<raft::Message> messages;
    std::set<raft::NodeId> isolated;
private:
    std::size_t flush_;
};

void TestBootstrapAndOwnershipGuards() {
    Directory directory;
    Reject([&] { Replica replica(directory.Path(), {1, {2}}, kNew); }, "Invalid config rejected");
    Check(!std::filesystem::exists(directory.Path()), "Invalid config creates no directory");
    Reject([&] { Open(directory.Path(), 1, kExisting); }, "Existing mode never initializes missing data");
    auto replica = Open(directory.Path());
    Check(replica->status().last_applied == 0 && !replica->LocalGet("missing"), "Fresh state has no data or progress");
    Reject([&] { Open(directory.Path()); }, "New mode refuses existing directory");
    Reject([&] { Open(directory.Path(), 1, kExisting); }, "Exclusive ownership covers coordinator metadata");
    Reject([&] { replica->Propose({}); }, "Follower rejects proposal");
    Reject([&] { replica->AdvanceTime(-1ms); }, "Bad time input rejected");
    Check(replica->usable(), "Caller mistakes leave replica usable");
    replica.reset();
    auto reopened = Open(directory.Path(), 1, kExisting);
    Check(reopened->status().last_applied == 0, "Fresh replica reopens");
}

void TestApplicationAcknowledgementGuards() {
    Directory directory; const auto path = directory.Path();
    {
        raft::Storage storage(path, Config(), kNew); storage.SetTermAndVote(1, std::nullopt);
        storage.Append({Put(1, "a", "v"), Put(2, "b", "v")});
    }
    auto disk = std::make_unique<raft::Storage>(path, Config(), kExisting);
    raft::Node node(Config(), std::move(disk), Options().election, 2);
    Check(node.status().commit_index == 2 && node.status().last_applied == 0, "Recovered commitment does not pretend application happened");
    Reject([&] { node.MarkApplied(2); }, "Cannot skip application");
    node.MarkApplied(1);
    Reject([&] { node.MarkApplied(1); }, "Cannot repeat acknowledgement");
    node.MarkApplied(2);
    Reject([&] { node.MarkApplied(3); }, "Cannot apply beyond commit");
    Check(node.status().last_applied == 2, "Bad acknowledgements do not alter progress");
    Reject([&] { raft::Node invalid(Config(), {}, Options().election, 1); }, "Passive node cannot restore a commit");
    Directory other;
    auto empty = std::make_unique<raft::Storage>(other.Path(), Config(), kNew);
    Reject([&] { raft::Node invalid(Config(), std::move(empty), Options().election, 1); }, "Cannot restore beyond durable log");
}

void TestOnlyCommittedEntriesReachStore() {
    Cluster cluster; cluster.Start();
    for (const auto id : {1, 2, 3}) {
        Check(cluster.nodes[id]->status().last_applied == 1 && !cluster.nodes[id]->LocalGet("name"), "Committed NoOp changes no key");
    }
    const auto proposal = cluster.PutValue(1, "name", "Alice");
    Check(!cluster.nodes[1]->IsApplied(proposal) && !cluster.nodes[1]->LocalGet("name"), "Local proposal alone never changes Store");
    cluster.DeliverAll();
    for (const auto id : {1, 2, 3}) {
        Check(cluster.nodes[id]->IsApplied(proposal) && cluster.nodes[id]->LocalGet("name") == "Alice", "Every node applies committed Put");
        Check(cluster.nodes[id]->status().last_applied == cluster.nodes[id]->status().commit_index, "Successful event finishes ordered application");
    }
    cluster.nodes[1]->Propose({raft::CommandType::Put, "uncommitted", "tail"});
    Check(!cluster.nodes[1]->LocalGet("uncommitted"), "A later uncommitted tail remains invisible");
}

void TestOrderedDeleteBinaryAndEmptyValues() {
    Directory directory; auto replica = Open(directory.Path());
    const std::string binary_key("k\0x", 3), binary_value("v\0z", 3);
    Accept(*replica, Append({Put(1, "k", "old"), Delete(2, "k"), Put(3, "k", "new"),
                            Put(4, binary_key, binary_value), Put(5, "", ""), Delete(6, "absent")}, 6));
    Check(replica->LocalGet("k") == "new" && replica->LocalGet(binary_key) == binary_value && replica->LocalGet("") == "" &&
          !replica->LocalGet("absent"), "Order, binary strings, empty values and absent deletes survive application");
    const auto before = Read(directory.Path() + "/data");
    Accept(*replica, Append({}, 6, 6, 1));
    Check(Read(directory.Path() + "/data") == before, "Repeated heartbeat does not reapply already applied commands");
    replica.reset(); replica = Open(directory.Path(), 1, kExisting);
    Check(replica->status().last_applied == 6 && replica->LocalGet("k") == "new" && replica->LocalGet(binary_key) == binary_value,
          "Full-prefix startup replay restores ordered state before reads");
}

void TestPartitionStaleReadsAndConflictRepair() {
    Cluster cluster; cluster.Start(); cluster.PutValue(1, "name", "Alice"); cluster.DeliverAll();
    cluster.isolated.insert(1);
    const auto abandoned = cluster.PutValue(1, "name", "uncommitted"); cluster.DeliverAll();
    Check(cluster.nodes[1]->LocalGet("name") == "Alice" && !cluster.nodes[1]->IsApplied(abandoned), "Minority proposal never enters Store");
    cluster.Advance(2, 150ms); cluster.DeliverAll();
    const auto accepted = cluster.PutValue(2, "name", "Bob"); cluster.DeliverAll();
    Check(cluster.nodes[1]->LocalGet("name") == "Alice" && cluster.nodes[2]->LocalGet("name") == "Bob", "Local read can be stale during partition");
    cluster.isolated.clear(); cluster.Advance(2, 20ms); cluster.DeliverAll();
    for (const auto id : {1, 2, 3}) {
        Check(cluster.nodes[id]->LocalGet("name") == "Bob" && cluster.nodes[id]->IsApplied(accepted), "Repair reaches same actual Store state");
        Check(!cluster.nodes[id]->IsApplied(abandoned), "Same index in a newer term is not the abandoned proposal");
    }
}

void TestRestartWithoutNetworkUsesDurableCommit() {
    Cluster cluster; cluster.Start(); const auto applied = cluster.PutValue(1, "k", "v"); cluster.DeliverAll();
    cluster.PutValue(1, "tail", "not committed");
    cluster.nodes[1].reset(); cluster.nodes[1] = Open(cluster.directory.Path(1), 1, kExisting);
    Check(cluster.nodes[1]->LocalGet("k") == "v" && !cluster.nodes[1]->LocalGet("tail") && cluster.nodes[1]->IsApplied(applied),
          "Restart restores confirmed state without exposing uncommitted log tail");
    Check(cluster.nodes[1]->status().role == raft::Role::Follower && cluster.nodes[1]->status().last_applied == 2 &&
          cluster.nodes[1]->status().last_log_index == 3, "Recovery separates role, commit, application and log length");
    const auto checkpoint = Read(cluster.directory.Path(1) + "/COMMIT");
    Reject([&] { cluster.nodes[1]->Step(Append({Put(2, "k", "wrong", 2)}, 2, 1, 1, 2)); },
           "Recovered commit floor protects already applied data against impossible overwrite");
    Check(Read(cluster.directory.Path(1) + "/COMMIT") == checkpoint, "Invalid overwrite never advances checkpoint");
}

void TestFlushCompactionAndReplay() {
    Directory directory; auto replica = Open(directory.Path());
    Accept(*replica, Append({Put(1, "k", "old"), Put(2, "keep", "yes")}, 2)); replica->Flush();
    Accept(*replica, Append({Delete(3, "k")}, 3, 2, 1)); replica->Flush();
    Check(replica->sstable_count() == 2, "Committed commands use normal SSTable flush path");
    replica->Compact(); Check(replica->sstable_count() == 1, "Local compaction remains available");
    replica.reset(); replica = Open(directory.Path(), 1, kExisting, 128);
    Check(!replica->LocalGet("k") && replica->LocalGet("keep") == "yes" && replica->status().last_applied == 3,
          "Replay over compacted state does not resurrect a deleted key");
}

void TestCheckpointCorruptionTruncationAndTemporaryFile() {
    Directory directory;
    { auto replica = Open(directory.Path()); Accept(*replica, Append({Put(1, "k", "v")}, 1)); }
    const auto path = directory.Path() + "/COMMIT"; const auto good = Read(path);
    for (std::size_t size = 0; size < good.size(); ++size) {
        Write(path, good.substr(0, size));
        Reject([&] { Open(directory.Path(), 1, kExisting); }, "Truncated checkpoint is rejected");
        Check(Read(path) == good.substr(0, size), "Rejected checkpoint is preserved");
    }
    auto damaged = good; damaged[28] ^= 1; Write(path, damaged);
    Reject([&] { Open(directory.Path(), 1, kExisting); }, "CRC damage is rejected");
    Write(path, good + "x"); Reject([&] { Open(directory.Path(), 1, kExisting); }, "Oversized metadata is rejected");
    Write(path, good); Write(path + ".tmp", "unfinished replacement");
    auto reopened = Open(directory.Path(), 1, kExisting);
    Check(reopened->LocalGet("k") == "v" && !std::filesystem::exists(path + ".tmp"), "Valid authoritative checkpoint wins over temporary file");
}

void TestCheckpointIdentityBoundsAndPrefixBinding() {
    Directory directory;
    { auto replica = Open(directory.Path()); Accept(*replica, Append({Put(1, "k", "one"), Put(2, "tail", "same")}, 2)); }
    const auto good = Read(directory.Path() + "/COMMIT");
    auto checkpoint = kvstore::detail::DecodeReplicaCheckpoint(good, Config());
    for (const int fault : {0, 1, 2}) {
        auto changed = checkpoint;
        if (fault == 0) { changed.index = 3; }
        else if (fault == 1) { changed.term = 2; }
        else { changed.fingerprint ^= 1; }
        Write(directory.Path() + "/COMMIT", kvstore::detail::EncodeReplicaCheckpoint(Config(), changed));
        Reject([&] { Open(directory.Path(), 1, kExisting); }, "Valid CRC does not excuse index, term or prefix disagreement");
    }
    Write(directory.Path() + "/COMMIT", kvstore::detail::EncodeReplicaCheckpoint(Config(2), checkpoint));
    Reject([&] { Open(directory.Path(), 1, kExisting); }, "Checkpoint belongs to a specific node identity");
    Write(directory.Path() + "/COMMIT", good);
    Directory other;
    { auto replica = Open(other.Path()); Accept(*replica, Append({Put(1, "k", "different"), Put(2, "tail", "same")}, 2)); }
    Write(other.Path() + "/COMMIT", good);
    Reject([&] { Open(other.Path(), 1, kExisting); }, "Same last index and term cannot hide a different earlier command");
}

void TestMissingFilesNeverBootstrapEmptyState() {
    for (const auto& name : {"COMMIT", "LOCK", "raft", "raft.lock", "data.manifest", "data.lock", "data"}) {
        Directory directory;
        { auto replica = Open(directory.Path()); Accept(*replica, Append({Put(1, "k", "v")}, 1)); }
        const auto path = directory.Path() + "/" + name; std::filesystem::remove(path);
        Reject([&] { Open(directory.Path(), 1, kExisting); }, "Missing authoritative component refuses startup");
        Check(!std::filesystem::exists(path), "Missing component is not silently recreated");
    }
    Directory incomplete; std::filesystem::create_directory(incomplete.Path());
    Reject([&] { Open(incomplete.Path(), 1, kExisting); }, "Interrupted initial setup is not treated as an empty replica");
}

void TestCheckpointAndApplicationExceptionsStopOutput() {
    for (const bool leader : {false, true}) {
        for (const auto stage : {Stage::CheckpointTempSynced, Stage::CheckpointRenamed, Stage::CheckpointDurable,
                                 Stage::CommandStored, Stage::AppliedAdvanced}) {
            Directory directory; auto replica = Open(directory.Path());
            raft::Message pending;
            if (leader) { pending = PendingPut(*replica); }
            const auto target = leader ? 2U : 1U;
            Access::Hook(*replica, [stage, target](Stage at, raft::LogIndex index) {
                if (at == stage && index == target) { throw std::runtime_error("Injected coordinator failure"); }
            });
            Reject([&] { if (leader) { Reply(*replica, pending); } else { replica->Step(Append({Put(1, "k", "value")}, 1)); } },
                   "Publication or application failure propagates");
            Check(!replica->usable(), "Coordinator fails closed");
            Reject([&] { replica->DrainMessages(); }, "Failure cannot release queued messages");
            Reject([&] { replica->LocalGet("k"); }, "Partially applied state is not readable");
            replica.reset(); replica = Open(directory.Path(), 1, kExisting);
            Check(replica->LocalGet("k") == (stage == Stage::CheckpointTempSynced ? std::optional<std::string>{} : "value"),
                  "Recovery follows authoritative checkpoint, not partial in-memory progress");
        }
    }
}

void TestStoreFailureAfterCommitReplaysSafely() {
    Directory directory; auto replica = Open(directory.Path(), 1, kNew, 1);
    kvstore::StoreTestAccess::Hook(Access::Engine(*replica), [](kvstore::StoreTestAccess::Stage at) {
        if (at == kvstore::StoreTestAccess::Stage::WalSynced) { throw std::runtime_error("Injected Store freeze failure"); }
    });
    Reject([&] { replica->Step(Append({Put(1, "k", "v")}, 1)); }, "Actual Store failure stops application");
    Check(!replica->usable(), "Underlying engine failure stops coordinator");
    Reject([&] { replica->DrainMessages(); }, "Failed Store does not release a reply");
    replica.reset(); replica = Open(directory.Path(), 1, kExisting);
    Check(replica->LocalGet("k") == "v" && replica->status().last_applied == 1, "Durable commitment completes the interrupted Store write on restart");
}

void WaitKilled(pid_t pid) {
    struct Guard {
        pid_t pid;
        ~Guard() { if (pid > 0) { ::kill(pid, SIGKILL); while (::waitpid(pid, nullptr, 0) == -1 && errno == EINTR) {} } }
    } guard{pid};
    int status = 0; pid_t result;
    do { result = ::waitpid(pid, &status, 0); } while (result == -1 && errno == EINTR);
    if (result == pid) { guard.pid = -1; }
    Check(result == pid && WIFSIGNALED(status) && WTERMSIG(status) == SIGKILL, "Child killed at selected application boundary");
}

void TestProcessCrashesAcrossCommitAndApplication() {
    for (const bool leader : {false, true}) {
        for (const auto stage : {Stage::CheckpointTempSynced, Stage::CheckpointRenamed, Stage::CheckpointDurable,
                                 Stage::CommandStored, Stage::AppliedAdvanced}) {
            Directory directory; const auto pid = ::fork(); Check(pid >= 0, "Fork application crash child");
            if (pid == 0) {
                try {
                    auto replica = Open(directory.Path()); raft::Message pending;
                    if (leader) { pending = PendingPut(*replica); }
                    const auto target = leader ? 2U : 1U;
                    Access::Hook(*replica, [stage, target](Stage at, raft::LogIndex index) {
                        if (at == stage && index == target) { ::kill(::getpid(), SIGKILL); }
                    });
                    if (leader) { Reply(*replica, pending); } else { replica->Step(Append({Put(1, "k", "value")}, 1)); }
                    ::_exit(4);
                } catch (...) { ::_exit(2); }
            }
            WaitKilled(pid);
            auto replica = Open(directory.Path(), 1, kExisting);
            const bool published = stage != Stage::CheckpointTempSynced;
            Check(replica->LocalGet("k") == (published ? std::optional<std::string>{"value"} : std::nullopt),
                  "Crash recovery uses whichever checkpoint was published");
            Check(replica->status().commit_index == replica->status().last_applied &&
                  replica->status().last_applied == (leader ? (published ? 2U : 1U) : (published ? 1U : 0U)),
                  "Replay completes before constructor returns");
        }
    }
    for (const auto stage : {Stage::CommandStored, Stage::AppliedAdvanced}) {
        Directory directory; const auto pid = ::fork(); Check(pid >= 0, "Fork interrupted batch child");
        if (pid == 0) {
            try {
                auto replica = Open(directory.Path());
                Access::Hook(*replica, [stage](Stage at, raft::LogIndex index) {
                    if (at == stage && index == 2) { ::kill(::getpid(), SIGKILL); }
                });
                replica->Step(Append({Put(1, "k", "old"), Delete(2, "k"), Put(3, "k", "final"), Put(4, "other", "yes")}, 4));
                ::_exit(4);
            } catch (...) { ::_exit(2); }
        }
        WaitKilled(pid); auto replica = Open(directory.Path(), 1, kExisting);
        Check(replica->LocalGet("k") == "final" && replica->LocalGet("other") == "yes" && replica->status().last_applied == 4,
              "Crash in the middle of a multi-entry commit replays the whole prefix in order");
    }
}

void TestRealPartialStoreWriteNeedsExplicitTailRecovery() {
    Directory directory; const auto pid = ::fork(); Check(pid >= 0, "Fork partial Store write child");
    if (pid == 0) {
        try {
            auto replica = Open(directory.Path());
            Access::Hook(*replica, [](Stage stage, raft::LogIndex) {
                if (stage == Stage::CheckpointDurable) {
                    ::signal(SIGXFSZ, SIG_IGN);
                    struct rlimit limit {}; if (::getrlimit(RLIMIT_FSIZE, &limit) != 0) { ::_exit(5); }
                    limit.rlim_cur = 1; if (::setrlimit(RLIMIT_FSIZE, &limit) != 0) { ::_exit(5); }
                }
            });
            try { replica->Step(Append({Put(1, "k", "committed")}, 1)); } catch (const std::exception&) {
                ::_exit(!replica->usable() && std::filesystem::file_size(directory.Path() + "/data") == 1 ? 0 : 3);
            }
            ::_exit(4);
        } catch (...) { ::_exit(2); }
    }
    int status = 0; pid_t result;
    do { result = ::waitpid(pid, &status, 0); } while (result == -1 && errno == EINTR);
    Check(result == pid && WIFEXITED(status) && WEXITSTATUS(status) == 0, "Actual one-byte Store WAL write failed closed");
    const auto before = Read(directory.Path() + "/data");
    Reject([&] { Open(directory.Path(), 1, kExisting); }, "Strict recovery rejects incomplete Store WAL tail");
    Check(Read(directory.Path() + "/data") == before, "Strict recovery preserves incomplete tail");
    auto options = Options(); options.store_recovery = kvstore::WalRecoveryMode::TruncateIncompleteTail;
    Replica recovered(directory.Path(), Config(), kExisting, options);
    Check(recovered.LocalGet("k") == "committed" && recovered.status().last_applied == 1,
          "Explicit tail truncation followed by Raft replay restores the committed command");
}

void TestMixedWorkloadMatchesIndependentMapAcrossRestarts() {
    for (const auto seed : {17U, 20261016U}) {
        Cluster cluster(256); cluster.Start(); std::mt19937 random(seed);
        std::map<raft::LogIndex, raft::LogEntry> committed;
        const auto observe = [&] {
            for (const auto id : {1, 2, 3}) {
                auto& replica = *cluster.nodes[id]; const auto state = replica.status();
                Check(state.last_applied == state.commit_index, "Every completed event applies the confirmed prefix");
                std::map<std::string, std::string> model;
                const auto& log = Access::Log(replica);
                for (raft::LogIndex i = 0; i < state.last_applied; ++i) {
                    const auto& entry = log[i]; const auto& command = entry.command;
                    const auto inserted = committed.emplace(entry.index, entry);
                    if (!inserted.second) {
                        const auto& old = inserted.first->second;
                        Check(old.term == entry.term && old.command.type == command.type && old.command.key == command.key &&
                              old.command.value == command.value, "Applied histories never disagree across nodes or restarts");
                    }
                    if (command.type == raft::CommandType::Put) { model[command.key] = command.value; }
                    else if (command.type == raft::CommandType::Delete) { model.erase(command.key); }
                }
                for (int key = 0; key < 8; ++key) {
                    const auto name = "key" + std::to_string(key); const auto found = model.find(name);
                    const auto expected = found == model.end() ? std::nullopt : std::optional<std::string>{found->second};
                    Check(replica.LocalGet(name) == expected, "Actual Store values match independent sequential map");
                }
            }
        };
        for (int event = 0; event < 250; ++event) {
            const raft::NodeId id = 1 + random() % 3; const auto action = random() % 12;
            if (action < 4 && cluster.nodes[id]->status().role == raft::Role::Leader) {
                cluster.Propose(id, {action == 0 ? raft::CommandType::Delete : raft::CommandType::Put,
                    "key" + std::to_string(random() % 8), action == 0 ? "" : "v" + std::to_string(event)});
            } else if (action < 7) { cluster.Advance(id, std::chrono::milliseconds{1 + random() % 50}); }
            else if (action == 11) { cluster.Restart(id); }
            else if (!cluster.messages.empty()) {
                const auto index = random() % cluster.messages.size();
                if (action == 7) { cluster.messages.erase(cluster.messages.begin() + static_cast<std::ptrdiff_t>(index)); }
                else {
                    if (action == 8 && cluster.messages.size() < 200) { cluster.messages.push_back(cluster.messages[index]); }
                    cluster.Deliver(index);
                }
            }
            observe();
        }
        cluster.messages.clear();
        for (int round = 0; round < 40; ++round) {
            for (const auto id : {1, 2, 3}) { cluster.Advance(id, 20ms); cluster.DeliverAll(); }
        }
        raft::NodeId leader = 0;
        for (const auto id : {1, 2, 3}) { if (cluster.nodes[id]->status().role == raft::Role::Leader) { leader = id; } }
        Check(leader != 0, "Cluster regains leader"); cluster.PutValue(leader, "key0", "final"); cluster.DeliverAll(); observe();
        for (const auto id : {1, 2, 3}) {
            Check(cluster.nodes[id]->LocalGet("key0") == "final", "Real Stores converge after fair delivery");
            cluster.Restart(id);
        }
        observe();
    }
}
}  // namespace

int main() {
    try {
        TestBootstrapAndOwnershipGuards(); TestApplicationAcknowledgementGuards(); TestOnlyCommittedEntriesReachStore();
        TestOrderedDeleteBinaryAndEmptyValues(); TestPartitionStaleReadsAndConflictRepair(); TestRestartWithoutNetworkUsesDurableCommit();
        TestFlushCompactionAndReplay(); TestCheckpointCorruptionTruncationAndTemporaryFile(); TestCheckpointIdentityBoundsAndPrefixBinding();
        TestMissingFilesNeverBootstrapEmptyState(); TestCheckpointAndApplicationExceptionsStopOutput(); TestStoreFailureAfterCommitReplaysSafely();
        TestProcessCrashesAcrossCommitAndApplication(); TestRealPartialStoreWriteNeedsExplicitTailRecovery();
        TestMixedWorkloadMatchesIndependentMapAcrossRestarts();
        std::cout << "All 15 replicated Store tests passed (12 process crashes and 1 real partial write).\n";
    } catch (const std::exception& error) {
        std::cerr << "Replicated Store test failed: " << error.what() << '\n'; return 1;
    }
}
