#include "kvstore/raft/node.h"
#include "kvstore/raft/storage.h"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <deque>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <iterator>
#include <map>
#include <memory>
#include <random>
#include <set>
#include <stdexcept>
#include <string>
#include <sys/wait.h>
#include <unistd.h>

namespace kvstore::raft {
struct StorageTestAccess {
    using Stage = Storage::Stage;
    static void Hook(Storage& storage, std::function<void(Stage)> hook) { storage.hook_ = std::move(hook); }
};
}  // namespace kvstore::raft

namespace {
namespace raft = kvstore::raft;
using namespace std::chrono_literals;
using Stage = raft::StorageTestAccess::Stage;
constexpr auto kNew = raft::StorageOpenMode::CreateNew;
constexpr auto kExisting = raft::StorageOpenMode::OpenExisting;

void Check(bool condition, const std::string& message) {
    if (!condition) { throw std::runtime_error(message); }
}
template <typename Action>
void Reject(Action action, const std::string& message) {
    try { action(); } catch (const std::exception&) { return; }
    throw std::runtime_error(message);
}
class Directory {
public:
    Directory() {
        char pattern[] = "/tmp/kvstore-replication-XXXXXX";
        const auto path = ::mkdtemp(pattern);
        Check(path != nullptr, "Create replication test directory"); path_ = path;
    }
    ~Directory() { std::error_code ignored; std::filesystem::remove_all(path_, ignored); }
    std::string Path(raft::NodeId id = 1) const { return (path_ / (std::to_string(id) + ".raft")).string(); }
private:
    std::filesystem::path path_;
};
raft::NodeConfig Config(raft::NodeId id) {
    raft::NodeConfig config{id, {}};
    for (raft::NodeId peer = 1; peer <= 3; ++peer) { if (peer != id) { config.peers.push_back(peer); } }
    return config;
}
raft::ElectionOptions Options(raft::NodeId id = 1) {
    const auto timeout = std::chrono::milliseconds{100 + static_cast<int>(id - 1) * 50};
    return {20ms, timeout, timeout, id};
}
std::unique_ptr<raft::Node> Open(const std::string& path, raft::NodeId id = 1, raft::StorageOpenMode mode = kNew) {
    return std::make_unique<raft::Node>(Config(id), std::make_unique<raft::Storage>(path, Config(id), mode), Options(id));
}
raft::LogEntry Entry(raft::LogIndex index, raft::Term term, std::string value = "v") {
    return {index, term, {raft::CommandType::Put, "k" + std::to_string(index), std::move(value)}};
}
bool Equal(const raft::LogEntry& a, const raft::LogEntry& b) {
    return a.index == b.index && a.term == b.term && a.command.type == b.command.type &&
           a.command.key == b.command.key && a.command.value == b.command.value;
}
std::string Read(const std::string& path) {
    std::ifstream input(path, std::ios::binary);
    Check(input.is_open(), "Read durable test fixture");
    return {std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
}
void Seed(const std::string& path, raft::NodeId id, raft::Term term, std::vector<raft::LogEntry> entries) {
    raft::Storage disk(path, Config(id), kNew);
    disk.SetTermAndVote(term, std::nullopt); disk.Append(entries);
}
raft::Message Append(raft::Term term, raft::LogIndex previous, raft::Term previous_term,
                     std::vector<raft::LogEntry> entries = {}, raft::LogIndex commit = 0, std::uint64_t rpc = 1) {
    return {2, 1, rpc, raft::AppendEntriesRequest{term, 2, previous, previous_term, std::move(entries), commit}};
}
bool Accept(raft::Node& node, const raft::Message& request) {
    node.Step(request);
    const auto replies = node.DrainMessages();
    Check(replies.size() == 1 && replies[0].rpc_id == request.rpc_id && replies[0].to == request.from,
          "Append response echoes request identity");
    return std::get<raft::AppendEntriesResponse>(replies[0].body).success;
}
std::vector<raft::Message> Elect(raft::Node& node) {
    node.AdvanceTime(node.status().election_timeout - node.status().election_elapsed);
    const auto votes = node.DrainMessages();
    Check(votes.size() == 2, "Campaign has two peers");
    node.Step({votes[0].to, node.config().id, votes[0].rpc_id, raft::RequestVoteResponse{node.status().term, true}});
    Check(node.status().role == raft::Role::Leader, "Elect leader");
    return node.DrainMessages();
}
void Reply(raft::Node& node, const raft::Message& request, bool success = true) {
    node.Step({request.to, request.from, request.rpc_id,
               raft::AppendEntriesResponse{std::get<raft::AppendEntriesRequest>(request.body).term, success}});
}

// The oracle remembers every observed committed entry across node restarts.
class Cluster {
public:
    Cluster() {
        for (raft::NodeId id = 1; id <= 3; ++id) { nodes[id] = Open(directory.Path(id), id); }
    }
    void Observe() {
        for (const auto& pair : nodes) {
            const auto state = pair.second->status();
            Check(state.commit_index <= state.last_log_index && state.last_applied == 0, "Commit is a log prefix, not application");
            for (const auto& entry : pair.second->CommittedEntries()) {
                const auto inserted = committed.emplace(entry.index, entry);
                Check(inserted.second || Equal(inserted.first->second, entry), "All committed histories agree at each index");
            }
            const auto& log = pair.second->persistent_state().log;
            for (const auto& entry : committed) {
                // Once a node has observed a committed index it must retain it after restart.
                if (entry.first <= known_commits[state.id]) {
                    Check(entry.first <= log.size() && Equal(log[entry.first - 1], entry.second), "Committed entries never disappear");
                }
            }
            known_commits[state.id] = std::max(known_commits[state.id], state.commit_index);
            if (state.role == raft::Role::Leader) {
                const auto inserted = leaders.emplace(state.term, state.id);
                Check(inserted.second || inserted.first->second == state.id, "Each term has at most one elected leader");
                if (inserted.second) {
                    for (const auto& entry : committed) {
                        Check(entry.first <= log.size() && Equal(log[entry.first - 1], entry.second),
                              "Every newly elected leader contains all previously observed committed entries");
                    }
                }
            }
        }
    }
    void Collect(raft::NodeId id) {
        for (auto& message : nodes.at(id)->DrainMessages()) { messages.push_back(std::move(message)); }
        Observe();
    }
    void Advance(raft::NodeId id, std::chrono::milliseconds time) { nodes.at(id)->AdvanceTime(time); Collect(id); }
    void Deliver(std::size_t index = 0) {
        auto position = messages.begin() + static_cast<std::ptrdiff_t>(index);
        auto message = std::move(*position); messages.erase(position);
        if (isolated.count(message.from) || isolated.count(message.to)) { return; }
        nodes.at(message.to)->Step(message); Collect(message.to);
    }
    void DeliverAll() {
        std::size_t count = 0;
        while (!messages.empty()) { Check(++count <= 5000, "Replication reaches idle"); Deliver(); }
    }
    void Start() { Advance(1, 100ms); DeliverAll(); }
    raft::Proposal Put(raft::NodeId id, std::string key, std::string value) {
        auto result = nodes.at(id)->Propose({raft::CommandType::Put, std::move(key), std::move(value)}); Collect(id); return result;
    }
    void Restart(raft::NodeId id) {
        nodes.at(id).reset(); nodes.at(id) = Open(directory.Path(id), id, kExisting); Observe();
    }
    Directory directory;
    std::map<raft::NodeId, std::unique_ptr<raft::Node>> nodes;
    std::deque<raft::Message> messages;
    std::set<raft::NodeId> isolated;
    std::map<raft::LogIndex, raft::LogEntry> committed;
    std::map<raft::NodeId, raft::LogIndex> known_commits;
    std::map<raft::Term, raft::NodeId> leaders;
};

void TestLeaderOnlyAndInvalidProposals() {
    Directory directory; auto node = Open(directory.Path());
    Reject([&] { node->Propose({raft::CommandType::Put, "k", "v"}); }, "Follower rejects proposals");
    raft::Node passive(Config(1));
    Reject([&] { passive.Propose({}); }, "Passive model cannot accept proposals");
    Elect(*node);
    const auto bytes = Read(directory.Path());
    for (const auto& command : std::vector<raft::Command>{
            {raft::CommandType::Delete, "k", "bad"}, {raft::CommandType::NoOp, {}, {}},
            {static_cast<raft::CommandType>(99), {}, {}},
            {raft::CommandType::Put, "k", std::string(raft::kMaxRaftCommandBytes, 'x')}}) {
        Reject([&] { node->Propose(command); }, "Reject invalid proposal before writing");
        Check(node->usable() && Read(directory.Path()) == bytes, "Invalid caller input leaves leader usable");
    }
    const auto proposal = node->Propose({raft::CommandType::Put, {}, {}});
    Check(proposal.index == 2 && proposal.term == 1, "Empty key and value are valid owned bytes");
}

void TestNoOpAndMajorityCommit() {
    Cluster cluster;
    cluster.Advance(1, 100ms);
    // Complete only the vote exchange with node 2.
    cluster.Deliver(); cluster.Deliver(1);
    Check(cluster.nodes[1]->status().role == raft::Role::Leader && cluster.nodes[1]->status().last_log_index == 1 &&
          cluster.nodes[1]->status().commit_index == 0, "Leader no-op is durable but not committed by election alone");
    cluster.DeliverAll();
    for (const auto id : {1, 2, 3}) {
        Check(cluster.nodes[id]->status().commit_index == 1 &&
              cluster.nodes[id]->CommittedEntries()[0].command.type == raft::CommandType::NoOp,
              "No-op commits after majority acknowledgement and propagates to followers");
    }
    const auto proposal = cluster.Put(1, "name", "Alice");
    Check(proposal.index == 2 && cluster.nodes[1]->status().commit_index == 1, "Proposal result is not a commit acknowledgement");
    Check(cluster.nodes[1]->persistent_state().log[1].command.value == "Alice", "Local durable append precedes replication output");
    // A separate reader cannot open the locked file, so the append boundary is also crash-tested below.
    cluster.isolated.insert(3); cluster.DeliverAll();
    Check(cluster.nodes[1]->status().commit_index == 2 && cluster.nodes[2]->status().commit_index == 2 &&
          cluster.nodes[3]->status().commit_index == 1, "One unavailable follower does not prevent majority commit");
}

void TestCommittedCursorOwnsItsEntries() {
    Cluster cluster; cluster.Start(); cluster.Put(1, "k", "v"); cluster.DeliverAll();
    auto copy = cluster.nodes[1]->CommittedEntries(1);
    Check(copy.size() == 1 && copy[0].index == 2, "Cursor returns only committed entries after its index");
    copy[0].command.value = "changed";
    Check(cluster.nodes[1]->CommittedEntries(1)[0].command.value == "v", "Caller receives owned entries");
    Check(cluster.nodes[1]->CommittedEntries(2).empty(), "Caught-up cursor is empty");
    Reject([&] { cluster.nodes[1]->CommittedEntries(3); }, "Cannot read past commit");
}

void TestFollowerChecksPrefix() {
    Directory directory; Seed(directory.Path(), 1, 3, {Entry(1, 1), Entry(2, 2)});
    auto node = Open(directory.Path(), 1, kExisting);
    const auto bytes = Read(directory.Path());
    Check(!Accept(*node, Append(3, 3, 2, {Entry(4, 3)}, 4)), "Reject missing prefix");
    Check(!Accept(*node, Append(3, 2, 1, {Entry(3, 3)}, 3)), "Reject wrong prefix term");
    Check(node->status().commit_index == 0 && Read(directory.Path()) == bytes, "Rejected append never changes log or commit");
    Check(Accept(*node, Append(3, 2, 2, {Entry(3, 3)}, 3)), "Accept matching prefix");
    Check(node->status().commit_index == 3, "Follower advances to confirmed leader commit after sync");
}

void TestDuplicatesAndShortRequestsRetainSuffix() {
    Directory directory; Seed(directory.Path(), 1, 3, {Entry(1, 1), Entry(2, 2), Entry(3, 3)});
    auto node = Open(directory.Path(), 1, kExisting);
    const auto bytes = Read(directory.Path());
    Check(Accept(*node, Append(3, 1, 1, {Entry(2, 2)}, 99)), "Accept matching shorter append");
    Check(node->status().last_log_index == 3 && node->status().commit_index == 2, "Commit only through this request, not unrelated local tail");
    Check(Accept(*node, Append(3, 0, 0, {}, 99)), "Delayed empty-prefix heartbeat is valid");
    Check(node->status().commit_index == 2 && Read(directory.Path()) == bytes, "Delayed heartbeat neither regresses commit nor truncates suffix");
    Check(Accept(*node, Append(3, 1, 1, {Entry(2, 2)}, 2)) && Read(directory.Path()) == bytes,
          "Duplicate append causes no extra durable write");
}

void TestConflictReplacesOnlySuffix() {
    Directory directory; Seed(directory.Path(), 1, 3, {Entry(1, 1), Entry(2, 2), Entry(3, 2)});
    auto node = Open(directory.Path(), 1, kExisting);
    const raft::LogEntry deletion{2, 3, {raft::CommandType::Delete, "k1", {}}};
    Check(Accept(*node, Append(3, 0, 0, {Entry(1, 1), deletion}, 2)), "Replace at first differing term");
    Check(node->status().last_log_index == 2 && Equal(node->persistent_state().log[0], Entry(1, 1)) &&
          Equal(node->persistent_state().log[1], deletion), "Keep common prefix, remove old tail, preserve Delete command");
    node.reset();
    auto reopened = Open(directory.Path(), 1, kExisting);
    Check(reopened->status().last_log_index == 2 && Equal(reopened->persistent_state().log[1], deletion), "Conflict repair survives reopen");
}

void TestCommittedAndSameTermConflictsStopNode() {
    for (const bool same_term : {false, true}) {
        Directory directory; Seed(directory.Path(), 1, 3, {Entry(1, 1), Entry(2, 2)});
        auto node = Open(directory.Path(), 1, kExisting);
        Check(Accept(*node, Append(3, 2, 2, {}, same_term ? 0 : 2)), "Set committed prefix for protection test");
        const auto bytes = Read(directory.Path());
        Reject([&] { node->Step(Append(3, 1, 1, {Entry(2, same_term ? 2 : 3, "different")})); },
               "Impossible leader history is an invariant failure");
        Check(!node->usable() && Read(directory.Path()) == bytes, "Fail stopped without corrupting existing log");
        Reject([&] { node->DrainMessages(); }, "Invariant failure emits no success");
    }
}

void TestMalformedAppendsAreIgnored() {
    Directory directory; auto node = Open(directory.Path()); node->AdvanceTime(90ms);
    const auto bytes = Read(directory.Path());
    auto good = std::get<raft::AppendEntriesRequest>(Append(9, 0, 0, {Entry(1, 9)}).body);
    std::vector<raft::AppendEntriesRequest> bad;
    auto value = good; value.entries[0].index = 2; bad.push_back(value);
    value = good; value.entries[0].term = 10; bad.push_back(value);
    value = good; value.entries[0].term = 0; bad.push_back(value);
    value = good; value.entries[0].command.type = raft::CommandType::NoOp; bad.push_back(value);
    value = good; value.entries[0].command.type = raft::CommandType::Delete; bad.push_back(value);
    value = good; value.entries[0].command.value.assign(raft::kMaxRaftCommandBytes + 1, 'x'); bad.push_back(value);
    value = good; value.prev_log_index = raft::kMaxRaftEntries + 1; bad.push_back(value);
    value = good; value.entries = {Entry(1, 9), Entry(2, 8)}; bad.push_back(value);
    for (const auto& request : bad) {
        node->Step({2, 1, 10, request});
        Check(node->DrainMessages().empty() && node->status().term == 0 && node->status().election_elapsed == 90ms &&
              Read(directory.Path()) == bytes, "Malformed request cannot alter durable state or defer election");
    }
}

void TestResponseCorrelationAndDuplicateRejection() {
    Directory directory; auto node = Open(directory.Path()); const auto initial = Elect(*node);
    const auto request = initial[0];
    auto wrong = request; wrong.rpc_id += 99; Reply(*node, wrong);
    node->Step({request.to, 1, request.rpc_id, raft::AppendEntriesResponse{0, true}});
    Check(node->status().commit_index == 0 && node->DrainMessages().empty(), "Wrong or stale responses do not count");
    Reply(*node, request);
    Check(node->status().commit_index == 1, "Matching durable peer confirmation commits no-op");
    node->DrainMessages();
    Reply(*node, request, false); Reply(*node, request);
    Check(node->status().commit_index == 1 && node->DrainMessages().empty(), "Delayed duplicate success and rejection cannot undo progress");
}

void TestRetryPreservesRequestAndAcceptsDelayedReply() {
    Directory directory; auto node = Open(directory.Path()); const auto initial = Elect(*node);
    node->Propose({raft::CommandType::Put, "later", "v"});
    Check(node->DrainMessages().empty(), "Busy peers wait for the outstanding request");
    for (int i = 0; i < 3; ++i) {
        node->AdvanceTime(20ms); const auto retried = node->DrainMessages();
        Check(retried.size() == 2, "Heartbeat retries both busy peers");
        for (std::size_t j = 0; j < 2; ++j) {
            const auto& body = std::get<raft::AppendEntriesRequest>(retried[j].body);
            Check(retried[j].rpc_id == initial[j].rpc_id && body.entries.size() == 1 && body.leader_commit == 0 &&
                  body.prev_log_index == 0, "Retry preserves exact ID, prefix and bounded payload");
        }
    }
    Reply(*node, initial[0]);
    Check(node->status().commit_index == 1, "An arbitrarily delayed matching acknowledgement is still useful");
    const auto next = node->DrainMessages();
    Check(next.size() == 1 && std::get<raft::AppendEntriesRequest>(next[0].body).entries[0].index == 2,
          "Successful reply immediately releases queued newer entries");
}

void TestBatchedCatchupAndOldTermCommitRule() {
    Directory directory;
    std::vector<raft::LogEntry> old;
    for (raft::LogIndex i = 1; i <= 140; ++i) { old.push_back(Entry(i, 1)); }
    Seed(directory.Path(), 1, 1, old);
    auto leader = Open(directory.Path(), 1, kExisting); auto follower = Open(directory.Path(2), 2);
    auto requests = Elect(*leader);
    std::size_t batches = 0, rejections = 0;
    std::deque<raft::Message> queue;
    for (auto& request : requests) { if (request.to == 2) { queue.push_back(std::move(request)); } }
    while (!queue.empty()) {
        Check(batches + rejections < 400, "Backtracking terminates");
        auto request = std::move(queue.front()); queue.pop_front();
        const auto& body = std::get<raft::AppendEntriesRequest>(request.body);
        Check(body.entries.size() <= 64, "Catchup RPC has bounded entry count");
        follower->Step(request);
        const auto replies = follower->DrainMessages();
        const auto success = std::get<raft::AppendEntriesResponse>(replies.at(0).body).success;
        if (!success) { ++rejections; }
        else if (!body.entries.empty()) { ++batches; }
        leader->Step(replies.at(0));
        if (success && body.prev_log_index + body.entries.size() < 141) {
            Check(leader->status().commit_index == 0, "Majority of old-term entries alone cannot advance commit");
        }
        for (auto& next : leader->DrainMessages()) { if (next.to == 2) { queue.push_back(std::move(next)); } }
    }
    Check(rejections == 140 && batches == 3 && leader->status().commit_index == 141 && follower->status().commit_index == 141,
          "Backtrack to common prefix, stream three batches, commit old prefix through current-term no-op");
    Check(follower->persistent_state().log.back().command.type == raft::CommandType::NoOp, "Current-term boundary is a no-op");
}

void TestCatchupByteBudget() {
    Directory directory;
    const std::string large(raft::kMaxRaftCommandBytes - 2, 'x');
    Seed(directory.Path(), 1, 1, {Entry(1, 1, large), Entry(2, 1, large), Entry(3, 1, large)});
    auto leader = Open(directory.Path(), 1, kExisting); auto follower = Open(directory.Path(2), 2);
    std::deque<raft::Message> queue;
    for (auto& request : Elect(*leader)) { if (request.to == 2) { queue.push_back(std::move(request)); } }
    std::size_t data_batches = 0;
    while (!queue.empty()) {
        auto request = std::move(queue.front()); queue.pop_front();
        const auto& body = std::get<raft::AppendEntriesRequest>(request.body);
        std::size_t bytes = 13;
        for (const auto& entry : body.entries) { bytes += 25 + entry.command.key.size() + entry.command.value.size(); }
        Check(bytes <= 2 * 1024 * 1024, "Catchup RPC fits encoded byte budget even for large entries");
        follower->Step(request);
        for (const auto& reply : follower->DrainMessages()) {
            if (std::get<raft::AppendEntriesResponse>(reply.body).success && !body.entries.empty() &&
                body.entries.front().command.type == raft::CommandType::Put) { ++data_batches; }
            leader->Step(reply);
        }
        for (auto& next : leader->DrainMessages()) { if (next.to == 2) { queue.push_back(std::move(next)); } }
    }
    Check(data_batches == 3 && follower->status().commit_index == 4, "Large commands require separate catchup batches");
}

void TestMinorityCannotCommitAndRejoinRepairsConflict() {
    Cluster cluster; cluster.Start(); cluster.Put(1, "keep", "committed"); cluster.DeliverAll();
    cluster.isolated.insert(1);
    const auto abandoned = cluster.Put(1, "lost", "uncommitted"); cluster.DeliverAll();
    Check(cluster.nodes[1]->status().commit_index == 2 && abandoned.index == 3, "Isolated leader can only append locally");
    cluster.Advance(2, 150ms); cluster.DeliverAll();
    Check(cluster.nodes[2]->status().role == raft::Role::Leader, "Majority elects a replacement");
    const auto deletion = cluster.nodes[2]->Propose({raft::CommandType::Delete, "keep", {}});
    cluster.Collect(2); cluster.DeliverAll();
    Check(cluster.nodes[2]->status().commit_index == deletion.index, "New leader commits deletion with its majority");
    cluster.isolated.clear(); cluster.Advance(2, 20ms); cluster.DeliverAll();
    for (const auto id : {1, 2, 3}) {
        const auto& log = cluster.nodes[id]->persistent_state().log;
        Check(cluster.nodes[id]->status().commit_index == deletion.index && log[2].command.type == raft::CommandType::NoOp &&
              log.back().command.type == raft::CommandType::Delete, "Rejoin replaces abandoned suffix while retaining committed prefix");
        for (const auto& entry : log) { Check(entry.command.key != "lost", "Uncommitted old-leader command does not survive repair"); }
    }
    Reject([&] { cluster.nodes[1]->Propose({}); }, "Demoted leader rejects new proposals");
}

void TestRestartReestablishesCommitWithoutNewClientCommand() {
    Cluster cluster; cluster.Start(); cluster.Put(1, "k", "v"); cluster.DeliverAll();
    for (const auto id : {1, 2, 3}) {
        cluster.Restart(id);
        Check(cluster.nodes[id]->status().commit_index == 0 && cluster.nodes[id]->status().last_log_index == 2,
              "Restart restores durable log without guessing commit from length");
    }
    cluster.messages.clear(); cluster.Advance(1, 100ms); cluster.DeliverAll();
    for (const auto id : {1, 2, 3}) {
        Check(cluster.nodes[id]->status().commit_index == 3 && cluster.nodes[id]->CommittedEntries()[1].command.value == "v",
              "New-term no-op reestablishes the committed old prefix with no client write");
    }
}

void TestPersistenceFailuresReleaseNoAcknowledgement() {
    for (const int action : {0, 1, 2}) {
        for (const auto stage : {Stage::RecordWritten, Stage::RecordSynced, Stage::StateUpdated}) {
            Directory directory;
            auto storage = std::make_unique<raft::Storage>(directory.Path(), Config(1), kNew);
            auto* access = storage.get();
            auto node = std::make_unique<raft::Node>(Config(1), std::move(storage), Options());
            std::vector<raft::Message> votes;
            if (action == 1) { Elect(*node); }
            else if (action == 2) { node->AdvanceTime(100ms); votes = node->DrainMessages(); }
            else { Check(Accept(*node, Append(1, 0, 0)), "Prepare follower term before injected append failure"); }
            raft::StorageTestAccess::Hook(*access, [stage](Stage at) {
                if (at == stage) { throw std::runtime_error("Injected replication persistence failure"); }
            });
            Reject([&] {
                if (action == 1) { node->Propose({raft::CommandType::Put, "new", "value"}); }
                else if (action == 2) { node->Step({votes[0].to, 1, votes[0].rpc_id, raft::RequestVoteResponse{1, true}}); }
                else { node->Step(Append(1, 0, 0, {Entry(1, 1)}, 1)); }
            }, "Persistence failure propagates");
            Check(!node->usable(), "Replication storage failure stops node");
            Reject([&] { node->DrainMessages(); }, "No replication message or success may escape failed persistence");
            node.reset();
            auto reopened = Open(directory.Path(), 1, kExisting);
            Check(reopened->status().last_log_index == (action == 1 ? 2 : 1) && reopened->status().commit_index == 0,
                  "Complete but unacknowledged frame recovers as log, not inferred commit");
        }
    }
}

void TestProcessCrashAfterSyncBeforeOutput() {
    for (const bool proposing : {false, true}) {
        Directory directory;
        const auto pid = ::fork(); Check(pid >= 0, "Fork replication crash child");
        if (pid == 0) {
            try {
                auto storage = std::make_unique<raft::Storage>(directory.Path(), Config(1), kNew);
                auto* access = storage.get();
                raft::Node node(Config(1), std::move(storage), Options());
                if (proposing) { Elect(node); }
                else { Accept(node, Append(1, 0, 0)); }
                raft::StorageTestAccess::Hook(*access, [](Stage at) {
                    if (at == Stage::RecordSynced) { ::kill(::getpid(), SIGKILL); }
                });
                if (proposing) { node.Propose({raft::CommandType::Put, "crash", "value"}); }
                else { node.Step(Append(1, 0, 0, {Entry(1, 1)}, 1)); }
                ::_exit(4);
            } catch (...) { ::_exit(2); }
        }
        struct Reap {
            pid_t pid;
            ~Reap() { if (pid > 0) { ::kill(pid, SIGKILL); while (::waitpid(pid, nullptr, 0) == -1 && errno == EINTR) {} } }
        } guard{pid};
        int status = 0; pid_t result;
        do { result = ::waitpid(pid, &status, 0); } while (result == -1 && errno == EINTR);
        if (result == pid) { guard.pid = -1; }
        Check(result == pid && WIFSIGNALED(status) && WTERMSIG(status) == SIGKILL, "Crash occurred after sync before protocol output");
        auto node = Open(directory.Path(), 1, kExisting);
        Check(node->status().last_log_index == (proposing ? 2 : 1) && node->status().commit_index == 0,
              "Both leader proposal and follower append survive crash without claiming commitment");
    }
}

void TestMixedCommandsUnderMessageFaultsAndRestarts() {
    for (const auto seed : {17U, 99U, 20261016U}) {
        Cluster cluster; cluster.Start(); std::mt19937 random(seed);
        for (int event = 0; event < 500; ++event) {
            const raft::NodeId id = 1 + random() % 3;
            const auto action = random() % 12;
            if (action < 3 && cluster.nodes[id]->status().role == raft::Role::Leader) {
                cluster.nodes[id]->Propose({action == 0 ? raft::CommandType::Delete : raft::CommandType::Put,
                    "key" + std::to_string(random() % 7), action == 0 ? "" : "value" + std::to_string(event)});
                cluster.Collect(id);
            } else if (action < 6) { cluster.Advance(id, std::chrono::milliseconds{1 + random() % 50}); }
            else if (action == 11) { cluster.Restart(id); }
            else if (!cluster.messages.empty()) {
                const auto index = random() % cluster.messages.size();
                if (action == 6) { cluster.messages.erase(cluster.messages.begin() + static_cast<std::ptrdiff_t>(index)); }
                else {
                    if (action == 7 && cluster.messages.size() < 300) { cluster.messages.push_back(cluster.messages[index]); }
                    cluster.Deliver(index);
                }
            }
            cluster.Observe();
        }
        cluster.messages.clear();
        for (int round = 0; round < 60; ++round) {
            for (const auto id : {1, 2, 3}) { cluster.Advance(id, 20ms); cluster.DeliverAll(); }
        }
        raft::NodeId leader = 0;
        for (const auto id : {1, 2, 3}) {
            if (cluster.nodes[id]->status().role == raft::Role::Leader) { Check(leader == 0, "Converge to one current leader"); leader = id; }
        }
        Check(leader != 0, "Cluster regains a leader");
        cluster.Put(leader, "final", "confirmed"); cluster.DeliverAll();
        const auto expected = cluster.nodes[leader]->CommittedEntries();
        for (const auto id : {1, 2, 3}) {
            const auto actual = cluster.nodes[id]->CommittedEntries();
            Check(actual.size() == expected.size(), "Fair delivery converges to the same committed prefix");
            std::map<std::string, std::string> model, reference;
            for (std::size_t i = 0; i < actual.size(); ++i) {
                Check(Equal(actual[i], expected[i]), "Every committed entry agrees after rejoin");
                const auto apply = [](auto& state, const raft::Command& command) {
                    if (command.type == raft::CommandType::Put) { state[command.key] = command.value; }
                    else if (command.type == raft::CommandType::Delete) { state.erase(command.key); }
                };
                apply(model, actual[i].command); apply(reference, expected[i].command);
            }
            Check(model == reference && model.at("final") == "confirmed", "Ordered committed commands produce the same independent map");
        }
    }
}
}  // namespace

int main() {
    try {
        TestLeaderOnlyAndInvalidProposals(); TestNoOpAndMajorityCommit(); TestCommittedCursorOwnsItsEntries();
        TestFollowerChecksPrefix(); TestDuplicatesAndShortRequestsRetainSuffix(); TestConflictReplacesOnlySuffix();
        TestCommittedAndSameTermConflictsStopNode(); TestMalformedAppendsAreIgnored();
        TestResponseCorrelationAndDuplicateRejection(); TestRetryPreservesRequestAndAcceptsDelayedReply();
        TestBatchedCatchupAndOldTermCommitRule(); TestCatchupByteBudget();
        TestMinorityCannotCommitAndRejoinRepairsConflict(); TestRestartReestablishesCommitWithoutNewClientCommand();
        TestPersistenceFailuresReleaseNoAcknowledgement(); TestProcessCrashAfterSyncBeforeOutput();
        TestMixedCommandsUnderMessageFaultsAndRestarts();
        std::cout << "All 17 Raft replication tests passed (2 persistence-boundary process crashes).\n";
    } catch (const std::exception& error) {
        std::cerr << "Raft replication test failed: " << error.what() << '\n'; return 1;
    }
}
