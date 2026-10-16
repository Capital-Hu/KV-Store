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
#include <limits>
#include <map>
#include <memory>
#include <random>
#include <set>
#include <stdexcept>
#include <string>
#include <vector>

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
using Milliseconds = std::chrono::milliseconds;
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

class TestDirectory {
public:
    TestDirectory() {
        char pattern[] = "/tmp/kvstore-election-XXXXXX";
        const auto path = ::mkdtemp(pattern);
        Check(path != nullptr, "Create election test directory");
        path_ = path;
    }
    ~TestDirectory() { std::error_code ignored; std::filesystem::remove_all(path_, ignored); }
    std::string Path(raft::NodeId id = 1) const { return (path_ / (std::to_string(id) + ".raft")).string(); }
private:
    std::filesystem::path path_;
};

raft::NodeConfig Config(raft::NodeId id) {
    raft::NodeConfig config{id, {}};
    for (raft::NodeId member = 1; member <= 3; ++member) { if (member != id) { config.peers.push_back(member); } }
    return config;
}

raft::ElectionOptions Options(Milliseconds timeout = 100ms) {
    return {20ms, timeout, timeout, 7};
}

std::unique_ptr<raft::Node> Open(const std::string& path, raft::NodeId id = 1,
                                raft::StorageOpenMode mode = kNew, raft::ElectionOptions options = Options()) {
    return std::make_unique<raft::Node>(Config(id), std::make_unique<raft::Storage>(path, Config(id), mode), options);
}

std::string Read(const std::string& path) {
    std::ifstream input(path, std::ios::binary);
    Check(input.is_open(), "Read election persistence fixture");
    return {std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
}

raft::Message Vote(raft::NodeId from, raft::NodeId to, raft::Term term, std::uint64_t rpc = 1,
                    raft::LogIndex last_index = 0, raft::Term last_term = 0) {
    return {from, to, rpc, raft::RequestVoteRequest{term, from, last_index, last_term}};
}

raft::Message Heartbeat(raft::NodeId from, raft::NodeId to, raft::Term term, std::uint64_t rpc = 1,
                         raft::LogIndex previous = 0, raft::Term previous_term = 0) {
    return {from, to, rpc, raft::AppendEntriesRequest{term, from, previous, previous_term, {}, 0}};
}

template <typename Body>
Body OnlyReply(raft::Node& node, raft::NodeId to, std::uint64_t rpc = 1) {
    const auto messages = node.DrainMessages();
    Check(messages.size() == 1 && messages[0].to == to && messages[0].rpc_id == rpc,
          "Reply preserves recipient and RPC correlation");
    return std::get<Body>(messages[0].body);
}

std::vector<raft::Message> Campaign(raft::Node& node) {
    node.AdvanceTime(node.status().election_timeout - node.status().election_elapsed);
    Check(node.status().role == raft::Role::Candidate, "Election timeout starts a campaign");
    auto requests = node.DrainMessages();
    Check(requests.size() == 2, "Campaign sends a request to each peer");
    return requests;
}

void Elect(raft::Node& node) {
    const auto requests = Campaign(node);
    node.Step({requests[0].to, requests[0].from, requests[0].rpc_id,
               raft::RequestVoteResponse{node.status().term, true}});
    Check(node.status().role == raft::Role::Leader, "Self-vote plus one distinct peer forms a majority");
    node.DrainMessages();
}

void SeedLog(const std::string& path, raft::Term term = 3) {
    raft::Storage storage(path, Config(1), kNew);
    storage.SetTermAndVote(term, std::nullopt);
    storage.Append({{1, 1, {raft::CommandType::Put, "a", "one"}},
                    {2, 2, {raft::CommandType::Put, "b", "two"}},
                    {3, 2, {raft::CommandType::Delete, "a", ""}}});
}

// Delivery is explicit: no real clocks, worker threads, or sockets.
class Cluster {
public:
    explicit Cluster(bool randomized = false) : randomized_(randomized) {
        for (raft::NodeId id = 1; id <= 3; ++id) { nodes[id] = Open(directory.Path(id), id, kNew, NodeOptions(id)); }
    }
    raft::ElectionOptions NodeOptions(raft::NodeId id) const {
        return randomized_ ? raft::ElectionOptions{20ms, 100ms, 200ms, 100 + id}
                           : Options(Milliseconds{100 + static_cast<Milliseconds::rep>(id - 1) * 40});
    }
    void Collect(raft::NodeId id) {
        for (auto& message : nodes.at(id)->DrainMessages()) { messages.push_back(std::move(message)); }
        RecordLeaders();
    }
    void Advance(raft::NodeId id, Milliseconds elapsed) { nodes.at(id)->AdvanceTime(elapsed); Collect(id); }
    void Deliver(std::size_t index = 0) {
        auto position = messages.begin() + static_cast<std::ptrdiff_t>(index);
        auto message = std::move(*position); messages.erase(position);
        if (isolated.count(message.from) || isolated.count(message.to)) { return; }
        nodes.at(message.to)->Step(message); Collect(message.to);
    }
    void DeliverAll() {
        std::size_t count = 0;
        while (!messages.empty()) { Check(++count <= 1000, "Message delivery reaches idle"); Deliver(); }
    }
    void Restart(raft::NodeId id) {
        nodes.at(id).reset();
        nodes.at(id) = Open(directory.Path(id), id, kExisting, NodeOptions(id));
        RecordLeaders();
    }
    void RecordLeaders() {
        for (const auto& pair : nodes) {
            const auto state = pair.second->status();
            Check(state.commit_index == 0 && state.last_applied == 0 && pair.second->persistent_state().log.empty(),
                  "Election-only simulation never creates or commits data");
            if (state.role != raft::Role::Leader) { continue; }
            const auto inserted = leaders.emplace(state.term, state.id);
            Check(inserted.second || inserted.first->second == state.id, "At most one elected leader per term across history");
        }
    }
    TestDirectory directory;
    std::map<raft::NodeId, std::unique_ptr<raft::Node>> nodes;
    std::deque<raft::Message> messages;
    std::set<raft::NodeId> isolated;
    std::map<raft::Term, raft::NodeId> leaders;
private:
    bool randomized_;
};

void TestElectionPersistsBeforeHeartbeats() {
    Cluster cluster;
    cluster.Advance(1, 99ms);
    Check(cluster.messages.empty() && cluster.nodes[1]->status().role == raft::Role::Follower, "No early election");
    cluster.Advance(1, 1ms);
    Check(cluster.nodes[1]->status().term == 1 && cluster.nodes[1]->persistent_state().voted_for == 1,
          "Campaign records its new term and self-vote");
    Check(cluster.messages.size() == 2, "Two vote requests await delivery");
    const auto& request = std::get<raft::RequestVoteRequest>(cluster.messages.front().body);
    Check(request.term == 1 && request.candidate_id == 1 && request.last_log_index == 0, "Campaign carries real log metadata");
    cluster.DeliverAll();
    Check(cluster.nodes[1]->status().role == raft::Role::Leader && cluster.nodes[1]->status().leader_id == 1,
          "Majority vote elects node one");
    for (const auto id : {2, 3}) {
        Check(cluster.nodes[id]->status().role == raft::Role::Follower && cluster.nodes[id]->status().leader_id == 1,
              "Initial heartbeats announce the elected leader");
        Check(cluster.nodes[id]->persistent_state().voted_for == 1, "Granted votes remain durable state");
    }
    cluster.Advance(1, 19ms);
    Check(cluster.messages.empty(), "Leader waits for its heartbeat interval");
    cluster.Advance(1, 1ms);
    Check(cluster.messages.size() == 2 && std::holds_alternative<raft::AppendEntriesRequest>(cluster.messages.front().body),
          "Heartbeat interval sends an empty AppendEntries to each peer");
    cluster.DeliverAll();
}

void TestHeartbeatsPreventElections() {
    Cluster cluster;
    cluster.Advance(1, 100ms); cluster.DeliverAll();
    for (int round = 0; round < 30; ++round) {
        for (const auto id : {2, 3}) { cluster.Advance(id, 20ms); }
        cluster.Advance(1, 20ms); cluster.DeliverAll();
        for (const auto id : {1, 2, 3}) { Check(cluster.nodes[id]->status().term == 1, "Heartbeats keep the original term"); }
    }
    Check(cluster.nodes[1]->status().role == raft::Role::Leader, "Idle leader remains elected");
}

void TestSplitVoteAndRetry() {
    Cluster cluster;
    cluster.Advance(1, 100ms); cluster.Advance(2, 140ms); cluster.Advance(3, 180ms);
    cluster.DeliverAll();
    for (const auto id : {1, 2, 3}) { Check(cluster.nodes[id]->status().role == raft::Role::Candidate, "Self-votes split term one"); }
    cluster.Advance(2, 140ms); cluster.DeliverAll();
    Check(cluster.nodes[2]->status().role == raft::Role::Leader && cluster.nodes[2]->status().term == 2,
          "A later timeout retries with a new term and wins");
}

void TestLeaderIsolationAndRejoin() {
    Cluster cluster;
    cluster.Advance(1, 100ms); cluster.DeliverAll();
    cluster.isolated.insert(1);
    cluster.Advance(2, 140ms); cluster.DeliverAll();
    Check(cluster.nodes[2]->status().role == raft::Role::Leader && cluster.nodes[2]->status().term == 2,
          "Remaining majority elects another leader");
    Check(cluster.nodes[1]->status().role == raft::Role::Leader && cluster.nodes[1]->status().term == 1,
          "Isolated old leader may still believe its older term is current");
    cluster.isolated.clear();
    cluster.Advance(2, 20ms); cluster.DeliverAll();
    Check(cluster.nodes[1]->status().role == raft::Role::Follower && cluster.nodes[1]->status().term == 2 &&
          cluster.nodes[1]->status().leader_id == 2, "Higher-term heartbeat demotes the rejoining old leader");
}

void TestWrongStaleAndDuplicateReplies() {
    TestDirectory directory;
    auto node = Open(directory.Path());
    const auto requests = Campaign(*node);
    const auto rpc = requests[0].rpc_id;
    const auto peer = requests[0].to;
    for (const auto& message : std::vector<raft::Message>{
             {peer, 1, rpc + 999, raft::RequestVoteResponse{1, true}},
             {peer, 1, rpc, raft::RequestVoteResponse{0, true}},
             {peer, 1, rpc, raft::RequestVoteResponse{1, false}},
             {1, 1, rpc, raft::RequestVoteResponse{1, true}},
             {99, 1, rpc, raft::RequestVoteResponse{9, true}}}) {
        node->Step(message);
        Check(node->status().role == raft::Role::Candidate && node->status().term == 1, "Invalid votes cannot manufacture a majority");
    }
    node->Step({peer, 1, rpc, raft::RequestVoteResponse{1, true}});
    Check(node->status().role == raft::Role::Leader, "One matching peer vote plus self is enough");
    Check(node->DrainMessages().size() == 2, "Leadership transition sends initial heartbeats once");
    node->Step({peer, 1, rpc, raft::RequestVoteResponse{1, true}});
    Check(node->DrainMessages().empty(), "Duplicate reply does not repeat the leadership transition");
    auto pending = Open(directory.Path(2), 2);
    const auto old = Campaign(*pending);
    Campaign(*pending);
    pending->Step({old[0].to, 2, old[0].rpc_id, raft::RequestVoteResponse{1, true}});
    Check(pending->status().role == raft::Role::Candidate && pending->status().term == 2, "Older-round replies cannot elect a new-round candidate");
}

void TestHigherTermsInEveryRpcStepDown() {
    const std::vector<raft::Message> messages{
        Vote(2, 1, 2), {2, 1, 999, raft::RequestVoteResponse{2, false}},
        Heartbeat(2, 1, 2), {2, 1, 999, raft::AppendEntriesResponse{2, false}}};
    for (const auto& message : messages) {
        TestDirectory directory;
        {
            auto node = Open(directory.Path()); Elect(*node);
            node->Step(message);
            Check(node->status().role == raft::Role::Follower && node->status().term == 2,
                  "Every RPC type can report a newer term, including unmatched responses");
        }
        raft::Storage disk(directory.Path(), Config(1), kExisting);
        Check(disk.state().current_term == 2, "Observed term is durable before further communication");
    }
}

void TestOldRequestsDoNotResetElection() {
    TestDirectory directory;
    { raft::Storage disk(directory.Path(), Config(1), kNew); disk.SetTermAndVote(2, std::nullopt); }
    auto node = Open(directory.Path(), 1, kExisting);
    node->AdvanceTime(90ms);
    node->Step(Vote(2, 1, 1));
    const auto vote = OnlyReply<raft::RequestVoteResponse>(*node, 2);
    Check(!vote.vote_granted && vote.term == 2, "Old vote request is rejected with the current term");
    node->Step(Heartbeat(2, 1, 1));
    const auto heartbeat = OnlyReply<raft::AppendEntriesResponse>(*node, 2);
    Check(!heartbeat.success && heartbeat.term == 2, "Old heartbeat is rejected");
    Check(node->status().election_elapsed == 90ms && !node->status().leader_id, "Old requests do not refresh leader authority");
    node->AdvanceTime(10ms);
    Check(node->status().role == raft::Role::Candidate && node->status().term == 3, "Stale traffic cannot defer an election");
}

void TestVotingResetsTimerAndRefusesSecondVote() {
    TestDirectory directory;
    auto node = Open(directory.Path());
    node->AdvanceTime(90ms); node->Step(Vote(2, 1, 1));
    Check(OnlyReply<raft::RequestVoteResponse>(*node, 2).vote_granted, "Grant the first fresh candidate");
    Check(node->status().election_elapsed == 0ms && node->persistent_state().voted_for == 2, "Granting a vote resets the timer after persistence");
    const auto bytes = Read(directory.Path());
    node->Step(Vote(2, 1, 1, 2));
    Check(OnlyReply<raft::RequestVoteResponse>(*node, 2, 2).vote_granted && Read(directory.Path()) == bytes,
          "Repeated request for the same candidate is idempotent");
    node->AdvanceTime(90ms); node->Step(Vote(3, 1, 1));
    Check(!OnlyReply<raft::RequestVoteResponse>(*node, 3).vote_granted && node->status().election_elapsed == 90ms,
          "Another candidate cannot get a second vote or reset the timer");
    node->AdvanceTime(10ms);
    Check(node->status().term == 2 && node->persistent_state().voted_for == 1, "A later local campaign uses a new term");
}

void TestHigherTermRejectedVoteKeepsTimer() {
    TestDirectory directory; SeedLog(directory.Path());
    auto node = Open(directory.Path(), 1, kExisting);
    node->AdvanceTime(90ms); node->Step(Vote(2, 1, 4));
    Check(!OnlyReply<raft::RequestVoteResponse>(*node, 2).vote_granted, "Higher term alone does not make an empty log fresh");
    Check(node->status().term == 4 && node->status().election_elapsed == 90ms && !node->persistent_state().voted_for,
          "Observe the higher term without rewarding an outdated candidate with a timer reset");
    node->AdvanceTime(10ms);
    Check(node->status().term == 5 && node->status().role == raft::Role::Candidate, "Outdated candidates cannot suppress progress");
}

void TestLogFreshnessUsesTermThenIndex() {
    struct Case { raft::LogIndex index; raft::Term term; bool grant; };
    for (const auto test : {Case{100, 1, false}, Case{2, 2, false}, Case{3, 2, true}, Case{9, 2, true}, Case{1, 3, true}}) {
        TestDirectory directory; SeedLog(directory.Path());
        auto node = Open(directory.Path(), 1, kExisting);
        node->Step(Vote(2, 1, 4, 1, test.index, test.term));
        Check(OnlyReply<raft::RequestVoteResponse>(*node, 2).vote_granted == test.grant,
              "Last-log term wins first; only equal terms compare last-log index");
        Check(node->status().term == 4 && node->status().last_log_index == 3, "Voting never rewrites the local log");
    }
}

void TestCandidateFollowsSameTermHeartbeat() {
    TestDirectory directory; auto node = Open(directory.Path());
    const auto requests = Campaign(*node);
    node->Step(Heartbeat(2, 1, 1));
    Check(OnlyReply<raft::AppendEntriesResponse>(*node, 2).success, "Matching same-term heartbeat succeeds");
    Check(node->status().role == raft::Role::Follower && node->status().leader_id == 2 &&
          node->persistent_state().voted_for == 1, "Candidate follows leader without erasing its same-term vote");
    node->Step({requests[0].to, 1, requests[0].rpc_id, raft::RequestVoteResponse{1, true}});
    Check(node->status().role == raft::Role::Follower && node->DrainMessages().empty(), "Old campaign response cannot revive a follower");
}

void TestHeartbeatPrefixChecksAndScope() {
    TestDirectory directory; SeedLog(directory.Path());
    auto node = Open(directory.Path(), 1, kExisting);
    const auto bytes = Read(directory.Path());
    struct Case { raft::LogIndex index; raft::Term term; bool match; };
    for (const auto test : {Case{2, 2, true}, Case{3, 1, false}, Case{9, 2, false}, Case{0, 0, true}}) {
        node->AdvanceTime(90ms); node->Step(Heartbeat(2, 1, 3, 1, test.index, test.term));
        Check(OnlyReply<raft::AppendEntriesResponse>(*node, 2).success == test.match, "Heartbeat checks its complete prefix metadata");
        Check(node->status().election_elapsed == 0ms && node->status().leader_id == 2, "Current leader traffic refreshes the timer even while logs differ");
    }
    auto append = Heartbeat(2, 1, 3, 7, 2, 2);
    std::get<raft::AppendEntriesRequest>(append.body).entries.push_back({3, 3, {raft::CommandType::Put, "x", "y"}});
    node->Step(append);
    Check(!OnlyReply<raft::AppendEntriesResponse>(*node, 2, 7).success, "Unimplemented data append is never acknowledged");
    auto commit = Heartbeat(2, 1, 3, 8, 2, 2);
    std::get<raft::AppendEntriesRequest>(commit.body).leader_commit = 2;
    node->Step(commit);
    Check(!OnlyReply<raft::AppendEntriesResponse>(*node, 2, 8).success, "Unimplemented commit progress is never acknowledged");
    Check(Read(directory.Path()) == bytes && node->persistent_state().log.size() == 3 && node->status().commit_index == 0,
          "Empty heartbeat preserves any extra follower suffix and does not commit data");
}

void TestInvalidMessagesAreIgnored() {
    TestDirectory directory; auto node = Open(directory.Path());
    node->AdvanceTime(90ms);
    const auto bytes = Read(directory.Path());
    std::vector<raft::Message> invalid{Vote(99, 1, 9), Vote(1, 1, 9), Vote(2, 3, 9), Vote(2, 1, 9, 0),
                                      Vote(2, 1, 9, 1, 0, 1), Vote(2, 1, 9, 1, 1, 10),
                                      Heartbeat(2, 1, 9, 1, 0, 1), Heartbeat(2, 1, 0)};
    auto mismatched = Vote(2, 1, 9); std::get<raft::RequestVoteRequest>(mismatched.body).candidate_id = 3;
    invalid.push_back(mismatched);
    mismatched = Heartbeat(2, 1, 9); std::get<raft::AppendEntriesRequest>(mismatched.body).leader_id = 3;
    invalid.push_back(mismatched);
    for (const auto& message : invalid) {
        node->Step(message);
        Check(node->status().term == 0 && node->status().election_elapsed == 90ms && node->DrainMessages().empty(),
              "Invalid identity/routing/metadata cannot modify the election");
    }
    Check(Read(directory.Path()) == bytes, "Invalid input does not change durable state");
}

void TestOptionsAndPassiveModelGuard() {
    for (const auto options : {raft::ElectionOptions{0ms, 100ms, 200ms, 1},
                               raft::ElectionOptions{100ms, 100ms, 200ms, 1},
                               raft::ElectionOptions{20ms, 200ms, 100ms, 1}}) {
        TestDirectory directory;
        Reject([&] { auto node = Open(directory.Path(), 1, kNew, options); }, "Reject invalid election timing configuration");
    }
    raft::Node passive(Config(1));
    Reject([&] { passive.Step(Vote(2, 1, 1)); }, "Protocol handling requires durable storage");
    passive.AdvanceTime(1h);
    Check(passive.usable() && passive.status().term == 0 && passive.DrainMessages().empty(), "Passive model keeps its original clock-only contract");
    TestDirectory directory;
    const raft::NodeConfig config{10, {20, 30}};
    raft::Node arbitrary(config, std::make_unique<raft::Storage>(directory.Path(10), config, kNew), Options());
    Elect(arbitrary);
    Check(arbitrary.status().leader_id == 10, "Protocol uses configured IDs rather than hard-coded 1/2/3");
}

void TestRandomTimeoutsAreRepeatableAndVaried() {
    TestDirectory first_dir, second_dir;
    const raft::ElectionOptions options{20ms, 100ms, 200ms, 42};
    auto first = Open(first_dir.Path(), 1, kNew, options), second = Open(second_dir.Path(), 1, kNew, options);
    std::set<Milliseconds::rep> seen;
    for (int round = 0; round < 12; ++round) {
        const auto timeout = first->status().election_timeout;
        Check(timeout >= 100ms && timeout <= 200ms && timeout == second->status().election_timeout,
              "Seeded election timeouts are bounded and reproducible");
        seen.insert(timeout.count());
        Campaign(*first); Campaign(*second);
    }
    Check(seen.size() > 1, "New campaigns redraw their election timeouts");
}

void TestTimeAndTermBoundaries() {
    TestDirectory directory; auto node = Open(directory.Path());
    node->AdvanceTime(Milliseconds::max());
    Check(node->status().term == 1 && node->DrainMessages().size() == 2, "Large clock jump coalesces to one timeout event");
    const auto bytes = Read(directory.Path());
    Reject([&] { node->AdvanceTime(1ms); }, "Reject logical clock overflow");
    Reject([&] { node->AdvanceTime(-1ms); }, "Reject negative clock advance");
    node->AdvanceTime(0ms);
    Check(node->usable() && node->status().term == 1 && Read(directory.Path()) == bytes, "Invalid clock input leaves protocol usable and unchanged");
    const auto path = directory.Path(2);
    { raft::Storage storage(path, Config(2), kNew); storage.SetTermAndVote(std::numeric_limits<raft::Term>::max(), std::nullopt); }
    {
        auto exhausted = Open(path, 2, kExisting);
        Reject([&] { exhausted->AdvanceTime(100ms); }, "Term must not wrap at its limit");
        Check(!exhausted->usable(), "Exhausted protocol stops rather than sending a wrapped-term vote");
    }
    raft::Storage storage(path, Config(2), kExisting);
    Check(storage.state().current_term == std::numeric_limits<raft::Term>::max(), "Durable term never wraps");
}

void TestVotesPersistAcrossRestart() {
    TestDirectory directory;
    {
        auto node = Open(directory.Path()); node->Step(Vote(2, 1, 7));
        Check(OnlyReply<raft::RequestVoteResponse>(*node, 2).vote_granted, "First request gets a durable vote");
    }
    {
        auto node = Open(directory.Path(), 1, kExisting);
        Check(node->status().term == 7 && node->persistent_state().voted_for == 2 && !node->status().leader_id,
              "Restart preserves vote but not an invented leader");
        node->Step(Vote(3, 1, 7));
        Check(!OnlyReply<raft::RequestVoteResponse>(*node, 3).vote_granted, "Restart does not enable a second vote");
        Campaign(*node);
        Check(node->status().term == 8 && node->persistent_state().voted_for == 1, "Restarted node can campaign in the next term");
    }
    raft::Storage disk(directory.Path(), Config(1), kExisting);
    Check(disk.state().current_term == 8 && disk.state().voted_for == 1, "Campaign term and self-vote survive another restart");
}

void TestFailedPersistenceStopsReplies() {
    for (const auto failure : {Stage::RecordWritten, Stage::RecordSynced, Stage::StateUpdated}) {
        for (const int action : {0, 1, 2}) {
            TestDirectory directory;
            auto storage = std::make_unique<raft::Storage>(directory.Path(), Config(1), kNew);
            int records = 0;
            raft::StorageTestAccess::Hook(*storage, [&](Stage stage) {
                if (stage == Stage::RecordWritten) { ++records; }
                if (records == (action == 1 ? 2 : 1) && stage == failure) { throw std::runtime_error("Injected election persistence failure"); }
            });
            {
                raft::Node node(Config(1), std::move(storage), Options());
                Reject([&] {
                    if (action == 0) { node.AdvanceTime(100ms); }
                    else if (action == 1) { node.Step(Vote(2, 1, 1)); }
                    else { node.Step({2, 1, 99, raft::AppendEntriesResponse{2, false}}); }
                }, "Persistence failure propagates out of the event");
                Check(!node.usable(), "Uncertain persistence stops the protocol");
                Reject([&] { node.DrainMessages(); }, "Failed protocol cannot release any reply or vote request");
                Reject([&] { node.status(); }, "Failed protocol cannot report stale state");
                Reject([&] { node.AdvanceTime(100ms); }, "Failed protocol cannot start another election");
                Reject([&] { node.Step(Vote(3, 1, 3)); }, "Failed protocol cannot vote again");
            }
            raft::Storage reopened(directory.Path(), Config(1), kExisting);
            Check(reopened.state().current_term == (action == 2 ? 2U : 1U), "Complete unacknowledged hard-state write may survive failure");
            Check(reopened.state().voted_for == (action == 2 ? std::nullopt : std::optional<raft::NodeId>(action == 0 ? 1 : 2)),
                  "Restart reconstructs the complete persisted vote");
        }
    }
}

void TestKilledAfterSyncBeforeOutbound() {
    for (const bool campaign : {false, true}) {
        TestDirectory directory;
        { raft::Storage storage(directory.Path(), Config(1), kNew); storage.SetTermAndVote(1, std::nullopt); }
        const auto pid = ::fork();
        Check(pid != -1, "Fork election crash child");
        if (pid == 0) {
            ::alarm(15);
            try {
                auto storage = std::make_unique<raft::Storage>(directory.Path(), Config(1), kExisting);
                raft::StorageTestAccess::Hook(*storage, [](Stage stage) {
                    if (stage == Stage::RecordSynced) { ::kill(::getpid(), SIGKILL); for (;;) { ::pause(); } }
                });
                raft::Node node(Config(1), std::move(storage), Options());
                if (campaign) { node.AdvanceTime(100ms); } else { node.Step(Vote(2, 1, 1)); }
                ::_exit(4);
            } catch (...) { ::_exit(2); }
        }
        struct ReapOnExit {
            pid_t pid;
            ~ReapOnExit() {
                if (pid > 0) { ::kill(pid, SIGKILL); while (::waitpid(pid, nullptr, 0) == -1 && errno == EINTR) {} }
            }
        } guard{pid};
        int status = 0; pid_t result;
        do { result = ::waitpid(pid, &status, 0); } while (result == -1 && errno == EINTR);
        if (result == pid) { guard.pid = -1; }
        Check(result == pid && WIFSIGNALED(status) && WTERMSIG(status) == SIGKILL, "Child stops after sync before sending protocol output");
        auto node = Open(directory.Path(), 1, kExisting);
        const auto term = campaign ? 2U : 1U;
        Check(node->status().term == term && node->persistent_state().voted_for == (campaign ? 1U : 2U),
              "Synced vote survives even when the request or reply never became sendable");
        node->Step(Vote(3, 1, term));
        Check(!OnlyReply<raft::RequestVoteResponse>(*node, 3).vote_granted, "Restart cannot double-vote after crash before outbound");
    }
}

void TestReorderedDuplicatedDroppedMessagesAndRestarts() {
    for (const unsigned int seed : {7U, 99U, 20261015U}) {
        Cluster cluster(true); std::mt19937 random(seed);
        for (int event = 0; event < 450; ++event) {
            const auto action = random() % 10;
            const raft::NodeId id = 1 + random() % 3;
            if (action < 5) { cluster.Advance(id, Milliseconds{1 + random() % 40}); }
            else if (action == 9) { cluster.Restart(id); }
            else if (!cluster.messages.empty()) {
                const auto index = random() % cluster.messages.size();
                if (action == 5) { cluster.messages.erase(cluster.messages.begin() + index); }
                else {
                    if (action == 6 && cluster.messages.size() < 300) { cluster.messages.push_back(cluster.messages[index]); }
                    cluster.Deliver(index);
                }
            }
            cluster.RecordLeaders();
        }
        cluster.messages.clear();
        for (int round = 0; round < 50; ++round) {
            for (const auto id : {1, 2, 3}) { cluster.Advance(id, 20ms); cluster.DeliverAll(); }
        }
        raft::NodeId leader = 0;
        for (const auto id : {1, 2, 3}) {
            if (cluster.nodes[id]->status().role == raft::Role::Leader) {
                Check(leader == 0, "Fair connected delivery converges to a single current leader"); leader = id;
            }
        }
        Check(leader != 0 && !cluster.leaders.empty(), "Cluster remains able to elect after message faults and restarts");
        for (const auto id : {1, 2, 3}) {
            Check(cluster.nodes[id]->status().leader_id == leader && cluster.nodes[id]->status().term == cluster.nodes[leader]->status().term,
                  "All nodes learn the same live leader after communication recovers");
        }
    }
}
}  // namespace

int main() {
    try {
        TestElectionPersistsBeforeHeartbeats(); TestHeartbeatsPreventElections(); TestSplitVoteAndRetry();
        TestLeaderIsolationAndRejoin(); TestWrongStaleAndDuplicateReplies(); TestHigherTermsInEveryRpcStepDown();
        TestOldRequestsDoNotResetElection(); TestVotingResetsTimerAndRefusesSecondVote(); TestHigherTermRejectedVoteKeepsTimer();
        TestLogFreshnessUsesTermThenIndex(); TestCandidateFollowsSameTermHeartbeat(); TestHeartbeatPrefixChecksAndScope();
        TestInvalidMessagesAreIgnored(); TestOptionsAndPassiveModelGuard(); TestRandomTimeoutsAreRepeatableAndVaried();
        TestTimeAndTermBoundaries(); TestVotesPersistAcrossRestart(); TestFailedPersistenceStopsReplies();
        TestKilledAfterSyncBeforeOutbound(); TestReorderedDuplicatedDroppedMessagesAndRestarts();
        std::cout << "All 20 Raft election tests passed (2 persistence-boundary process crashes).\n";
    } catch (const std::exception& error) {
        std::cerr << "Raft election test failed: " << error.what() << '\n';
        return 1;
    }
    return 0;
}
