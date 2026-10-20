#pragma once

#include "kvstore/raft/types.h"

#include <chrono>
#include <cstddef>
#include <memory>
#include <random>

namespace kvstore::raft {

class Storage;

struct ElectionOptions {
    std::chrono::milliseconds heartbeat_interval{50};
    std::chrono::milliseconds election_timeout_min{150};
    std::chrono::milliseconds election_timeout_max{300};
    std::optional<std::uint64_t> random_seed;  // Set in tests for repeatable timeouts.
};

// Acceptance into the leader's durable log is not a commit acknowledgement.
struct Proposal {
    LogIndex index;
    Term term;
};

// An owned observation of the node, not a writable reference to its state.
struct NodeStatus {
    NodeId id = 0;
    Role role = Role::Follower;
    Term term = 0;
    std::optional<NodeId> leader_id;
    LogIndex last_log_index = 0;
    Term last_log_term = 0;
    LogIndex commit_index = 0;
    LogIndex last_applied = 0;
    std::chrono::milliseconds logical_time{0};
    std::chrono::milliseconds election_elapsed{0};
    std::chrono::milliseconds election_timeout{0};
    std::chrono::milliseconds heartbeat_elapsed{0};
};

// Single-threaded protocol model. Its caller will serialize events.
// Storage-backed nodes run elections; the constructor without Storage stays passive.
// State-machine I/O belongs to its caller; no Store, network, or worker threads.
class Node {
public:
    explicit Node(NodeConfig config);
    // recovered_commit must come from validated durable commit metadata, never log length.
    Node(NodeConfig config, std::unique_ptr<Storage> storage, ElectionOptions options = {},
         LogIndex recovered_commit = 0);
    ~Node();
    Node(const Node&) = delete;
    Node& operator=(const Node&) = delete;

    const NodeConfig& config() const { return config_; }
    const PersistentState& persistent_state() const;
    bool usable() const;
    std::size_t majority_size() const { return (config_.peers.size() + 1) / 2 + 1; }
    NodeStatus status() const;

    // A time jump emits at most one timeout event, without replaying missed intervals.
    // Negative or overflowing clock advances throw before changing the node.
    void AdvanceTime(std::chrono::milliseconds elapsed);
    void Step(const Message& message);
    std::vector<Message> DrainMessages();
    Proposal Propose(Command command);
    // Append a fresh NoOp whose majority commitment can fence a client read.
    Proposal ProposeReadBarrier();
    // Owned committed prefix after the caller's cursor; this does not apply entries.
    std::vector<LogEntry> CommittedEntries(LogIndex after = 0) const;
    // Acknowledge exactly the next entry, only after successful state-machine application.
    void MarkApplied(LogIndex index);

private:
    struct PendingAppend {
        std::uint64_t rpc_id;
        AppendEntriesRequest request;
    };
    struct Peer {
        NodeId id;
        std::uint64_t vote_rpc_id = 0;
        bool vote_granted = false;
        LogIndex next_index = 1;
        LogIndex match_index = 0;
        LogIndex acknowledged_commit = 0;
        std::optional<PendingAppend> pending = std::nullopt;
    };
    void EnsureUsable() const;
    void Fail();
    void ResetElectionTimer();
    void ClearElection();
    void ObserveHigherTerm(Term term);
    void StartElection();
    void BecomeLeader();
    void SendHeartbeats();
    void SendAppend(Peer& peer, bool retry = false);
    void AdvanceCommit();
    Proposal AppendProposal(Command command);
    std::uint64_t NextRpcId();
    void Handle(const Message& message, const RequestVoteRequest& request);
    void Handle(const Message& message, const RequestVoteResponse& response);
    void Handle(const Message& message, const AppendEntriesRequest& request);
    void Handle(const Message& message, const AppendEntriesResponse& response);

    NodeConfig config_;
    PersistentState initial_;
    std::unique_ptr<Storage> storage_;
    Role role_ = Role::Follower;
    std::optional<NodeId> leader_id_;
    LogIndex commit_index_ = 0;
    LogIndex last_applied_ = 0;
    std::chrono::milliseconds logical_time_{0};
    ElectionOptions options_;
    std::mt19937_64 random_;
    std::chrono::milliseconds election_elapsed_{0};
    std::chrono::milliseconds election_timeout_{0};
    std::chrono::milliseconds heartbeat_elapsed_{0};
    std::vector<Peer> peers_;
    std::vector<Message> outbox_;
    std::uint64_t last_rpc_id_ = 0;
    bool usable_ = true;
};

}  // namespace kvstore::raft
