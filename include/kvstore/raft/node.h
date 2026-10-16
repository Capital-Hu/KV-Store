#pragma once

#include "kvstore/raft/types.h"

#include <chrono>
#include <cstddef>
#include <memory>

namespace kvstore::raft {

class Storage;

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
};

// Single-threaded protocol model. Its caller will serialize events.
// An optional Storage restores durable state; no Store, threads, or election logic.
class Node {
public:
    explicit Node(NodeConfig config);
    Node(NodeConfig config, std::unique_ptr<Storage> storage);
    ~Node();
    Node(const Node&) = delete;
    Node& operator=(const Node&) = delete;

    const NodeConfig& config() const { return config_; }
    const PersistentState& persistent_state() const;
    std::size_t majority_size() const { return (config_.peers.size() + 1) / 2 + 1; }
    NodeStatus status() const;

    // Advance only logical time. Election timers will consume it in a later step.
    // Negative or overflowing advances throw without changing the node.
    void AdvanceTime(std::chrono::milliseconds elapsed);

private:
    NodeConfig config_;
    PersistentState initial_;
    std::unique_ptr<Storage> storage_;
    Role role_ = Role::Follower;
    std::optional<NodeId> leader_id_;
    LogIndex commit_index_ = 0;
    LogIndex last_applied_ = 0;
    std::chrono::milliseconds logical_time_{0};
};

}  // namespace kvstore::raft
