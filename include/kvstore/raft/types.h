#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <variant>
#include <vector>

namespace kvstore::raft {

using NodeId = std::uint64_t;
using Term = std::uint64_t;
using LogIndex = std::uint64_t;

constexpr bool ValidMemberCount(std::size_t count) { return count > 0 && count % 2 == 1; }

// Static odd-sized membership. Peers exclude this node; IDs must be nonzero.
struct NodeConfig {
    NodeId id = 0;
    std::vector<NodeId> peers;
};

enum class Role { Follower, Candidate, Leader };
enum class CommandType { Put, Delete, NoOp };

// Owned bytes, independent of the storage engine and any network encoding.
struct Command {
    CommandType type = CommandType::Put;
    std::string key;
    std::string value;  // Empty for Delete; NoOp has empty key and value.
};

struct LogEntry {
    LogIndex index = 0;  // Real entries start at 1; 0 denotes the empty prefix.
    Term term = 0;
    Command command;
};

// Identifies a committed prefix: its last index, that entry's term and the
// coordinator's opaque checksum of the prefix. Index 0 denotes the empty prefix.
struct SnapshotInfo {
    LogIndex index = 0;
    Term term = 0;
    std::uint32_t fingerprint = 0;
};

// These fields will require durable storage before the relevant RPC replies.
// Storage persists these independently of the KV engine's WAL.
// log holds only entries after base, so log[0] has index base.index + 1.
struct PersistentState {
    Term current_term = 0;
    std::optional<NodeId> voted_for;
    std::vector<LogEntry> log;
    SnapshotInfo base;

    LogIndex last_index() const { return base.index + log.size(); }
    const LogEntry& EntryAt(LogIndex index) const { return log[static_cast<std::size_t>(index - base.index - 1)]; }
    // Valid for base.index through last_index().
    Term TermAt(LogIndex index) const { return index == base.index ? base.term : EntryAt(index).term; }
};

// In-memory RPC values; the network encoding is a later step.
struct RequestVoteRequest {
    Term term = 0;
    NodeId candidate_id = 0;
    LogIndex last_log_index = 0;
    Term last_log_term = 0;
};

struct RequestVoteResponse {
    Term term = 0;
    bool vote_granted = false;
};

struct AppendEntriesRequest {
    Term term = 0;
    NodeId leader_id = 0;
    LogIndex prev_log_index = 0;
    Term prev_log_term = 0;
    std::vector<LogEntry> entries;  // Empty for a heartbeat; prefix checks still apply.
    LogIndex leader_commit = 0;
};

struct AppendEntriesResponse {
    Term term = 0;
    bool success = false;
};

// State-machine files travel separately; this message asks the follower to adopt them.
struct InstallSnapshotRequest {
    Term term = 0;
    NodeId leader_id = 0;
    SnapshotInfo snapshot;
};

struct InstallSnapshotResponse {
    Term term = 0;
    bool success = false;
};

using MessageBody = std::variant<RequestVoteRequest, RequestVoteResponse,
                                 AppendEntriesRequest, AppendEntriesResponse,
                                 InstallSnapshotRequest, InstallSnapshotResponse>;

// Responses echo the request's rpc_id; transport supplies trusted peer identity.
struct Message {
    NodeId from = 0;
    NodeId to = 0;
    std::uint64_t rpc_id = 0;
    MessageBody body;
};

}  // namespace kvstore::raft
