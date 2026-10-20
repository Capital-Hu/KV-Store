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

// These fields will require durable storage before the relevant RPC replies.
// Storage persists these independently of the KV engine's WAL.
struct PersistentState {
    Term current_term = 0;
    std::optional<NodeId> voted_for;
    std::vector<LogEntry> log;
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

using MessageBody = std::variant<RequestVoteRequest, RequestVoteResponse,
                                 AppendEntriesRequest, AppendEntriesResponse>;

// Responses echo the request's rpc_id; transport supplies trusted peer identity.
struct Message {
    NodeId from = 0;
    NodeId to = 0;
    std::uint64_t rpc_id = 0;
    MessageBody body;
};

}  // namespace kvstore::raft
