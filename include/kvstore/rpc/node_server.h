#pragma once

#include "kvstore/replicated_store.h"

namespace kvstore::rpc {

struct Member { raft::NodeId id; std::string address; };
std::uint64_t ParseUnsigned(const std::string& text);
void ValidateAddress(const std::string& address);
std::vector<Member> ParseMembers(const std::string& text);

struct ServerOptions {
    ServerOptions();
    raft::NodeId id = 0;
    std::vector<Member> members;
    // Optional outbound Raft endpoints, keyed by the same member IDs (e.g. test proxies).
    // Listening and client/admin addresses continue to come from members.
    std::vector<Member> raft_routes;
    std::string directory;
    raft::StorageOpenMode mode = raft::StorageOpenMode::OpenExisting;
    ReplicatedStoreOptions replica;
    std::chrono::milliseconds tick_interval{10};
    std::chrono::milliseconds rpc_timeout{150};
    std::chrono::milliseconds client_wait_timeout{1000};
};

// Local, trusted static odd-sized transport. Internally serializes all Replica calls.
// Lifecycle operations belong to the owning thread; Stop is idempotent.
class NodeServer {
public:
    explicit NodeServer(ServerOptions options);
    ~NodeServer();
    NodeServer(const NodeServer&) = delete;
    NodeServer& operator=(const NodeServer&) = delete;
    void Stop();
    bool failed() const;
private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace kvstore::rpc
