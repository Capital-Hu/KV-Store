#pragma once

#include "kvstore/rpc/node_server.h"

namespace kvstore::rpc {

enum class ClientCode { Ok, Unavailable, Unknown, Rejected };

struct ClientResult {
    ClientCode code = ClientCode::Unavailable;
    std::optional<std::string> value;
    std::optional<raft::Proposal> proposal;
    std::string message;
};

struct ClientOptions {
    std::vector<Member> members;
    std::chrono::milliseconds timeout{5000};
    std::chrono::milliseconds rpc_timeout{1500};
};

// Calls have independent state and may run concurrently. Never retries an ambiguous write.
// Successful Get uses a fresh committed read barrier; missing keys return Ok + nullopt.
class Client {
public:
    explicit Client(ClientOptions options);
    ~Client();
    Client(const Client&) = delete;
    Client& operator=(const Client&) = delete;
    ClientResult Put(const std::string& key, const std::string& value) const;
    ClientResult Delete(const std::string& key) const;
    ClientResult Get(const std::string& key) const;
private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace kvstore::rpc
