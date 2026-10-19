#pragma once

#include "kvstore/raft/node.h"
#include "raft_node.pb.h"

namespace kvstore::rpc {
constexpr int kMaxRpcBytes = 4 * 1024 * 1024;
wire::Envelope EncodeMessage(const raft::Message& message);
raft::Message DecodeMessage(const wire::Envelope& message);
raft::Command DecodeCommand(const wire::Command& command, bool allow_noop = true);
void EncodeCommand(const raft::Command& command, wire::Command* result);
void EncodeStatus(const raft::NodeStatus& status, wire::NodeStatus* result);
}  // namespace kvstore::rpc
