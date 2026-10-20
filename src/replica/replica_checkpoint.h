#pragma once

#include "kvstore/raft/types.h"

#include <string_view>

namespace kvstore::detail {

struct ReplicaCheckpoint {
    raft::LogIndex index = 0;
    raft::Term term = 0;
    std::uint32_t fingerprint = 0;
};

constexpr std::size_t kReplicaCheckpointBytes = 52;
std::string EncodeReplicaCheckpoint(const raft::NodeConfig& config, const ReplicaCheckpoint& checkpoint);
ReplicaCheckpoint DecodeReplicaCheckpoint(std::string_view bytes, const raft::NodeConfig& config);
std::uint32_t ExtendReplicaFingerprint(std::uint32_t previous, const raft::LogEntry& entry);

}  // namespace kvstore::detail
