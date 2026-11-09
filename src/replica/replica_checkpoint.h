#pragma once

#include "kvstore/raft/types.h"

#include <string_view>

namespace kvstore::detail {

struct ReplicaCheckpoint {
    raft::LogIndex index = 0;
    raft::Term term = 0;
    std::uint32_t fingerprint = 0;
};

std::size_t ReplicaCheckpointBytes(const raft::NodeConfig& config);
std::string EncodeReplicaCheckpoint(const raft::NodeConfig& config, const ReplicaCheckpoint& checkpoint);
ReplicaCheckpoint DecodeReplicaCheckpoint(std::string_view bytes, const raft::NodeConfig& config);
struct SnapshotMeta {
    raft::SnapshotInfo info;
    std::uint32_t file_count = 0;
};
constexpr std::size_t kSnapshotMetaBytes = 32;
constexpr std::uint32_t kMaxSnapshotFiles = 100000;
std::string EncodeSnapshotMeta(const SnapshotMeta& meta);
SnapshotMeta DecodeSnapshotMeta(std::string_view bytes);

std::uint32_t ExtendReplicaFingerprint(std::uint32_t previous, const raft::LogEntry& entry);

}  // namespace kvstore::detail
