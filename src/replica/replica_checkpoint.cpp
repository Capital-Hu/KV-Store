#include "replica_checkpoint.h"
#include "common/coding.h"
#include "raft/storage_codec.h"

#include <algorithm>
#include <stdexcept>

namespace kvstore::detail {

std::size_t ReplicaCheckpointBytes(const raft::NodeConfig& config) {
    // Identity uses the Raft header layout, followed by index, term and fingerprint.
    return raft::detail::FileHeaderBytes(config) + 20;
}

std::string EncodeReplicaCheckpoint(const raft::NodeConfig& config, const ReplicaCheckpoint& checkpoint) {
    raft::detail::ValidateConfig(config);
    const bool legacy = config.peers.size() == 2;
    std::string bytes(legacy ? "KVC1" : "KVC2");
    AppendU64(bytes, config.id);
    if (!legacy) { AppendU32(bytes, static_cast<std::uint32_t>(config.peers.size())); }
    auto peers = config.peers;
    std::sort(peers.begin(), peers.end());
    for (const auto peer : peers) { AppendU64(bytes, peer); }
    AppendU64(bytes, checkpoint.index);
    AppendU64(bytes, checkpoint.term);
    AppendU32(bytes, checkpoint.fingerprint);
    AppendU32(bytes, Crc32(bytes));
    return bytes;
}

ReplicaCheckpoint DecodeReplicaCheckpoint(std::string_view bytes, const raft::NodeConfig& config) {
    const auto size = ReplicaCheckpointBytes(config);
    const auto magic = config.peers.size() == 2 ? "KVC1" : "KVC2";
    if (bytes.size() != size || bytes.substr(0, 4) != magic ||
        ReadU32(bytes, size - 4) != Crc32(bytes.substr(0, size - 4))) {
        throw std::runtime_error("Invalid replica commit checkpoint or checksum");
    }
    const auto progress = size - 24;
    const ReplicaCheckpoint result{ReadU64(bytes, progress), ReadU64(bytes, progress + 8), ReadU32(bytes, progress + 16)};
    if (((result.index == 0) != (result.term == 0)) || (result.index == 0 && result.fingerprint != 0) ||
        bytes != EncodeReplicaCheckpoint(config, result)) {
        throw std::runtime_error("Replica checkpoint has invalid progress or a different node identity");
    }
    return result;
}

std::uint32_t ExtendReplicaFingerprint(std::uint32_t previous, const raft::LogEntry& entry) {
    std::string bytes;
    AppendU32(bytes, previous);
    AppendU64(bytes, entry.index);
    AppendU64(bytes, entry.term);
    bytes.push_back(static_cast<char>(entry.command.type));
    AppendU32(bytes, static_cast<std::uint32_t>(entry.command.key.size()));
    AppendU32(bytes, static_cast<std::uint32_t>(entry.command.value.size()));
    bytes += entry.command.key;
    bytes += entry.command.value;
    return Crc32(bytes);
}

}  // namespace kvstore::detail
