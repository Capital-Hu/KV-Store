#include "replica_checkpoint.h"
#include "coding.h"

#include <algorithm>
#include <stdexcept>

namespace kvstore::detail {

std::string EncodeReplicaCheckpoint(const raft::NodeConfig& config, const ReplicaCheckpoint& checkpoint) {
    std::string bytes("KVC1");
    AppendU64(bytes, config.id);
    AppendU64(bytes, std::min(config.peers.at(0), config.peers.at(1)));
    AppendU64(bytes, std::max(config.peers.at(0), config.peers.at(1)));
    AppendU64(bytes, checkpoint.index);
    AppendU64(bytes, checkpoint.term);
    AppendU32(bytes, checkpoint.fingerprint);
    AppendU32(bytes, Crc32(bytes));
    return bytes;
}

ReplicaCheckpoint DecodeReplicaCheckpoint(std::string_view bytes, const raft::NodeConfig& config) {
    if (bytes.size() != kReplicaCheckpointBytes || bytes.substr(0, 4) != "KVC1" ||
        ReadU32(bytes, 48) != Crc32(bytes.substr(0, 48))) {
        throw std::runtime_error("Invalid replica commit checkpoint or checksum");
    }
    const ReplicaCheckpoint result{ReadU64(bytes, 28), ReadU64(bytes, 36), ReadU32(bytes, 44)};
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
