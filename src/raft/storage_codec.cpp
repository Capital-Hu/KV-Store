#include "storage_codec.h"
#include "common/coding.h"

#include <algorithm>
#include <limits>
#include <set>
#include <stdexcept>
#include <utility>

namespace kvstore::raft::detail {
namespace {
namespace coding = kvstore::detail;
constexpr std::string_view kFrameMagic = "RFR1";
constexpr std::size_t kEntryHeaderBytes = 25;

std::size_t EntryBytes(const LogEntry& entry) {
    return kEntryHeaderBytes + entry.command.key.size() + entry.command.value.size();
}

class Cursor {
public:
    explicit Cursor(std::string_view bytes) : bytes_(bytes) {}
    std::string_view Take(std::size_t count) {
        if (count > bytes_.size() - offset_) { throw std::runtime_error("Incomplete Raft record payload"); }
        const auto result = bytes_.substr(offset_, count);
        offset_ += count;
        return result;
    }
    std::uint64_t U64() { return coding::ReadU64(Take(8), 0); }
    std::uint32_t U32() { return coding::ReadU32(Take(4), 0); }
    unsigned char U8() { return static_cast<unsigned char>(Take(1)[0]); }
    std::size_t remaining() const { return bytes_.size() - offset_; }
private:
    std::string_view bytes_;
    std::size_t offset_ = 0;
};
}  // namespace

void ValidateConfig(const NodeConfig& config) {
    if (config.id == 0 || !ValidMemberCount(config.peers.size() + 1) ||
        config.peers.size() > std::numeric_limits<std::uint32_t>::max() ||
        config.peers.size() > (std::numeric_limits<std::size_t>::max() - 40) / 8) {
        throw std::invalid_argument("Raft requires a nonzero node ID and a positive odd member count");
    }
    std::set<NodeId> seen;
    for (const auto peer : config.peers) {
        if (peer == 0 || peer == config.id || !seen.insert(peer).second) {
            throw std::invalid_argument("Raft peers must be distinct, nonzero and exclude this node");
        }
    }
}

void ValidateTermAndVote(const PersistentState& state, const NodeConfig& config,
                         Term term, std::optional<NodeId> voted_for) {
    if (term < state.current_term) { throw std::invalid_argument("Raft term cannot decrease"); }
    if (voted_for && (term == 0 || (*voted_for != config.id &&
        std::find(config.peers.begin(), config.peers.end(), *voted_for) == config.peers.end()))) {
        throw std::invalid_argument("Raft vote must name a cluster member in a nonzero term");
    }
    if (term == state.current_term && state.voted_for && state.voted_for != voted_for) {
        throw std::invalid_argument("Raft vote cannot change or clear within one term");
    }
}

bool SameEntry(const LogEntry& left, const LogEntry& right) {
    return left.index == right.index && left.term == right.term && left.command.type == right.command.type &&
           left.command.key == right.command.key && left.command.value == right.command.value;
}

bool ValidCommand(const Command& command) {
    if (command.key.size() > kMaxRaftCommandBytes ||
        command.value.size() > kMaxRaftCommandBytes - command.key.size()) { return false; }
    switch (command.type) {
        case CommandType::Put: return true;
        case CommandType::Delete: return command.value.empty();
        case CommandType::NoOp: return command.key.empty() && command.value.empty();
    }
    return false;
}

std::size_t ValidateSuffix(const PersistentState& state, std::size_t log_bytes,
                           LogIndex first_index, const std::vector<LogEntry>& entries) {
    if (first_index == 0 || first_index > state.log.size() + 1) {
        throw std::invalid_argument("Raft suffix must follow an existing log prefix");
    }
    const auto prefix = static_cast<std::size_t>(first_index - 1);
    if (entries.size() > kMaxRaftEntries - prefix) { throw std::length_error("Too many live Raft entries"); }
    for (std::size_t i = prefix; i < state.log.size(); ++i) { log_bytes -= EntryBytes(state.log[i]); }
    auto previous_term = prefix == 0 ? 0 : state.log[prefix - 1].term;
    for (std::size_t i = 0; i < entries.size(); ++i) {
        const auto& entry = entries[i];
        const auto& command = entry.command;
        if (entry.index != first_index + i || entry.term == 0 || entry.term < previous_term ||
            entry.term > state.current_term) {
            throw std::invalid_argument("Raft log indexes and terms must be ordered and match the current term bound");
        }
        if (!ValidCommand(command)) {
            throw std::invalid_argument("Invalid Raft command or key plus value exceeds 1 MiB");
        }
        if (prefix + i < state.log.size() && state.log[prefix + i].term == entry.term &&
            !SameEntry(state.log[prefix + i], entry)) {
            throw std::invalid_argument("A Raft index and term cannot identify different commands");
        }
        const auto bytes = EntryBytes(entry);
        if (bytes > kMaxRaftLogBytes - log_bytes) { throw std::length_error("Live Raft log exceeds byte limit"); }
        log_bytes += bytes;
        previous_term = entry.term;
    }
    return log_bytes;
}

std::string EncodeFileHeader(const NodeConfig& config) {
    ValidateConfig(config);
    const bool legacy = config.peers.size() == 2;
    std::string bytes(legacy ? "KVR1" : "KVR2");
    coding::AppendU64(bytes, config.id);
    if (!legacy) { coding::AppendU32(bytes, static_cast<std::uint32_t>(config.peers.size())); }
    auto peers = config.peers;
    std::sort(peers.begin(), peers.end());
    for (const auto peer : peers) { coding::AppendU64(bytes, peer); }
    coding::AppendU32(bytes, coding::Crc32(bytes));
    return bytes;
}

std::size_t FileHeaderBytes(const NodeConfig& config) {
    ValidateConfig(config);
    return config.peers.size() == 2 ? 32 : 20 + 8 * config.peers.size();
}

void ValidateFileHeader(std::string_view bytes, const NodeConfig& config) {
    const auto expected = EncodeFileHeader(config);
    if (bytes.size() != expected.size() || bytes.substr(0, 4) != expected.substr(0, 4) ||
        coding::ReadU32(bytes, bytes.size() - 4) != coding::Crc32(bytes.substr(0, bytes.size() - 4))) {
        throw std::runtime_error("Invalid Raft file header or checksum");
    }
    if (bytes != expected) { throw std::runtime_error("Raft file belongs to a different node or cluster"); }
}

std::string EncodeChange(const Change& change) {
    std::string bytes(1, static_cast<char>(change.type));
    if (change.type == ChangeType::TermAndVote) {
        coding::AppendU64(bytes, change.term);
        coding::AppendU64(bytes, change.voted_for.value_or(0));
    } else if (change.type == ChangeType::Suffix) {
        coding::AppendU64(bytes, change.first_index);
        coding::AppendU32(bytes, static_cast<std::uint32_t>(change.entries.size()));
        for (const auto& entry : change.entries) {
            if (EntryBytes(entry) > kMaxRaftRecordBytes - bytes.size()) {
                throw std::length_error("Raft batch exceeds record limit");
            }
            coding::AppendU64(bytes, entry.index);
            coding::AppendU64(bytes, entry.term);
            bytes.push_back(entry.command.type == CommandType::Put ? 1 :
                            entry.command.type == CommandType::Delete ? 2 : 3);
            coding::AppendU32(bytes, static_cast<std::uint32_t>(entry.command.key.size()));
            coding::AppendU32(bytes, static_cast<std::uint32_t>(entry.command.value.size()));
            bytes += entry.command.key;
            bytes += entry.command.value;
        }
    } else { throw std::invalid_argument("Unknown Raft change type"); }
    return bytes;
}

Change DecodeChange(std::string_view payload) {
    Cursor input(payload);
    Change change;
    change.type = static_cast<ChangeType>(input.U8());
    if (change.type == ChangeType::TermAndVote) {
        change.term = input.U64();
        const auto vote = input.U64();
        if (vote != 0) { change.voted_for = vote; }
    } else if (change.type == ChangeType::Suffix) {
        change.first_index = input.U64();
        const auto count = input.U32();
        if (count > kMaxRaftEntries || count > input.remaining() / kEntryHeaderBytes) {
            throw std::runtime_error("Invalid Raft entry count");
        }
        change.entries.reserve(count);
        for (std::uint32_t i = 0; i < count; ++i) {
            LogEntry entry;
            entry.index = input.U64(); entry.term = input.U64();
            const auto type = input.U8();
            if (type < 1 || type > 3) { throw std::runtime_error("Unknown Raft command type"); }
            entry.command.type = type == 1 ? CommandType::Put : type == 2 ? CommandType::Delete : CommandType::NoOp;
            const auto key_bytes = input.U32(), value_bytes = input.U32();
            if (key_bytes > kMaxRaftCommandBytes || value_bytes > kMaxRaftCommandBytes - key_bytes) {
                throw std::runtime_error("Raft command exceeds byte limit");
            }
            entry.command.key = std::string(input.Take(key_bytes));
            entry.command.value = std::string(input.Take(value_bytes));
            change.entries.push_back(std::move(entry));
        }
    } else { throw std::runtime_error("Unknown Raft change type"); }
    if (input.remaining() != 0) { throw std::runtime_error("Unexpected Raft payload trailing bytes"); }
    return change;
}

std::string EncodeFrame(std::string_view payload) {
    if (payload.empty() || payload.size() > kMaxRaftRecordBytes) { throw std::length_error("Invalid Raft record size"); }
    std::string bytes(kFrameMagic);
    coding::AppendU32(bytes, static_cast<std::uint32_t>(payload.size()));
    coding::AppendU32(bytes, coding::Crc32(bytes));
    bytes.append(payload);
    coding::AppendU32(bytes, coding::Crc32(payload));
    return bytes;
}

std::size_t FramePayloadBytes(std::string_view header) {
    if (header.size() != kFrameHeaderBytes || header.substr(0, 4) != kFrameMagic ||
        coding::ReadU32(header, 8) != coding::Crc32(header.substr(0, 8))) {
        throw std::runtime_error("Invalid Raft record header or checksum");
    }
    const auto length = coding::ReadU32(header, 4);
    if (length == 0 || length > kMaxRaftRecordBytes) { throw std::runtime_error("Invalid Raft record length"); }
    return length;
}

void ValidatePartialFrameHeader(std::string_view bytes) {
    const auto magic_bytes = std::min(bytes.size(), kFrameMagic.size());
    if (bytes.substr(0, magic_bytes) != kFrameMagic.substr(0, magic_bytes)) {
        throw std::runtime_error("Corrupt Raft record magic");
    }
    if (bytes.size() >= 8) {
        const auto length = coding::ReadU32(bytes, 4);
        if (length == 0 || length > kMaxRaftRecordBytes) { throw std::runtime_error("Invalid partial Raft record length"); }
    }
}

}  // namespace kvstore::raft::detail
