#pragma once

#include "kvstore/raft/storage.h"

#include <string_view>

namespace kvstore::raft::detail {

constexpr std::size_t kFrameHeaderBytes = 12;
constexpr std::size_t kFrameTrailerBytes = 4;

enum class ChangeType { TermAndVote = 1, Suffix = 2 };

struct Change {
    ChangeType type = ChangeType::TermAndVote;
    Term term = 0;
    std::optional<NodeId> voted_for;
    LogIndex first_index = 0;
    std::vector<LogEntry> entries;
};

void ValidateConfig(const NodeConfig& config);
bool ValidCommand(const Command& command);
void ValidateTermAndVote(const PersistentState& state, const NodeConfig& config,
                         Term term, std::optional<NodeId> voted_for);
std::size_t ValidateSuffix(const PersistentState& state, std::size_t log_bytes,
                           LogIndex first_index, const std::vector<LogEntry>& entries);
bool SameEntry(const LogEntry& left, const LogEntry& right);

std::string EncodeFileHeader(const NodeConfig& config);
std::size_t FileHeaderBytes(const NodeConfig& config);
void ValidateFileHeader(std::string_view bytes, const NodeConfig& config);
std::string EncodeChange(const Change& change);
Change DecodeChange(std::string_view payload);
std::string EncodeFrame(std::string_view payload);
std::size_t FramePayloadBytes(std::string_view header);
void ValidatePartialFrameHeader(std::string_view bytes);

}  // namespace kvstore::raft::detail
