#pragma once

#include "kvstore/raft/types.h"

#include <cstddef>
#include <functional>
#include <string>
#include <string_view>

namespace kvstore::raft {

constexpr std::size_t kMaxRaftCommandBytes = 1024 * 1024;
constexpr std::size_t kMaxRaftRecordBytes = 16 * 1024 * 1024;
constexpr std::size_t kMaxRaftLogBytes = 64 * 1024 * 1024;
constexpr std::size_t kMaxRaftEntries = 1000000;

enum class StorageOpenMode { CreateNew, OpenExisting };
enum class StorageRecoveryMode { Strict, TruncateIncompleteTail };

// Single-threaded, exclusively owned durable state, separate from Store.
// The parent directory must exist; path, path.lock and path.tmp are reserved.
class Storage {
public:
    Storage(const std::string& path, NodeConfig config, StorageOpenMode mode,
            StorageRecoveryMode recovery = StorageRecoveryMode::Strict);
    ~Storage();
    Storage(const Storage&) = delete;
    Storage& operator=(const Storage&) = delete;
    Storage(Storage&&) = delete;
    Storage& operator=(Storage&&) = delete;

    const NodeConfig& config() const { return config_; }
    const PersistentState& state() const;
    bool usable() const { return usable_; }
    std::size_t truncated_tail_bytes() const { return truncated_tail_bytes_; }

    // Persist before publishing the new in-memory state or returning success.
    // A recorded vote cannot be changed or cleared within the same term.
    void SetTermAndVote(Term term, std::optional<NodeId> voted_for);
    void Append(const std::vector<LogEntry>& entries);
    // Keep [1, first_index), replace the ENTIRE suffix with entries (possibly empty).
    // The future protocol layer must also protect its committed prefix.
    void ReplaceSuffix(LogIndex first_index, const std::vector<LogEntry>& entries);

private:
    friend struct StorageTestAccess;
    enum class Stage { RecordWritten, RecordSynced, StateUpdated };
    void Notify(Stage stage);
    void EnsureUsable() const;
    void AppendFrame(std::string_view frame);
    void Load(StorageRecoveryMode recovery);

    std::string path_;
    NodeConfig config_;
    int fd_ = -1;
    int lock_fd_ = -1;
    PersistentState state_;
    std::size_t log_bytes_ = 0;
    std::size_t truncated_tail_bytes_ = 0;
    bool usable_ = true;
    std::function<void(Stage)> hook_;
};

}  // namespace kvstore::raft
