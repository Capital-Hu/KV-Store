#pragma once

#include "kvstore/raft/node.h"
#include "kvstore/raft/storage.h"
#include "kvstore/store.h"

namespace kvstore {

struct ReplicatedStoreOptions {
    raft::ElectionOptions election;
    WalRecoveryMode store_recovery = WalRecoveryMode::Strict;
    raft::StorageRecoveryMode raft_recovery = raft::StorageRecoveryMode::Strict;
    std::size_t flush_threshold_bytes = 4 * 1024 * 1024;
    std::size_t compaction_threshold_tables = 4;
};

// A single-threaded coordinator owning one Raft node and its private Store.
// CreateNew requires an absent directory; OpenExisting never bootstraps missing metadata.
// Only committed Put/Delete commands reach Store. Startup replays the full saved prefix.
class ReplicatedStore {
public:
    ReplicatedStore(const std::string& directory, raft::NodeConfig config, raft::StorageOpenMode mode,
                    ReplicatedStoreOptions options = {});
    ~ReplicatedStore();
    ReplicatedStore(const ReplicatedStore&) = delete;
    ReplicatedStore& operator=(const ReplicatedStore&) = delete;

    bool usable() const;
    raft::NodeStatus status() const;
    raft::Proposal Propose(raft::Command command);
    raft::Proposal ProposeReadBarrier();
    void Step(const raft::Message& message);
    void AdvanceTime(std::chrono::milliseconds elapsed);
    std::vector<raft::Message> DrainMessages();
    bool IsApplied(raft::Proposal proposal) const;
    // Local applied state only; a follower or isolated leader may serve stale data.
    // This is not a linearizable client read API.
    std::optional<std::string> LocalGet(const std::string& key);
    void Flush();
    void Compact();
    std::size_t sstable_count();

private:
    friend struct ReplicatedStoreTestAccess;
    enum class ApplyStage { CheckpointTempSynced, CheckpointRenamed, CheckpointDurable, CommandStored, AppliedAdvanced };
    struct DirectoryLock;
    void EnsureUsable() const;
    void ApplyCommitted();
    void SaveCommit(raft::LogIndex index);
    void Notify(ApplyStage stage, raft::LogIndex index);

    std::string directory_;
    raft::NodeConfig config_;
    // Destruction closes Store and Raft before releasing the outer directory lock.
    std::unique_ptr<DirectoryLock> lock_;
    std::unique_ptr<raft::Node> node_;
    std::unique_ptr<Store> store_;
    raft::LogIndex saved_commit_ = 0;
    std::uint32_t prefix_fingerprint_ = 0;
    bool usable_ = true;
    std::function<void(ApplyStage, raft::LogIndex)> hook_;
};

}  // namespace kvstore
