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
    // Snapshot once this many applied entries or live log bytes accumulate. Zero entries disables it.
    std::size_t snapshot_threshold_entries = 100000;
    std::size_t snapshot_threshold_bytes = 32 * 1024 * 1024;
    // Recent entries kept after truncation so slightly lagging peers avoid a full snapshot.
    std::size_t snapshot_retain_entries = 1000;
    std::size_t snapshot_retain_bytes = 8 * 1024 * 1024;
};

// One complete durable state-machine image; paths are ordered oldest SSTable first.
struct SnapshotFiles {
    raft::SnapshotInfo info;
    std::vector<std::string> paths;
};

namespace detail_snapshot {
struct Latest { raft::SnapshotInfo info; std::uint32_t file_count = 0; };
}

// A single-threaded coordinator owning one Raft node and its private Store.
// CreateNew requires an absent directory; OpenExisting never bootstraps missing metadata.
// Only committed Put/Delete commands reach Store. Startup replays the saved prefix after the Raft base.
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

    std::optional<SnapshotFiles> LatestSnapshot() const;
    // False when this node already holds or has staged the prefix, so a transfer would be wasted.
    bool ShouldReceiveSnapshot(const raft::SnapshotInfo& snapshot) const;
    // Transports fill this directory with SnapshotFileName(0..count-1), fully synced,
    // without any serialization against the other methods, then call Publish serialized.
    const std::string& incoming_snapshot_directory() const { return incoming_directory_; }
    void PublishIncomingSnapshot(const raft::SnapshotInfo& snapshot, std::uint32_t file_count);
    static std::string SnapshotFileName(std::size_t ordinal) { return Store::CheckpointFileName(ordinal); }

private:
    friend struct ReplicatedStoreTestAccess;
    enum class ApplyStage { CheckpointTempSynced, CheckpointRenamed, CheckpointDurable, CommandStored, AppliedAdvanced,
                            InstallMarked, InstallStoreReset, InstallRaftReset, InstallCommitSaved };
    struct DirectoryLock;
    void EnsureUsable() const;
    void ApplyCommitted();
    void SaveCommit(raft::LogIndex index);
    void WriteCommitFile(raft::LogIndex index, raft::Term term, std::uint32_t fingerprint);
    void MaybeSnapshot();
    void InstallPendingSnapshot();
    void ResetStoreFrom(const std::string& snapshot_directory, std::uint32_t file_count) const;
    std::string SnapshotDirectory(raft::LogIndex index) const;
    void PublishSnapshotDirectory(const std::string& from, const raft::SnapshotInfo& info, std::uint32_t file_count);
    void LoadSnapshots(const raft::PersistentState& state);
    void Notify(ApplyStage stage, raft::LogIndex index);

    std::string directory_;
    std::string incoming_directory_;
    ReplicatedStoreOptions options_;
    raft::NodeConfig config_;
    // Destruction closes Store and Raft before releasing the outer directory lock.
    std::unique_ptr<DirectoryLock> lock_;
    std::unique_ptr<raft::Node> node_;
    std::unique_ptr<Store> store_;
    raft::LogIndex saved_commit_ = 0;
    std::uint32_t prefix_fingerprint_ = 0;
    bool usable_ = true;
    std::optional<detail_snapshot::Latest> latest_snapshot_;
    std::optional<raft::SnapshotInfo> staged_snapshot_;
    std::uint32_t staged_file_count_ = 0;
    std::function<void(ApplyStage, raft::LogIndex)> hook_;
};

}  // namespace kvstore
