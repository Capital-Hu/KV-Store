#include "kvstore/replicated_store.h"
#include "common/file_io.h"
#include "replica_checkpoint.h"

#include <filesystem>
#include <stdexcept>
#include <string_view>
#include <utility>

#include <fcntl.h>
#include <unistd.h>

namespace kvstore {

namespace {
namespace fs = std::filesystem;
constexpr auto kSnapshotPrefix = "snapshot.";

void WriteSynced(const std::string& path, std::string_view bytes) {
    detail::OwnedFd file(detail::OpenRegular(path, O_WRONLY | O_CREAT | O_TRUNC));
    detail::WriteAll(file.get(), bytes);
    detail::SyncFile(file.get());
}

void RenameDurably(const std::string& from, const std::string& to) {
    if (::rename(from.c_str(), to.c_str()) == -1) { detail::ThrowSystemError("Publish snapshot directory"); }
    detail::SyncParentDirectory(to);
}

detail::SnapshotMeta ReadMeta(const std::string& directory) {
    return detail::DecodeSnapshotMeta(detail::ReadFile(directory + "/META", detail::kSnapshotMetaBytes));
}

std::optional<raft::LogIndex> SnapshotIndexOf(const std::string& name) {
    const std::string prefix(kSnapshotPrefix);
    if (name.compare(0, prefix.size(), prefix) != 0 || name.size() == prefix.size()) { return std::nullopt; }
    raft::LogIndex index = 0;
    for (auto i = prefix.size(); i < name.size(); ++i) {
        if (name[i] < '0' || name[i] > '9') { return std::nullopt; }
        index = index * 10 + static_cast<raft::LogIndex>(name[i] - '0');
    }
    return index;
}

bool Same(const raft::SnapshotInfo& a, const raft::SnapshotInfo& b) {
    return a.index == b.index && a.term == b.term && a.fingerprint == b.fingerprint;
}
}  // namespace

struct ReplicatedStore::DirectoryLock {
    DirectoryLock(const std::string& path, raft::StorageOpenMode mode)
        : fd(detail::OpenRegular(path, O_RDWR | (mode == raft::StorageOpenMode::CreateNew ? O_CREAT | O_EXCL : 0))) {
        detail::LockExclusive(fd.get());
        detail::SyncFile(fd.get());
        detail::SyncParentDirectory(path);
    }
    detail::OwnedFd fd;
};

ReplicatedStore::ReplicatedStore(const std::string& directory, raft::NodeConfig config,
                               raft::StorageOpenMode mode, ReplicatedStoreOptions options)
    : options_(options), config_(std::move(config)) {
    // Validate configuration and timeouts before creating any files.
    raft::Node validation(config_, {}, options.election);
    if (directory.empty() || directory.find('\0') != std::string::npos ||
        (mode != raft::StorageOpenMode::CreateNew && mode != raft::StorageOpenMode::OpenExisting) ||
        (options.store_recovery != WalRecoveryMode::Strict && options.store_recovery != WalRecoveryMode::TruncateIncompleteTail) ||
        (options.raft_recovery != raft::StorageRecoveryMode::Strict &&
         options.raft_recovery != raft::StorageRecoveryMode::TruncateIncompleteTail)) {
        throw std::invalid_argument("Invalid replica directory, open mode or recovery policy");
    }
    if (options.snapshot_threshold_entries != 0 &&
        (options.snapshot_retain_entries >= options.snapshot_threshold_entries ||
         options.snapshot_retain_bytes >= options.snapshot_threshold_bytes ||
         options.snapshot_threshold_bytes > raft::kMaxRaftLogBytes ||
         options.snapshot_threshold_entries > raft::kMaxRaftEntries)) {
        throw std::invalid_argument("Snapshot thresholds must fit the log and exceed the retained tail");
    }
    directory_ = std::filesystem::absolute(directory).lexically_normal().string();
    incoming_directory_ = directory_ + "/snapshot-incoming";
    const bool creating = mode == raft::StorageOpenMode::CreateNew;
    if (creating) {
        if (!std::filesystem::create_directory(directory_)) { throw std::runtime_error("Replica directory already exists"); }
        detail::SyncParentDirectory(directory_);
    } else if (!std::filesystem::is_directory(std::filesystem::symlink_status(directory_))) {
        throw std::runtime_error("Replica directory is missing or is not a real directory");
    }
    lock_ = std::make_unique<DirectoryLock>(directory_ + "/LOCK", mode);
    detail::ReplicaCheckpoint checkpoint;
    const auto checkpoint_path = directory_ + "/COMMIT";
    if (!creating && (!detail::PathExists(checkpoint_path) || !detail::PathExists(directory_ + "/data.manifest") ||
                      !detail::PathExists(directory_ + "/data.lock"))) {
        throw std::runtime_error("Replica metadata is missing; refusing to bootstrap empty data");
    }
    auto storage = std::make_unique<raft::Storage>(directory_ + "/raft", config_, mode, options.raft_recovery);
    const auto install_directory = directory_ + "/snapshot-install";
    if (!creating && detail::PathExists(install_directory)) {
        // A crash interrupted an install after its point of no return: finish it.
        const auto meta = ReadMeta(install_directory);
        ResetStoreFrom(install_directory, meta.file_count);
        storage->ResetToSnapshot(meta.info);
        WriteCommitFile(meta.info.index, meta.info.term, meta.info.fingerprint);
        fs::remove_all(SnapshotDirectory(meta.info.index));
        RenameDurably(install_directory, SnapshotDirectory(meta.info.index));
    }
    if (!creating) {
        checkpoint = detail::DecodeReplicaCheckpoint(
            detail::ReadFile(checkpoint_path, detail::ReplicaCheckpointBytes(config_)), config_);
    }
    const auto& state = storage->state();
    if (checkpoint.index < state.base.index || checkpoint.index > state.last_index() ||
        (checkpoint.index != 0 && state.TermAt(checkpoint.index) != checkpoint.term)) {
        throw std::runtime_error("Replica commit checkpoint exceeds or disagrees with the Raft log");
    }
    std::uint32_t fingerprint = state.base.fingerprint;
    for (auto i = state.base.index + 1; i <= checkpoint.index; ++i) {
        fingerprint = detail::ExtendReplicaFingerprint(fingerprint, state.EntryAt(i));
    }
    if (fingerprint != checkpoint.fingerprint) { throw std::runtime_error("Replica committed prefix fingerprint mismatch"); }
    LoadSnapshots(state);
    node_ = std::make_unique<raft::Node>(config_, std::move(storage), options.election, checkpoint.index);
    if (latest_snapshot_) { node_->SetSnapshot(latest_snapshot_->info); }
    store_ = std::make_unique<Store>(directory_ + "/data", options.store_recovery,
                                   options.flush_threshold_bytes, options.compaction_threshold_tables);
    saved_commit_ = checkpoint.index;
    prefix_fingerprint_ = checkpoint.fingerprint;
    // Never promote a leftover temporary file into authoritative commit metadata.
    detail::RemoveFile(checkpoint_path + ".tmp");
    if (creating) { SaveCommit(0); }
    // No public reads are possible until replay of the entire saved prefix finishes.
    ApplyCommitted();
}

ReplicatedStore::~ReplicatedStore() = default;

bool ReplicatedStore::usable() const { return usable_ && node_ && node_->usable(); }

void ReplicatedStore::EnsureUsable() const {
    if (!usable()) { throw std::runtime_error("Replicated Store failed; close and reopen before reuse"); }
}

void ReplicatedStore::Notify(ApplyStage stage, raft::LogIndex index) {
    if (hook_) { hook_(stage, index); }
}

void ReplicatedStore::SaveCommit(raft::LogIndex index) {
    auto fingerprint = prefix_fingerprint_;
    const auto& state = node_->persistent_state();
    for (auto i = saved_commit_ + 1; i <= index; ++i) { fingerprint = detail::ExtendReplicaFingerprint(fingerprint, state.EntryAt(i)); }
    WriteCommitFile(index, index == 0 ? 0 : state.TermAt(index), fingerprint);
    saved_commit_ = index;
    prefix_fingerprint_ = fingerprint;
}

void ReplicatedStore::WriteCommitFile(raft::LogIndex index, raft::Term term, std::uint32_t fingerprint) {
    const auto bytes = detail::EncodeReplicaCheckpoint(config_, {index, term, fingerprint});
    const auto path = directory_ + "/COMMIT";
    const auto temporary = path + ".tmp";
    detail::RemoveFile(temporary);
    {
        detail::OwnedFd fd(detail::OpenRegular(temporary, O_WRONLY | O_CREAT | O_EXCL));
        detail::WriteAll(fd.get(), bytes);
        detail::SyncFile(fd.get());
    }
    Notify(ApplyStage::CheckpointTempSynced, index);
    if (::rename(temporary.c_str(), path.c_str()) == -1) { detail::ThrowSystemError("Publish replica commit checkpoint"); }
    Notify(ApplyStage::CheckpointRenamed, index);
    detail::SyncParentDirectory(path);
    Notify(ApplyStage::CheckpointDurable, index);
}

std::string ReplicatedStore::SnapshotDirectory(raft::LogIndex index) const {
    return directory_ + "/" + kSnapshotPrefix + std::to_string(index);
}

void ReplicatedStore::ResetStoreFrom(const std::string& snapshot_directory, std::uint32_t file_count) const {
    std::vector<std::string> files;
    files.reserve(file_count);
    for (std::uint32_t i = 0; i < file_count; ++i) { files.push_back(snapshot_directory + "/" + SnapshotFileName(i)); }
    Store::ResetToSnapshot(directory_ + "/data", files);
}

void ReplicatedStore::LoadSnapshots(const raft::PersistentState& state) {
    fs::remove_all(incoming_directory_);
    fs::remove_all(directory_ + "/snapshot-staged");
    fs::remove_all(directory_ + "/snapshot-tmp");
    std::optional<raft::LogIndex> newest;
    std::vector<fs::path> stale;
    for (const auto& item : fs::directory_iterator(directory_)) {
        const auto index = SnapshotIndexOf(item.path().filename().string());
        if (!index) { continue; }
        if (!newest || *index > *newest) {
            if (newest) { stale.push_back(SnapshotDirectory(*newest)); }
            newest = index;
        } else { stale.push_back(item.path()); }
    }
    for (const auto& path : stale) { fs::remove_all(path); }
    if (newest) {
        const auto meta = ReadMeta(SnapshotDirectory(*newest));
        if (meta.info.index != *newest) { throw std::runtime_error("Snapshot directory disagrees with its metadata"); }
        latest_snapshot_ = detail_snapshot::Latest{meta.info, meta.file_count};
    }
    if (state.base.index != 0 && (!newest || *newest < state.base.index)) {
        throw std::runtime_error("Truncated Raft log has no covering snapshot");
    }
}

// Move a complete directory into place as the newest snapshot, then drop older ones.
void ReplicatedStore::PublishSnapshotDirectory(const std::string& from, const raft::SnapshotInfo& info, std::uint32_t file_count) {
    const auto target = SnapshotDirectory(info.index);
    fs::remove_all(target);
    RenameDurably(from, target);
    const auto previous = latest_snapshot_;
    latest_snapshot_ = detail_snapshot::Latest{info, file_count};
    if (previous && previous->info.index != info.index) { fs::remove_all(SnapshotDirectory(previous->info.index)); }
}

void ReplicatedStore::MaybeSnapshot() {
    if (options_.snapshot_threshold_entries == 0) { return; }
    const auto applied = node_->status().last_applied;
    const auto& state = node_->persistent_state();
    const auto base = state.base.index;
    if (applied <= base) { return; }
    if (applied - base < options_.snapshot_threshold_entries && node_->live_log_bytes() < options_.snapshot_threshold_bytes) { return; }
    auto boundary = applied;
    std::size_t kept_entries = 0, kept_bytes = 0;
    while (boundary > base && kept_entries < options_.snapshot_retain_entries) {
        const auto& command = state.EntryAt(boundary).command;
        const auto size = 25 + command.key.size() + command.value.size();
        if (size > options_.snapshot_retain_bytes - kept_bytes) { break; }
        kept_bytes += size; ++kept_entries; --boundary;
    }
    if (boundary <= base) { return; }

    auto fingerprint = state.base.fingerprint;
    std::uint32_t boundary_fingerprint = 0;
    for (auto i = base + 1; i <= applied; ++i) {
        fingerprint = detail::ExtendReplicaFingerprint(fingerprint, state.EntryAt(i));
        if (i == boundary) { boundary_fingerprint = fingerprint; }
    }
    const raft::SnapshotInfo info{applied, state.TermAt(applied), fingerprint};
    const auto temporary = directory_ + "/snapshot-tmp";
    fs::remove_all(temporary);
    fs::create_directory(temporary);
    const auto files = store_->CreateCheckpoint(temporary);
    const auto count = static_cast<std::uint32_t>(files.size());
    WriteSynced(temporary + "/META", detail::EncodeSnapshotMeta({info, count}));
    detail::SyncParentDirectory(temporary + "/META");
    PublishSnapshotDirectory(temporary, info, count);
    node_->SetSnapshot(info);
    // Only now is the prefix replaceable: a durable snapshot covers everything being dropped.
    node_->CompactLog(boundary, boundary_fingerprint);
}

std::optional<SnapshotFiles> ReplicatedStore::LatestSnapshot() const {
    EnsureUsable();
    if (!latest_snapshot_) { return std::nullopt; }
    SnapshotFiles result{latest_snapshot_->info, {}};
    const auto directory = SnapshotDirectory(result.info.index);
    for (std::uint32_t i = 0; i < latest_snapshot_->file_count; ++i) { result.paths.push_back(directory + "/" + SnapshotFileName(i)); }
    return result;
}

bool ReplicatedStore::ShouldReceiveSnapshot(const raft::SnapshotInfo& snapshot) const {
    EnsureUsable();
    return snapshot.index > node_->status().commit_index && !(staged_snapshot_ && Same(*staged_snapshot_, snapshot));
}

void ReplicatedStore::PublishIncomingSnapshot(const raft::SnapshotInfo& snapshot, std::uint32_t file_count) {
    EnsureUsable();
    const auto staged = directory_ + "/snapshot-staged";
    WriteSynced(incoming_directory_ + "/META", detail::EncodeSnapshotMeta({snapshot, file_count}));
    detail::SyncParentDirectory(incoming_directory_ + "/META");
    staged_snapshot_.reset();
    fs::remove_all(staged);
    RenameDurably(incoming_directory_, staged);
    staged_snapshot_ = snapshot;
    staged_file_count_ = file_count;
}

void ReplicatedStore::InstallPendingSnapshot() {
    const auto snapshot = *node_->PendingSnapshotInstall();
    if (!staged_snapshot_ || !Same(*staged_snapshot_, snapshot)) {
        node_->FinishSnapshotInstall(false);  // Files have not arrived; the leader will ask again.
        return;
    }
    const auto file_count = staged_file_count_;
    const auto install = directory_ + "/snapshot-install";
    // This rename is the commit point: a restart from here on must finish the install.
    RenameDurably(directory_ + "/snapshot-staged", install);
    staged_snapshot_.reset();
    Notify(ApplyStage::InstallMarked, snapshot.index);
    store_.reset();
    ResetStoreFrom(install, file_count);
    store_ = std::make_unique<Store>(directory_ + "/data", options_.store_recovery,
                                     options_.flush_threshold_bytes, options_.compaction_threshold_tables);
    Notify(ApplyStage::InstallStoreReset, snapshot.index);
    node_->FinishSnapshotInstall(true);
    Notify(ApplyStage::InstallRaftReset, snapshot.index);
    WriteCommitFile(snapshot.index, snapshot.term, snapshot.fingerprint);
    saved_commit_ = snapshot.index;
    prefix_fingerprint_ = snapshot.fingerprint;
    Notify(ApplyStage::InstallCommitSaved, snapshot.index);
    PublishSnapshotDirectory(install, snapshot, file_count);
}

void ReplicatedStore::ApplyCommitted() {
    const auto committed = node_->status().commit_index;
    if (committed > saved_commit_) { SaveCommit(committed); }
    while (node_->status().last_applied < committed) {
        const auto index = node_->status().last_applied + 1;
        const auto& command = node_->persistent_state().EntryAt(index).command;
        switch (command.type) {
            case raft::CommandType::Put: store_->Put(command.key, command.value); break;
            case raft::CommandType::Delete: store_->Delete(command.key); break;
            case raft::CommandType::NoOp: break;
        }
        Notify(ApplyStage::CommandStored, index);
        node_->MarkApplied(index);
        Notify(ApplyStage::AppliedAdvanced, index);
    }
    MaybeSnapshot();
}

raft::NodeStatus ReplicatedStore::status() const { EnsureUsable(); return node_->status(); }

raft::Proposal ReplicatedStore::Propose(raft::Command command) {
    EnsureUsable();
    raft::Proposal result;
    try { result = node_->Propose(std::move(command)); }
    catch (...) { if (!node_->usable()) { usable_ = false; } throw; }
    try { ApplyCommitted(); } catch (...) { usable_ = false; throw; }
    return result;
}

void ReplicatedStore::Step(const raft::Message& message) {
    EnsureUsable();
    try {
        node_->Step(message);
        if (node_->PendingSnapshotInstall()) { InstallPendingSnapshot(); }
        ApplyCommitted();
    } catch (...) { usable_ = false; throw; }
}

raft::Proposal ReplicatedStore::ProposeReadBarrier() {
    EnsureUsable();
    raft::Proposal result;
    try { result = node_->ProposeReadBarrier(); }
    catch (...) { if (!node_->usable()) { usable_ = false; } throw; }
    try { ApplyCommitted(); } catch (...) { usable_ = false; throw; }
    return result;
}

void ReplicatedStore::AdvanceTime(std::chrono::milliseconds elapsed) {
    EnsureUsable();
    try { node_->AdvanceTime(elapsed); }
    catch (...) { if (!node_->usable()) { usable_ = false; } throw; }
    try { ApplyCommitted(); } catch (...) { usable_ = false; throw; }
}

std::vector<raft::Message> ReplicatedStore::DrainMessages() { EnsureUsable(); return node_->DrainMessages(); }

bool ReplicatedStore::IsApplied(raft::Proposal proposal) const {
    EnsureUsable();
    const auto& state = node_->persistent_state();
    // A truncated index can no longer prove which term wrote it, so the outcome stays unconfirmed.
    return proposal.index != 0 && proposal.index >= state.base.index && proposal.index <= node_->status().last_applied &&
        state.TermAt(proposal.index) == proposal.term;
}

std::optional<std::string> ReplicatedStore::LocalGet(const std::string& key) {
    EnsureUsable();
    try { return store_->Get(key); } catch (...) { usable_ = false; throw; }
}

void ReplicatedStore::Flush() {
    EnsureUsable();
    try { store_->Flush(); } catch (...) { usable_ = false; throw; }
}

void ReplicatedStore::Compact() {
    EnsureUsable();
    try { store_->Compact(); } catch (...) { usable_ = false; throw; }
}

std::size_t ReplicatedStore::sstable_count() {
    EnsureUsable();
    try { return store_->sstable_count(); } catch (...) { usable_ = false; throw; }
}

}  // namespace kvstore
