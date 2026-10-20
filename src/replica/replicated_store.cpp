#include "kvstore/replicated_store.h"
#include "common/file_io.h"
#include "replica_checkpoint.h"

#include <filesystem>
#include <stdexcept>
#include <utility>

#include <fcntl.h>
#include <unistd.h>

namespace kvstore {

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
    : config_(std::move(config)) {
    // Validate configuration and timeouts before creating any files.
    raft::Node validation(config_, {}, options.election);
    if (directory.empty() || directory.find('\0') != std::string::npos ||
        (mode != raft::StorageOpenMode::CreateNew && mode != raft::StorageOpenMode::OpenExisting) ||
        (options.store_recovery != WalRecoveryMode::Strict && options.store_recovery != WalRecoveryMode::TruncateIncompleteTail) ||
        (options.raft_recovery != raft::StorageRecoveryMode::Strict &&
         options.raft_recovery != raft::StorageRecoveryMode::TruncateIncompleteTail)) {
        throw std::invalid_argument("Invalid replica directory, open mode or recovery policy");
    }
    directory_ = std::filesystem::absolute(directory).lexically_normal().string();
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
    if (!creating) {
        checkpoint = detail::DecodeReplicaCheckpoint(
            detail::ReadFile(checkpoint_path, detail::kReplicaCheckpointBytes), config_);
        if (!detail::PathExists(directory_ + "/data.manifest") || !detail::PathExists(directory_ + "/data.lock")) {
            throw std::runtime_error("Replica Store metadata is missing; refusing to bootstrap empty data");
        }
    }
    auto storage = std::make_unique<raft::Storage>(directory_ + "/raft", config_, mode, options.raft_recovery);
    const auto& log = storage->state().log;
    if (checkpoint.index > log.size() ||
        (checkpoint.index != 0 && log[checkpoint.index - 1].term != checkpoint.term)) {
        throw std::runtime_error("Replica commit checkpoint exceeds or disagrees with the Raft log");
    }
    std::uint32_t fingerprint = 0;
    for (raft::LogIndex i = 0; i < checkpoint.index; ++i) {
        fingerprint = detail::ExtendReplicaFingerprint(fingerprint, log[i]);
    }
    if (fingerprint != checkpoint.fingerprint) { throw std::runtime_error("Replica committed prefix fingerprint mismatch"); }
    node_ = std::make_unique<raft::Node>(config_, std::move(storage), options.election, checkpoint.index);
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
    const auto& log = node_->persistent_state().log;
    for (auto i = saved_commit_; i < index; ++i) { fingerprint = detail::ExtendReplicaFingerprint(fingerprint, log[i]); }
    const auto bytes = detail::EncodeReplicaCheckpoint(config_, {index, index == 0 ? 0 : log[index - 1].term, fingerprint});
    const auto path = directory_ + "/COMMIT";
    const auto temporary = path + ".tmp";
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
    saved_commit_ = index;
    prefix_fingerprint_ = fingerprint;
}

void ReplicatedStore::ApplyCommitted() {
    const auto committed = node_->status().commit_index;
    if (committed > saved_commit_) { SaveCommit(committed); }
    while (node_->status().last_applied < committed) {
        const auto index = node_->status().last_applied + 1;
        const auto& command = node_->persistent_state().log[index - 1].command;
        switch (command.type) {
            case raft::CommandType::Put: store_->Put(command.key, command.value); break;
            case raft::CommandType::Delete: store_->Delete(command.key); break;
            case raft::CommandType::NoOp: break;
        }
        Notify(ApplyStage::CommandStored, index);
        node_->MarkApplied(index);
        Notify(ApplyStage::AppliedAdvanced, index);
    }
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
    try { node_->Step(message); ApplyCommitted(); } catch (...) { usable_ = false; throw; }
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
    return proposal.index != 0 && proposal.index <= node_->status().last_applied &&
        node_->persistent_state().log[proposal.index - 1].term == proposal.term;
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
