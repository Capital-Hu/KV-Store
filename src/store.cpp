#include "kvstore/store.h"
#include "kvstore/wal_record.h"
#include "kvstore/compaction.h"
#include "file_io.h"

#include <cerrno>
#include <charconv>
#include <filesystem>
#include <limits>
#include <set>
#include <stdexcept>
#include <utility>

#include <fcntl.h>
#include <unistd.h>

namespace kvstore {
namespace {

// Recognize only canonical numbered files in this Store's reserved namespace.
std::optional<std::uint64_t> FileId(const std::string& name, const std::string& prefix) {
    if (name.compare(0, prefix.size(), prefix) != 0) { return std::nullopt; }
    const auto digits = std::string_view(name).substr(prefix.size());
    std::uint64_t id = 0;
    const auto parsed = std::from_chars(digits.data(), digits.data() + digits.size(), id);
    if (parsed.ec != std::errc{} || parsed.ptr != digits.data() + digits.size() ||
        id == 0 || std::to_string(id) != digits) {
        return std::nullopt;
    }
    return id;
}

std::filesystem::path Parent(const std::string& base) {
    const auto parent = std::filesystem::path(base).parent_path();
    return parent.empty() ? std::filesystem::path(".") : parent;
}

bool HasNumberedFiles(const std::string& base) {
    const auto name = std::filesystem::path(base).filename().string();
    for (const auto& file : std::filesystem::directory_iterator(Parent(base))) {
        const auto filename = file.path().filename().string();
        if (FileId(filename, name + ".sst.") || FileId(filename, name + ".wal.")) { return true; }
    }
    return false;
}

std::size_t EntryBytes(const std::string& key, const std::optional<std::string>& value) {
    return 17 + key.size() + (value ? value->size() : 0);
}

}  // namespace

Store::Store(const std::string& wal_path, WalRecoveryMode recovery, std::size_t flush_threshold_bytes)
    : base_path_(wal_path), flush_threshold_bytes_(flush_threshold_bytes) {
    if (wal_path.empty() || wal_path.find('\0') != std::string::npos) {
        throw std::invalid_argument("WAL path must be nonempty and contain no null bytes");
    }
    if (recovery != WalRecoveryMode::Strict && recovery != WalRecoveryMode::TruncateIncompleteTail) {
        throw std::invalid_argument("Unknown WAL recovery mode");
    }
    // Anchor relative paths now, so a later chdir cannot redirect flush or cleanup.
    base_path_ = std::filesystem::absolute(wal_path).string();
    try {
        // This lock survives WAL rotation. Never unlink the lock file while in use.
        lock_fd_ = detail::OpenRegular(base_path_ + ".lock", O_RDWR | O_CREAT);
        detail::LockExclusive(lock_fd_);
        const bool has_manifest = detail::PathExists(base_path_ + ".manifest");
        if (has_manifest) {
            manifest_ = DecodeManifest(detail::ReadFile(base_path_ + ".manifest", kMaxManifestBytes));
        } else if (HasNumberedFiles(base_path_)) {
            // Do not silently bootstrap an empty Store after metadata has been lost.
            throw std::runtime_error("Manifest is missing but numbered store files exist");
        }
        for (const auto id : manifest_.sstable_ids) {
            sstables_.push_back(std::make_unique<SSTableReader>(SSTablePath(id)));
        }
        // An existing manifest must refer to an existing WAL, never a recreated empty one.
        fd_ = detail::OpenRegular(WalPath(manifest_.wal_id),
                                  O_RDWR | O_APPEND | (has_manifest ? 0 : O_CREAT));
        detail::LockExclusive(fd_);
        Replay(recovery);
        detail::SyncFile(fd_);
        detail::SyncParentDirectory(base_path_);
        if (!has_manifest) {
            // Bootstrap metadata before accepting writes or creating numbered data files.
            detail::RemoveFile(base_path_ + ".manifest.tmp");
            PublishManifest(manifest_);
        }
        // Validate every live file first; only then remove unpublished/obsolete files.
        CleanupObsoleteFiles();
    } catch (...) {
        if (fd_ != -1) { ::close(fd_); fd_ = -1; }
        if (lock_fd_ != -1) { ::close(lock_fd_); lock_fd_ = -1; }
        throw;
    }
}

Store::~Store() {
    if (fd_ != -1) { ::close(fd_); }
    if (lock_fd_ != -1) { ::close(lock_fd_); }
}

std::string Store::WalPath(std::uint64_t id) const {
    return id == 0 ? base_path_ : base_path_ + ".wal." + std::to_string(id);
}

std::string Store::SSTablePath(std::uint64_t id) const {
    return base_path_ + ".sst." + std::to_string(id);
}

void Store::Notify(FlushStage stage) {
    if (flush_hook_) { flush_hook_(stage); }
}

void Store::Notify(CompactStage stage) {
    if (compact_hook_) { compact_hook_(stage); }
}

void Store::PublishManifest(const Manifest& manifest, bool for_compaction) {
    const auto bytes = EncodeManifest(manifest);
    const auto path = base_path_ + ".manifest";
    const auto temporary = path + ".tmp";
    {
        detail::OwnedFd fd(detail::OpenRegular(temporary, O_WRONLY | O_CREAT | O_EXCL));
        detail::WriteAll(fd.get(), bytes);
        detail::SyncFile(fd.get());
    }
    if (for_compaction) { Notify(CompactStage::ManifestTempSynced); }
    else { Notify(FlushStage::ManifestTempSynced); }
    if (::rename(temporary.c_str(), path.c_str()) == -1) { detail::ThrowSystemError("Publish manifest"); }
    if (for_compaction) { Notify(CompactStage::ManifestRenamed); }
    else { Notify(FlushStage::ManifestRenamed); }
    // rename changes visibility atomically; directory synchronization makes it durable.
    detail::SyncParentDirectory(path);
    if (for_compaction) { Notify(CompactStage::ManifestDurable); }
    else { Notify(FlushStage::ManifestDurable); }
}

void Store::CleanupObsoleteFiles() {
    std::set<std::uint64_t> live(manifest_.sstable_ids.begin(), manifest_.sstable_ids.end());
    const auto name = std::filesystem::path(base_path_).filename().string();
    std::vector<std::string> obsolete;
    for (const auto& file : std::filesystem::directory_iterator(Parent(base_path_))) {
        const auto filename = file.path().filename().string();
        const auto table_id = FileId(filename, name + ".sst.");
        const auto wal_id = FileId(filename, name + ".wal.");
        if ((table_id && live.count(*table_id) == 0) || (wal_id && *wal_id != manifest_.wal_id)) {
            obsolete.push_back(file.path().string());
        }
    }
    if (manifest_.wal_id != 0) { obsolete.push_back(base_path_); }
    obsolete.push_back(base_path_ + ".manifest.tmp");
    for (const auto& path : obsolete) { detail::RemoveFile(path); }
    detail::SyncParentDirectory(base_path_);
}

void Store::ApplyToMemTable(const std::string& key, const std::optional<std::string>& value) {
    const auto old = table_.Lookup(key);
    const auto old_bytes = old ? EntryBytes(old->key, old->value) : 0;
    const auto new_bytes = EntryBytes(key, value);
    if (new_bytes > std::numeric_limits<std::size_t>::max() - (table_bytes_ - old_bytes)) {
        throw std::length_error("MemTable byte accounting overflow");
    }
    if (value) { table_.Put(key, *value); } else { table_.Delete(key); }
    table_bytes_ = table_bytes_ - old_bytes + new_bytes;
}
void Store::Replay(WalRecoveryMode recovery) {
    std::string pending;
    off_t valid_bytes = 0;
    char chunk[64 * 1024];
    for (;;) {
        const auto count = ::read(fd_, chunk, sizeof(chunk));
        if (count == -1) {
            if (errno == EINTR) {
                continue;
            }
            detail::ThrowSystemError("Read WAL");
        }
        if (count == 0) {
            break;
        }
        pending.append(chunk, static_cast<std::size_t>(count));
        std::size_t consumed = 0;
        while (consumed < pending.size()) {
            const auto result = DecodeWalRecord(std::string_view(pending).substr(consumed));
            if (result.status == WalDecodeStatus::Incomplete) {
                break;  // Keep the bytes and read the next chunk.
            }
            if (result.status == WalDecodeStatus::Corrupt) {
                throw std::runtime_error("Corrupt WAL record at byte " + std::to_string(valid_bytes));
            }
            const auto& record = result.record.value();
            if (record.type == WalRecordType::Put) {
                ApplyToMemTable(record.key, record.value);
            } else {
                ApplyToMemTable(record.key, std::nullopt);
            }
            consumed += result.bytes_consumed;
            valid_bytes += static_cast<off_t>(result.bytes_consumed);
        }
        pending.erase(0, consumed);
    }

    if (!pending.empty()) {
        if (recovery == WalRecoveryMode::Strict) {
            throw std::runtime_error("Incomplete WAL tail at byte " + std::to_string(valid_bytes));
        }
        // This explicit policy assumes an interrupted append caused the tail.
        // A corrupt length field can also look incomplete; do not repair implicitly.
        int result;
        do {
            result = ::ftruncate(fd_, valid_bytes);
        } while (result == -1 && errno == EINTR);
        if (result == -1) {
            detail::ThrowSystemError("Truncate incomplete WAL tail");
        }
        truncated_tail_bytes_ = pending.size();
    }
}

void Store::EnsureUsable() const {
    if (!usable_) { throw std::logic_error("Store is unusable after a write failure; close and reopen it"); }
}

void Store::AppendAndSync(std::string_view bytes) {
    detail::WriteAll(fd_, bytes);
    detail::SyncFile(fd_);
}

void Store::MaybeFlush() {
    if (flush_threshold_bytes_ != 0 && table_bytes_ >= flush_threshold_bytes_) { Flush(); }
}

void Store::Put(const std::string& key, const std::string& value) {
    EnsureUsable();
    const auto bytes = EncodeWalRecord({WalRecordType::Put, key, value});
    try {
        AppendAndSync(bytes);
        ApplyToMemTable(key, value);
        MaybeFlush();
    } catch (...) {
        usable_ = false;
        throw;
    }
}

std::optional<std::string> Store::Get(const std::string& key) const {
    EnsureUsable();
    const auto memory = table_.Lookup(key);
    if (memory) { return memory->value; }  // Includes tombstones: stop, do not search older files.
    for (auto it = sstables_.rbegin(); it != sstables_.rend(); ++it) {
        const auto entry = (*it)->Get(key);
        if (entry) {
            if (entry->type == SSTableEntryType::Tombstone) { return std::nullopt; }
            return entry->value;
        }
    }
    return std::nullopt;
}

bool Store::Delete(const std::string& key) {
    EnsureUsable();
    const auto bytes = EncodeWalRecord({WalRecordType::Delete, key, ""});
    const bool existed = Get(key).has_value();  // Check the complete view, not only the MemTable.
    try {
        AppendAndSync(bytes);
        ApplyToMemTable(key, std::nullopt);
        MaybeFlush();
        return existed;
    } catch (...) {
        usable_ = false;
        throw;
    }
}

void Store::Flush() {
    EnsureUsable();
    const auto snapshot = table_.Snapshot();
    if (snapshot.empty()) { return; }
    if (manifest_.next_file_id > std::numeric_limits<std::uint64_t>::max() - 2) {
        throw std::length_error("Store file IDs exhausted");
    }
    Manifest next = manifest_;
    const auto table_id = next.next_file_id;
    next.wal_id = table_id + 1;
    next.next_file_id += 2;
    next.sstable_ids.push_back(table_id);
    // Validate metadata and reserve in-memory space before publishing anything.
    EncodeManifest(next);
    sstables_.reserve(next.sstable_ids.size());
    std::vector<SSTableEntry> entries;
    entries.reserve(snapshot.size());
    for (const auto& entry : snapshot) {
        const auto type = entry.value ? SSTableEntryType::Value : SSTableEntryType::Tombstone;
        entries.push_back({type, entry.key, entry.value.value_or("")});
    }
    try {
        WriteSSTable(SSTablePath(table_id), entries);
        Notify(FlushStage::SSTableWritten);
        detail::SyncFile(SSTablePath(table_id));
        Notify(FlushStage::SSTableSynced);
        auto reader = std::make_unique<SSTableReader>(SSTablePath(table_id));
        detail::OwnedFd new_wal(detail::OpenRegular(WalPath(next.wal_id), O_RDWR | O_APPEND | O_CREAT | O_EXCL));
        detail::LockExclusive(new_wal.get());
        detail::SyncFile(new_wal.get());
        detail::SyncParentDirectory(base_path_);  // Persist BOTH new data-file names before publication.
        Notify(FlushStage::WalSynced);
        PublishManifest(next);
        // After the directory sync, the new file set is authoritative and durable.
        const auto old_wal_path = WalPath(manifest_.wal_id);
        const int old_fd = fd_;
        manifest_ = std::move(next);
        sstables_.push_back(std::move(reader));
        fd_ = new_wal.Release();
        ::close(old_fd);
        table_.Clear();
        table_bytes_ = 0;
        Notify(FlushStage::WalSwitched);
        detail::RemoveFile(old_wal_path);
        Notify(FlushStage::OldWalRemoved);
        detail::SyncParentDirectory(base_path_);
    } catch (...) {
        usable_ = false;
        throw;
    }
}

void Store::Compact() {
    EnsureUsable();
    if (manifest_.sstable_ids.empty()) { return; }
    if (manifest_.next_file_id == std::numeric_limits<std::uint64_t>::max()) {
        throw std::length_error("Store file IDs exhausted");
    }
    const auto old_ids = manifest_.sstable_ids;
    const auto output_path = SSTablePath(manifest_.next_file_id);
    std::vector<std::string> inputs;
    inputs.reserve(old_ids.size());
    for (const auto id : old_ids) { inputs.push_back(SSTablePath(id)); }
    Manifest next = manifest_;
    next.sstable_ids = {next.next_file_id++};
    EncodeManifest(next);
    std::vector<std::unique_ptr<SSTableReader>> replacement;
    replacement.reserve(1);
    try {
        // Every current SSTable participates. Pending memory/WAL are newer and untouched.
        MergeSSTables(inputs, output_path, true);
        Notify(CompactStage::SSTableWritten);
        detail::SyncFile(output_path);
        detail::SyncParentDirectory(base_path_);
        Notify(CompactStage::SSTableSynced);
        replacement.push_back(std::make_unique<SSTableReader>(output_path));
        PublishManifest(next, true);
        manifest_ = std::move(next);
        sstables_.swap(replacement);
        replacement.clear();  // Close old readers before reclaiming their files.
        Notify(CompactStage::ReadersSwitched);
        for (const auto id : old_ids) {
            detail::RemoveFile(SSTablePath(id));
            Notify(CompactStage::OldTableRemoved);
        }
        detail::SyncParentDirectory(base_path_);
    } catch (...) {
        usable_ = false;
        throw;
    }
}

}  // namespace kvstore
