#include "kvstore/store.h"
#include "kvstore/wal_record.h"
#include "common/file_io.h"

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

void Apply(MemTable& table, std::size_t& bytes, const std::string& key,
           const std::optional<std::string>& value) {
    const auto old = table.Lookup(key);
    const auto old_bytes = old ? EntryBytes(old->key, old->value) : 0;
    const auto new_bytes = EntryBytes(key, value);
    if (new_bytes > std::numeric_limits<std::size_t>::max() - (bytes - old_bytes)) {
        throw std::length_error("MemTable byte accounting overflow");
    }
    if (value) { table.Put(key, *value); } else { table.Delete(key); }
    bytes = bytes - old_bytes + new_bytes;
}

}  // namespace

Store::Store(const std::string& wal_path, WalRecoveryMode recovery, std::size_t flush_threshold_bytes,
             std::size_t compaction_threshold_tables, std::size_t block_cache_capacity_bytes)
    : base_path_(wal_path), block_cache_(std::make_shared<BlockCache>(block_cache_capacity_bytes)),
      flush_threshold_bytes_(flush_threshold_bytes),
      compaction_threshold_tables_(compaction_threshold_tables) {
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
            sstables_.push_back(OpenSSTable(id));
        }
        if (manifest_.frozen_wal_id) {
            frozen_ = std::make_shared<FrozenTable>();
            frozen_->wal_id = *manifest_.frozen_wal_id;
            detail::OwnedFd old_wal(detail::OpenRegular(WalPath(frozen_->wal_id), O_RDONLY));
            // A frozen WAL was synced before rotation; never truncate it implicitly.
            Replay(old_wal.get(), frozen_->table, frozen_->bytes, WalRecoveryMode::Strict);
            if (frozen_->bytes == 0) { throw std::runtime_error("Frozen WAL must contain pending entries"); }
            if (manifest_.next_file_id == std::numeric_limits<std::uint64_t>::max()) {
                throw std::length_error("Store file IDs exhausted");
            }
            frozen_->sstable_id = manifest_.next_file_id++;
            frozen_->request = ++flush_requested_;
        }
        // An existing manifest must refer to an existing WAL, never a recreated empty one.
        fd_ = detail::OpenRegular(WalPath(manifest_.wal_id),
                                  O_RDWR | O_APPEND | (has_manifest ? 0 : O_CREAT));
        detail::LockExclusive(fd_);
        Replay(fd_, table_, table_bytes_, recovery);
        detail::SyncFile(fd_);
        detail::SyncParentDirectory(base_path_);
        if (!has_manifest) {
            // Bootstrap metadata before accepting writes or creating numbered data files.
            detail::RemoveFile(base_path_ + ".manifest.tmp");
            PublishManifest(manifest_);
        }
        // Validate every live file first; only then remove unpublished/obsolete files.
        CleanupObsoleteFiles();
        worker_ = std::thread(&Store::BackgroundLoop, this);
        flush_worker_ = std::thread(&Store::FlushLoop, this);
    } catch (...) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            stopping_ = true;
            if (!flush_worker_.joinable()) { flush_stopped_ = true; }
            work_ready_.notify_one();
            flush_ready_.notify_one();
        }
        if (flush_worker_.joinable()) { flush_worker_.join(); }
        if (worker_.joinable()) { worker_.join(); }
        if (fd_ != -1) { ::close(fd_); fd_ = -1; }
        if (lock_fd_ != -1) { ::close(lock_fd_); lock_fd_ = -1; }
        throw;
    }
}

Store::~Store() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        stopping_ = true;
        work_ready_.notify_one();
        flush_ready_.notify_one();
    }
    // Join without holding mutex_: the worker needs it to finish queued jobs.
    if (flush_worker_.joinable()) { flush_worker_.join(); }
    if (worker_.joinable()) { worker_.join(); }
    if (fd_ != -1) { ::close(fd_); }
    if (lock_fd_ != -1) { ::close(lock_fd_); }
}

std::string Store::WalPath(std::uint64_t id) const {
    return id == 0 ? base_path_ : base_path_ + ".wal." + std::to_string(id);
}

std::string Store::SSTablePath(std::uint64_t id) const {
    return base_path_ + ".sst." + std::to_string(id);
}

std::unique_ptr<SSTableReader> Store::OpenSSTable(std::uint64_t id) const {
    SSTableReadOptions options;
    options.block_cache = block_cache_;
    return std::make_unique<SSTableReader>(SSTablePath(id), std::move(options));
}

void Store::Notify(FlushStage stage) {
    if (active_flush_hook_) { active_flush_hook_(stage); }
}

void Store::Notify(CompactStage stage) {
    if (active_compact_hook_) { active_compact_hook_(stage); }
}

void Store::Notify(FreezeStage stage) {
    if (freeze_hook_) { freeze_hook_(stage); }
}

void Store::PublishManifest(const Manifest& manifest, Publication publication) {
    const auto bytes = EncodeManifest(manifest);
    const auto path = base_path_ + ".manifest";
    const auto temporary = path + ".tmp";
    {
        detail::OwnedFd fd(detail::OpenRegular(temporary, O_WRONLY | O_CREAT | O_EXCL));
        detail::WriteAll(fd.get(), bytes);
        detail::SyncFile(fd.get());
    }
    const auto notify = [this, publication](FlushStage flush, CompactStage compact, FreezeStage freeze) {
        switch (publication) {
        case Publication::Freeze: Notify(freeze); break;
        case Publication::Flush: Notify(flush); break;
        case Publication::Compaction: Notify(compact); break;
        }
    };
    notify(FlushStage::ManifestTempSynced, CompactStage::ManifestTempSynced, FreezeStage::ManifestTempSynced);
    if (::rename(temporary.c_str(), path.c_str()) == -1) { detail::ThrowSystemError("Publish manifest"); }
    notify(FlushStage::ManifestRenamed, CompactStage::ManifestRenamed, FreezeStage::ManifestRenamed);
    // rename changes visibility atomically; directory synchronization makes it durable.
    detail::SyncParentDirectory(path);
    notify(FlushStage::ManifestDurable, CompactStage::ManifestDurable, FreezeStage::ManifestDurable);
}

void Store::CleanupObsoleteFiles() {
    std::set<std::uint64_t> live(manifest_.sstable_ids.begin(), manifest_.sstable_ids.end());
    const auto name = std::filesystem::path(base_path_).filename().string();
    std::vector<std::string> obsolete;
    for (const auto& file : std::filesystem::directory_iterator(Parent(base_path_))) {
        const auto filename = file.path().filename().string();
        const auto table_id = FileId(filename, name + ".sst.");
        const auto wal_id = FileId(filename, name + ".wal.");
        if ((table_id && live.count(*table_id) == 0) ||
            (wal_id && *wal_id != manifest_.wal_id &&
             (!manifest_.frozen_wal_id || *wal_id != *manifest_.frozen_wal_id))) {
            obsolete.push_back(file.path().string());
        }
    }
    if (manifest_.wal_id != 0 && (!manifest_.frozen_wal_id || *manifest_.frozen_wal_id != 0)) {
        obsolete.push_back(base_path_);
    }
    obsolete.push_back(base_path_ + ".manifest.tmp");
    for (const auto& path : obsolete) { detail::RemoveFile(path); }
    detail::SyncParentDirectory(base_path_);
}

void Store::ApplyToMemTable(const std::string& key, const std::optional<std::string>& value) {
    Apply(table_, table_bytes_, key, value);
}
void Store::Replay(int fd, MemTable& table, std::size_t& table_bytes, WalRecoveryMode recovery) {
    std::string pending;
    off_t valid_bytes = 0;
    char chunk[64 * 1024];
    for (;;) {
        const auto count = ::read(fd, chunk, sizeof(chunk));
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
                Apply(table, table_bytes, record.key, record.value);
            } else {
                Apply(table, table_bytes, record.key, std::nullopt);
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
            result = ::ftruncate(fd, valid_bytes);
        } while (result == -1 && errno == EINTR);
        if (result == -1) {
            detail::ThrowSystemError("Truncate incomplete WAL tail");
        }
        truncated_tail_bytes_ = pending.size();
    }
}

void Store::EnsureUsable() const {
    if (background_error_) { std::rethrow_exception(background_error_); }
    if (!usable_) { throw std::logic_error("Store is unusable after a write failure; close and reopen it"); }
}

void Store::FailLocked() {
    usable_ = false;
    work_ready_.notify_one();
    flush_ready_.notify_one();
    work_done_.notify_all();
}

void Store::AppendAndSync(std::string_view bytes) {
    detail::WriteAll(fd_, bytes);
    detail::SyncFile(fd_);
}

void Store::MaybeFlush(std::unique_lock<std::mutex>& lock) {
    if (flush_threshold_bytes_ != 0 && table_bytes_ >= flush_threshold_bytes_) { FreezeLocked(lock); }
}

void Store::Put(const std::string& key, const std::string& value) {
    std::unique_lock<std::mutex> lock(mutex_);
    EnsureUsable();
    const auto bytes = EncodeWalRecord({WalRecordType::Put, key, value});
    WaitForWriteRoomLocked(lock);
    try {
        AppendAndSync(bytes);
        ApplyToMemTable(key, value);
        MaybeFlush(lock);
    } catch (...) {
        FailLocked();
        throw;
    }
}

std::optional<std::string> Store::Get(const std::string& key) const {
    std::lock_guard<std::mutex> lock(mutex_);
    EnsureUsable();
    return GetLocked(key);
}

std::optional<std::string> Store::GetLocked(const std::string& key) const {
    const auto memory = table_.Lookup(key);
    if (memory) { return memory->value; }  // Includes tombstones: stop, do not search older files.
    if (frozen_) {
        const auto frozen = frozen_->table.Lookup(key);
        if (frozen) { return frozen->value; }
    }
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
    std::unique_lock<std::mutex> lock(mutex_);
    EnsureUsable();
    const auto bytes = EncodeWalRecord({WalRecordType::Delete, key, ""});
    WaitForWriteRoomLocked(lock);
    const bool existed = GetLocked(key).has_value();  // Check the complete view, not only the MemTable.
    try {
        AppendAndSync(bytes);
        ApplyToMemTable(key, std::nullopt);
        MaybeFlush(lock);
        return existed;
    } catch (...) {
        FailLocked();
        throw;
    }
}

std::size_t Store::sstable_count() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return manifest_.sstable_ids.size();
}

std::size_t Store::memtable_bytes() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return table_bytes_;
}

std::size_t Store::frozen_memtable_bytes() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return frozen_ ? frozen_->bytes : 0;
}

BlockCacheStats Store::block_cache_stats() const {
    return block_cache_->stats();
}

}  // namespace kvstore
