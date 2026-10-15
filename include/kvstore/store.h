#pragma once

#include "kvstore/memtable.h"
#include "kvstore/manifest.h"
#include "kvstore/sstable.h"

#include <cstddef>
#include <condition_variable>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

namespace kvstore {

enum class WalRecoveryMode {
    Strict,
    TruncateIncompleteTail,
};

// Thread-safe Store operations; callers must finish before destruction starts.
// The base path's parent directory must already exist.
// The base path also names its manifest, lock, SSTables and numbered WALs.
// Successful writes synchronize the WAL before updating the MemTable.
class Store {
public:
    explicit Store(const std::string& wal_path,
                   WalRecoveryMode recovery = WalRecoveryMode::Strict,
                   std::size_t flush_threshold_bytes = 4 * 1024 * 1024,
                   std::size_t compaction_threshold_tables = 4,
                   std::size_t block_cache_capacity_bytes = kDefaultBlockCacheBytes);
    ~Store();

    // A file descriptor and its lock must have exactly one owner.
    Store(const Store&) = delete;
    Store& operator=(const Store&) = delete;
    Store(Store&&) = delete;
    Store& operator=(Store&&) = delete;

    // Key + value must fit within the WAL's 1 MiB payload limit.
    void Put(const std::string& key, const std::string& value);
    std::optional<std::string> Get(const std::string& key) const;
    bool Delete(const std::string& key);

    // Freeze pending entries and wait until their SSTable is durably published.
    void Flush();
    // Rotate WAL and queue a frozen table; may wait for the one frozen slot.
    // Threshold 0 disables automatic freezing, but manual requests still work.
    void RequestFlush();
    // Queue a background merge and wait for this request to finish.
    void Compact();
    // Queue without waiting. Threshold 0 disables automatic compaction after Flush.
    void RequestCompaction();
    // Wait for flushes queued before this call, then capture and wait for compaction requests.
    // This includes automatic compaction scheduled by those flushes; failures propagate.
    void WaitForBackgroundWork();
    std::size_t sstable_count() const;
    // Encoded entry sizes in the active/frozen tables; not allocator or map-node memory.
    std::size_t memtable_bytes() const;
    std::size_t frozen_memtable_bytes() const;
    BlockCacheStats block_cache_stats() const;

    std::size_t truncated_tail_bytes() const { return truncated_tail_bytes_; }

private:
    // Deterministic test seam; production has no hook or environment-controlled faults.
    friend struct StoreTestAccess;
    enum class FreezeStage {
        WalSynced, ManifestTempSynced, ManifestRenamed, ManifestDurable, MemTableFrozen,
    };
    enum class FlushStage {
        SSTableWritten, SSTableSynced, ManifestTempSynced,
        ManifestRenamed, ManifestDurable, FrozenCleared, OldWalRemoved,
    };
    enum class CompactStage {
        SSTableWritten, SSTableSynced, ManifestTempSynced, ManifestRenamed,
        ManifestDurable, ReadersSwitched, OldTableRemoved,
    };
    void Notify(FlushStage stage);
    void Notify(CompactStage stage);
    void Notify(FreezeStage stage);
    enum class Publication { Freeze, Flush, Compaction };
    void PublishManifest(const Manifest& manifest, Publication publication = Publication::Flush);
    void CleanupObsoleteFiles();
    void ApplyToMemTable(const std::string& key, const std::optional<std::string>& value);
    void MaybeFlush(std::unique_lock<std::mutex>& lock);
    // These helpers require mutex_; public wrappers acquire it exactly once.
    std::uint64_t FreezeLocked(std::unique_lock<std::mutex>& lock);
    void WaitForWriteRoomLocked(std::unique_lock<std::mutex>& lock);
    void WaitForFlushLocked(std::unique_lock<std::mutex>& lock, std::uint64_t target);
    std::optional<std::string> GetLocked(const std::string& key) const;
    std::uint64_t ScheduleCompactionLocked();
    void WaitLocked(std::unique_lock<std::mutex>& lock, std::uint64_t target);
    void FailLocked();
    void BackgroundLoop();
    void RunCompaction();
    struct FrozenTable {
        MemTable table;
        std::size_t bytes = 0;
        std::uint64_t wal_id = 0;
        std::uint64_t sstable_id = 0;
        std::uint64_t request = 0;
        std::function<void(FlushStage)> hook;
    };
    void FlushLoop();
    void RunFlush(const std::shared_ptr<FrozenTable>& frozen);
    std::string WalPath(std::uint64_t id) const;
    std::string SSTablePath(std::uint64_t id) const;
    std::unique_ptr<SSTableReader> OpenSSTable(std::uint64_t id) const;
    void Replay(int fd, MemTable& table, std::size_t& table_bytes, WalRecoveryMode recovery);
    void AppendAndSync(std::string_view bytes);
    void EnsureUsable() const;

    int fd_ = -1;
    int lock_fd_ = -1;
    std::string base_path_;
    Manifest manifest_;
    std::shared_ptr<BlockCache> block_cache_;
    std::vector<std::unique_ptr<SSTableReader>> sstables_;
    MemTable table_;
    std::shared_ptr<FrozenTable> frozen_;
    std::size_t table_bytes_ = 0;
    std::size_t flush_threshold_bytes_;
    std::size_t compaction_threshold_tables_;
    std::function<void(FlushStage)> flush_hook_;
    std::function<void(FlushStage)> active_flush_hook_;
    std::function<void(FreezeStage)> freeze_hook_;
    std::function<void(CompactStage)> compact_hook_;
    // Copied under mutex_ at job start; thereafter used only by the worker.
    std::function<void(CompactStage)> active_compact_hook_;
    bool usable_ = true;
    std::size_t truncated_tail_bytes_ = 0;
    mutable std::mutex mutex_;
    std::condition_variable work_ready_;
    std::condition_variable work_done_;
    std::condition_variable flush_ready_;
    std::uint64_t flush_requested_ = 0;
    std::uint64_t flush_completed_ = 0;
    std::uint64_t requested_ = 0;
    std::uint64_t completed_ = 0;
    bool stopping_ = false;
    bool flush_stopped_ = false;
    std::exception_ptr background_error_;
    std::thread worker_;
    std::thread flush_worker_;
};

}  // namespace kvstore
