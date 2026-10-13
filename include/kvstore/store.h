#pragma once

#include "kvstore/memtable.h"
#include "kvstore/manifest.h"
#include "kvstore/sstable.h"

#include <cstddef>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace kvstore {

enum class WalRecoveryMode {
    Strict,
    TruncateIncompleteTail,
};

// Single-threaded store. The base path's parent directory must already exist.
// The base path also names its manifest, lock, SSTables and numbered WALs.
// Successful writes synchronize the WAL before updating the MemTable.
class Store {
public:
    explicit Store(const std::string& wal_path,
                   WalRecoveryMode recovery = WalRecoveryMode::Strict,
                   std::size_t flush_threshold_bytes = 4 * 1024 * 1024);
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

    // Synchronously publish pending entries as a new SSTable and rotate the WAL.
    // No pending entries means no work. Threshold 0 disables automatic flushing.
    void Flush();
    // Merge all current SSTables synchronously. Pending MemTable/WAL stay unchanged.
    // Full coverage and the absence of snapshots allow obsolete tombstones to be dropped.
    void Compact();
    std::size_t sstable_count() const { return manifest_.sstable_ids.size(); }
    // Encoded entry sizes, including tombstones; not allocator or map-node memory.
    std::size_t memtable_bytes() const { return table_bytes_; }

    std::size_t truncated_tail_bytes() const { return truncated_tail_bytes_; }

private:
    // Deterministic test seam; production has no hook or environment-controlled faults.
    friend struct StoreTestAccess;
    enum class FlushStage {
        SSTableWritten, SSTableSynced, WalSynced, ManifestTempSynced,
        ManifestRenamed, ManifestDurable, WalSwitched, OldWalRemoved,
    };
    enum class CompactStage {
        SSTableWritten, SSTableSynced, ManifestTempSynced, ManifestRenamed,
        ManifestDurable, ReadersSwitched, OldTableRemoved,
    };
    void Notify(FlushStage stage);
    void Notify(CompactStage stage);
    void PublishManifest(const Manifest& manifest, bool for_compaction = false);
    void CleanupObsoleteFiles();
    void ApplyToMemTable(const std::string& key, const std::optional<std::string>& value);
    void MaybeFlush();
    std::string WalPath(std::uint64_t id) const;
    std::string SSTablePath(std::uint64_t id) const;
    void Replay(WalRecoveryMode recovery);
    void AppendAndSync(std::string_view bytes);
    void EnsureUsable() const;

    int fd_ = -1;
    int lock_fd_ = -1;
    std::string base_path_;
    Manifest manifest_;
    std::vector<std::unique_ptr<SSTableReader>> sstables_;
    MemTable table_;
    std::size_t table_bytes_ = 0;
    std::size_t flush_threshold_bytes_;
    std::function<void(FlushStage)> flush_hook_;
    std::function<void(CompactStage)> compact_hook_;
    bool usable_ = true;
    std::size_t truncated_tail_bytes_ = 0;
};

}  // namespace kvstore
