#include "kvstore/store.h"
#include "file_io.h"

#include <limits>
#include <stdexcept>
#include <utility>

#include <fcntl.h>
#include <unistd.h>

namespace kvstore {

void Store::WaitForWriteRoomLocked(std::unique_lock<std::mutex>& lock) {
    if (flush_threshold_bytes_ == 0) { return; }
    // Do not let other writers keep growing a full active table while it waits to freeze.
    work_done_.wait(lock, [this] {
        return !usable_ || !frozen_ || table_bytes_ < flush_threshold_bytes_;
    });
    EnsureUsable();
    if (table_bytes_ >= flush_threshold_bytes_) {
        // A previous writer may still be waiting to freeze this table. Help before appending.
        FreezeLocked(lock);
    }
}

std::uint64_t Store::FreezeLocked(std::unique_lock<std::mutex>& lock) {
    if (table_bytes_ == 0) { return flush_requested_; }
    // Exactly one frozen slot. Waiting releases mutex_ so readers and workers can progress.
    work_done_.wait(lock, [this] { return !usable_ || !frozen_; });
    EnsureUsable();
    if (table_bytes_ == 0) { return flush_requested_; }
    if (manifest_.next_file_id > std::numeric_limits<std::uint64_t>::max() - 2 ||
        flush_requested_ == std::numeric_limits<std::uint64_t>::max()) {
        throw std::length_error("Store file or flush request IDs exhausted");
    }
    auto frozen = std::make_shared<FrozenTable>();
    frozen->bytes = table_bytes_;
    frozen->wal_id = manifest_.wal_id;
    frozen->sstable_id = manifest_.next_file_id;
    frozen->request = flush_requested_ + 1;
    frozen->hook = flush_hook_;
    Manifest next = manifest_;
    next.frozen_wal_id = manifest_.wal_id;
    next.wal_id = next.next_file_id + 1;
    next.next_file_id += 2;
    EncodeManifest(next);
    try {
        detail::OwnedFd new_wal(detail::OpenRegular(WalPath(next.wal_id), O_RDWR | O_APPEND | O_CREAT | O_EXCL));
        detail::LockExclusive(new_wal.get());
        detail::SyncFile(new_wal.get());
        detail::SyncParentDirectory(base_path_);
        Notify(FreezeStage::WalSynced);
        // Publish BOTH recovery WALs before allowing writes into the new one.
        PublishManifest(next, Publication::Freeze);
        const auto old_fd = fd_;
        manifest_ = std::move(next);
        frozen->table.Swap(table_);
        table_bytes_ = 0;
        frozen_ = std::move(frozen);
        fd_ = new_wal.Release();
        ::close(old_fd);  // Keep its file: frozen_->table still depends on it for recovery.
        flush_requested_ = frozen_->request;
        Notify(FreezeStage::MemTableFrozen);
        flush_ready_.notify_one();
        return flush_requested_;
    } catch (...) {
        FailLocked();
        throw;
    }
}

void Store::RequestFlush() {
    std::unique_lock<std::mutex> lock(mutex_);
    EnsureUsable();
    FreezeLocked(lock);
}

void Store::WaitForFlushLocked(std::unique_lock<std::mutex>& lock, std::uint64_t target) {
    work_done_.wait(lock, [this, target] { return !usable_ || flush_completed_ >= target; });
    EnsureUsable();
}

void Store::Flush() {
    std::unique_lock<std::mutex> lock(mutex_);
    EnsureUsable();
    const auto target = FreezeLocked(lock);
    WaitForFlushLocked(lock, target);
}

void Store::FlushLoop() {
    for (;;) {
        try {
            std::shared_ptr<FrozenTable> frozen;
            {
                std::unique_lock<std::mutex> lock(mutex_);
                flush_ready_.wait(lock, [this] { return stopping_ || !usable_ || frozen_; });
                if (!usable_ || (stopping_ && !frozen_)) {
                    flush_stopped_ = true;
                    work_ready_.notify_one();
                    return;
                }
                frozen = frozen_;
                active_flush_hook_ = frozen->hook;
            }
            RunFlush(frozen);
            const auto completed = frozen->request;
            frozen.reset();
            {
                std::lock_guard<std::mutex> lock(mutex_);
                flush_completed_ = completed;
                active_flush_hook_ = {};
                work_done_.notify_all();
            }
        } catch (...) {
            std::lock_guard<std::mutex> lock(mutex_);
            if (!background_error_) { background_error_ = std::current_exception(); }
            FailLocked();
            flush_stopped_ = true;
            work_ready_.notify_one();
            return;
        }
    }
}

void Store::RunFlush(const std::shared_ptr<FrozenTable>& frozen) {
    const auto path = SSTablePath(frozen->sstable_id);
    {
        SSTableWriter writer(path);
        // The frozen map never changes; foreground readers may inspect it concurrently.
        for (const auto& entry : frozen->table.Snapshot()) {
            writer.Append({entry.value ? SSTableEntryType::Value : SSTableEntryType::Tombstone,
                           entry.key, entry.value.value_or("")});
        }
        writer.Finish();
    }
    Notify(FlushStage::SSTableWritten);
    detail::SyncFile(path);
    detail::SyncParentDirectory(base_path_);
    Notify(FlushStage::SSTableSynced);
    auto reader = std::make_unique<SSTableReader>(path);

    std::lock_guard<std::mutex> lock(mutex_);
    try {
        EnsureUsable();
        if (frozen_ != frozen || manifest_.frozen_wal_id != frozen->wal_id) {
            throw std::logic_error("Frozen MemTable and recovery WAL must agree");
        }
        // Compaction may have replaced older SSTables. Append to CURRENT metadata.
        Manifest next = manifest_;
        next.sstable_ids.push_back(frozen->sstable_id);
        next.frozen_wal_id.reset();
        EncodeManifest(next);
        sstables_.reserve(next.sstable_ids.size());
        PublishManifest(next, Publication::Flush);
        manifest_ = std::move(next);
        sstables_.push_back(std::move(reader));
        frozen_.reset();
        Notify(FlushStage::FrozenCleared);
        detail::RemoveFile(WalPath(frozen->wal_id));
        Notify(FlushStage::OldWalRemoved);
        detail::SyncParentDirectory(base_path_);
        if (compaction_threshold_tables_ != 0 &&
            manifest_.sstable_ids.size() >= compaction_threshold_tables_) {
            ScheduleCompactionLocked();
        }
    } catch (...) {
        // A failed publication must stop both workers before releasing the state lock.
        background_error_ = std::current_exception();
        FailLocked();
        throw;
    }
}

}  // namespace kvstore
