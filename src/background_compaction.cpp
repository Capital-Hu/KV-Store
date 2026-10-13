#include "kvstore/store.h"
#include "kvstore/compaction.h"
#include "file_io.h"

#include <algorithm>
#include <limits>
#include <stdexcept>
#include <utility>

namespace kvstore {

std::uint64_t Store::ScheduleCompactionLocked() {
    if (requested_ == std::numeric_limits<std::uint64_t>::max()) {
        throw std::length_error("Compaction request IDs exhausted");
    }
    ++requested_;
    work_ready_.notify_one();
    return requested_;
}

void Store::RequestCompaction() {
    std::lock_guard<std::mutex> lock(mutex_);
    EnsureUsable();
    ScheduleCompactionLocked();
}

void Store::WaitLocked(std::unique_lock<std::mutex>& lock, std::uint64_t target) {
    // wait releases mutex_ while sleeping, then reacquires it before returning.
    work_done_.wait(lock, [this, target] { return completed_ >= target || !usable_; });
    EnsureUsable();
}

void Store::WaitForBackgroundWork() {
    std::unique_lock<std::mutex> lock(mutex_);
    EnsureUsable();
    WaitLocked(lock, requested_);
}

void Store::Compact() {
    std::unique_lock<std::mutex> lock(mutex_);
    EnsureUsable();
    const auto target = ScheduleCompactionLocked();
    WaitLocked(lock, target);
}

void Store::BackgroundLoop() {
    for (;;) {
        std::uint64_t target;
        try {
            {
                std::unique_lock<std::mutex> lock(mutex_);
                work_ready_.wait(lock, [this] {
                    return stopping_ || !usable_ || requested_ > completed_;
                });
                if (!usable_ || (stopping_ && requested_ == completed_)) { return; }
                // Requests already queued can share one pass; newer requests get another pass.
                target = requested_;
                active_compact_hook_ = compact_hook_;
            }
            RunCompaction();
            {
                std::lock_guard<std::mutex> lock(mutex_);
                completed_ = target;
                active_compact_hook_ = {};
                work_done_.notify_all();
            }
        } catch (...) {
            std::lock_guard<std::mutex> lock(mutex_);
            if (!background_error_) { background_error_ = std::current_exception(); }
            FailLocked();
            return;
        }
    }
}

void Store::RunCompaction() {
    std::vector<std::uint64_t> old_ids;
    std::uint64_t output_id;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        EnsureUsable();
        if (manifest_.sstable_ids.empty()) { return; }
        if (manifest_.next_file_id == std::numeric_limits<std::uint64_t>::max()) {
            throw std::length_error("Store file IDs exhausted");
        }
        old_ids = manifest_.sstable_ids;
        // Reserve before unlocking so foreground Flush cannot reuse this file ID.
        output_id = manifest_.next_file_id++;
    }
    const auto output_path = SSTablePath(output_id);
    std::vector<std::string> inputs;
    inputs.reserve(old_ids.size());
    for (const auto id : old_ids) { inputs.push_back(SSTablePath(id)); }

    // Use independent readers outside mutex_. Foreground reads keep their own cursors.
    // All older disk versions are covered; newer files cannot need these tombstones.
    MergeSSTables(inputs, output_path, true);
    Notify(CompactStage::SSTableWritten);
    detail::SyncFile(output_path);
    detail::SyncParentDirectory(base_path_);
    Notify(CompactStage::SSTableSynced);
    auto reader = std::make_unique<SSTableReader>(output_path);

    std::lock_guard<std::mutex> lock(mutex_);
    try {
        EnsureUsable();
        if (manifest_.sstable_ids.size() < old_ids.size() ||
            !std::equal(old_ids.begin(), old_ids.end(), manifest_.sstable_ids.begin())) {
            throw std::logic_error("Compaction inputs must remain the oldest SSTable prefix");
        }
        // Rebase onto CURRENT metadata: Flush may have appended tables and rotated WAL.
        Manifest next = manifest_;
        next.sstable_ids.erase(next.sstable_ids.begin(), next.sstable_ids.begin() + old_ids.size());
        next.sstable_ids.insert(next.sstable_ids.begin(), output_id);
        EncodeManifest(next);
        std::vector<std::unique_ptr<SSTableReader>> replacement;
        replacement.reserve(next.sstable_ids.size());
        replacement.push_back(std::move(reader));
        PublishManifest(next, true);
        for (std::size_t i = old_ids.size(); i < sstables_.size(); ++i) {
            replacement.push_back(std::move(sstables_[i]));
        }
        manifest_ = std::move(next);
        sstables_.swap(replacement);
        replacement.clear();  // No foreground query can still be using an old reader.
        Notify(CompactStage::ReadersSwitched);
        for (const auto id : old_ids) {
            detail::RemoveFile(SSTablePath(id));
            Notify(CompactStage::OldTableRemoved);
        }
        detail::SyncParentDirectory(base_path_);
    } catch (...) {
        // Poison BEFORE unlocking: disk publication may already have succeeded.
        background_error_ = std::current_exception();
        FailLocked();
        throw;
    }
}

}  // namespace kvstore
