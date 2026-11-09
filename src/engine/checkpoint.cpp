#include "kvstore/store.h"
#include "common/file_io.h"

#include <cstdio>
#include <filesystem>
#include <limits>
#include <stdexcept>

#include <fcntl.h>
#include <unistd.h>

namespace kvstore {

std::string Store::CheckpointFileName(std::size_t ordinal) {
    char name[32];
    std::snprintf(name, sizeof(name), "%06zu.sst", ordinal);
    return name;
}

std::vector<std::string> Store::CreateCheckpoint(const std::string& directory) {
    std::unique_lock<std::mutex> lock(mutex_);
    EnsureUsable();
    WaitForFlushLocked(lock, FreezeLocked(lock));
    if (table_bytes_ != 0 || frozen_) { throw std::logic_error("Checkpoint requires a quiescent writer"); }
    // Holding mutex_ keeps compaction from unlinking a table before it is linked here.
    std::vector<std::string> paths;
    paths.reserve(manifest_.sstable_ids.size());
    for (const auto id : manifest_.sstable_ids) {
        paths.push_back(directory + "/" + CheckpointFileName(paths.size()));
        detail::HardLink(SSTablePath(id), paths.back());
    }
    if (!paths.empty()) { detail::SyncParentDirectory(paths.back()); }
    return paths;
}

void Store::ResetToSnapshot(const std::string& wal_path, const std::vector<std::string>& sstables) {
    const auto base = std::filesystem::absolute(wal_path).string();
    const auto manifest_path = base + ".manifest";
    detail::OwnedFd lock(detail::OpenRegular(base + ".lock", O_RDWR | O_CREAT));
    detail::LockExclusive(lock.get());
    auto manifest = DecodeManifest(detail::ReadFile(manifest_path, kMaxManifestBytes));
    if (sstables.size() >= std::numeric_limits<std::uint64_t>::max() - manifest.next_file_id) {
        throw std::length_error("Store file IDs exhausted");
    }
    Manifest next;
    next.next_file_id = manifest.next_file_id;
    for (const auto& source : sstables) {
        const auto target = base + ".sst." + std::to_string(next.next_file_id);
        detail::RemoveFile(target);  // A crashed reset may have left this unpublished link.
        detail::HardLink(source, target);
        next.sstable_ids.push_back(next.next_file_id++);
    }
    next.wal_id = next.next_file_id++;
    const auto wal = base + ".wal." + std::to_string(next.wal_id);
    detail::RemoveFile(wal);
    {
        detail::OwnedFd file(detail::OpenRegular(wal, O_RDWR | O_APPEND | O_CREAT | O_EXCL));
        detail::SyncFile(file.get());
    }
    detail::SyncParentDirectory(base);
    const auto temporary = manifest_path + ".tmp";
    detail::RemoveFile(temporary);
    {
        detail::OwnedFd file(detail::OpenRegular(temporary, O_WRONLY | O_CREAT | O_EXCL));
        detail::WriteAll(file.get(), EncodeManifest(next));
        detail::SyncFile(file.get());
    }
    // This rename is the atomic switch; the next open removes every file it no longer lists.
    if (::rename(temporary.c_str(), manifest_path.c_str()) == -1) { detail::ThrowSystemError("Publish snapshot manifest"); }
    detail::SyncParentDirectory(manifest_path);
}

}  // namespace kvstore
