#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace kvstore {

// File IDs share a single namespace. ID 0 denotes the original WAL path.
// SSTable IDs are ordered oldest to newest; this order defines lookup priority.
struct Manifest {
    std::uint64_t wal_id = 0;
    std::uint64_t next_file_id = 1;
    std::vector<std::uint64_t> sstable_ids;
    // At most one frozen MemTable; its WAL is older than the active WAL.
    std::optional<std::uint64_t> frozen_wal_id = std::nullopt;
};

// Bound metadata allocation. Compaction will prevent unbounded file accumulation.
constexpr std::size_t kMaxManifestTables = 100000;
constexpr std::size_t kMaxManifestBytes = 36 + 8 * kMaxManifestTables;

// KVM1 | wal_id:u64 | next_file_id:u64 | count:u32 | ids:u64[count] | crc32:u32
// KVM2 adds frozen_wal_id:u64 immediately after count; used only while frozen.
// All integers are little-endian. Decode requires one complete file, no trailing bytes.
std::string EncodeManifest(const Manifest& manifest);
Manifest DecodeManifest(std::string_view bytes);

}  // namespace kvstore
