#pragma once

#include <cstdint>
#include <fstream>
#include <optional>
#include <string>
#include <vector>

namespace kvstore {

enum class SSTableEntryType {
    Value,
    Tombstone,
};

struct SSTableEntry {
    SSTableEntryType type;
    std::string key;
    std::string value;  // Tombstones must have an empty value.
};

// Write a new file; never overwrite an existing path.
// Entries must have strictly increasing, unique keys (std::string order).
// Each key + value is limited to 1 MiB, matching the WAL codec.
// Format: KVS1 | entry_count:u32 little-endian | sorted WAL-encoded records.
// Reuse the record codec, not the WAL's append/replay semantics.
// A failed write may leave an incomplete file. Atomic publication and durable
// file-set management belong to the later Store flush step, not this function.
void WriteSSTable(const std::string& path, const std::vector<SSTableEntry>& entries);

// Single-threaded reader. The file must not be modified while it is open.
// Opening validates the entire file; lookups scan it without loading a full table.
class SSTableReader {
public:
    explicit SSTableReader(const std::string& path);

    SSTableReader(const SSTableReader&) = delete;
    SSTableReader& operator=(const SSTableReader&) = delete;
    SSTableReader(SSTableReader&&) = delete;
    SSTableReader& operator=(SSTableReader&&) = delete;

    // nullopt: key absent from this file. A returned Tombstone means deleted.
    // A returned Value may contain an empty string; these are three distinct cases.
    std::optional<SSTableEntry> Get(const std::string& key);
    std::uint32_t size() const { return entry_count_; }

private:
    std::ifstream input_;  // Owns and closes the file through RAII.
    std::uint32_t entry_count_ = 0;
};

}  // namespace kvstore
