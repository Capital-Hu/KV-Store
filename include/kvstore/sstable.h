#pragma once

#include "kvstore/bloom_filter.h"

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

// Logical work for Get only; excludes opening validation and Next-based merge scans.
// These are not physical disk-read counts: std::ifstream and the OS may buffer I/O.
struct SSTableReadStats {
    std::uint64_t get_calls = 0;
    std::uint64_t bloom_negatives = 0;
    std::uint64_t table_scans = 0;
    std::uint64_t entries_examined = 0;
};

// Write a new file; never overwrite an existing path.
// Entries must have strictly increasing, unique keys (std::string order).
// Each key + value is limited to 1 MiB, matching the WAL codec.
// Format: KVS1 | entry_count:u32 little-endian | sorted WAL-encoded records.
// Reuse the record codec, not the WAL's append/replay semantics.
// A failed write may leave an incomplete file. Store coordinates synchronization
// and durable file-set publication; this function only constructs the file.
void WriteSSTable(const std::string& path, const std::vector<SSTableEntry>& entries);

// Streaming writer: append sorted unique entries, then Finish before publication.
// Finish patches the count and closes the file; it does not synchronize or publish.
// Destruction without Finish may leave an incomplete, unpublished file.
class SSTableWriter {
public:
    explicit SSTableWriter(const std::string& path);
    ~SSTableWriter();
    SSTableWriter(const SSTableWriter&) = delete;
    SSTableWriter& operator=(const SSTableWriter&) = delete;
    SSTableWriter(SSTableWriter&&) = delete;
    SSTableWriter& operator=(SSTableWriter&&) = delete;

    void Append(const SSTableEntry& entry);
    void Finish();
    std::uint32_t size() const { return entry_count_; }

private:
    void EnsureWritable() const;
    int fd_ = -1;
    std::uint32_t entry_count_ = 0;
    std::optional<std::string> previous_key_;
    bool usable_ = true;
};

// Single-threaded reader. The file must not be modified while it is open.
// Opening validates the entire file and builds an in-memory filter, including tombstones.
// A possible match still requires scanning; filters are rebuilt on every open.
class SSTableReader {
public:
    explicit SSTableReader(const std::string& path, bool use_bloom_filter = true);

    SSTableReader(const SSTableReader&) = delete;
    SSTableReader& operator=(const SSTableReader&) = delete;
    SSTableReader(SSTableReader&&) = delete;
    SSTableReader& operator=(SSTableReader&&) = delete;

    // nullopt: key absent from this file. A returned Tombstone means deleted.
    // A returned Value may contain an empty string; these are three distinct cases.
    std::optional<SSTableEntry> Get(const std::string& key);
    std::uint32_t size() const { return entry_count_; }
    const SSTableReadStats& read_stats() const { return read_stats_; }
    bool bloom_filter_enabled() const { return bloom_ && bloom_->enabled(); }

    // A scanning Get rewinds/advances this cursor; a Bloom negative leaves it unchanged.
    // Use separate readers for independent scans. Returned entries own their strings.
    void Rewind();
    std::optional<SSTableEntry> Next();

private:
    std::ifstream input_;  // Owns and closes the file through RAII.
    std::uint32_t entry_count_ = 0;
    std::uint32_t entries_read_ = 0;
    std::optional<BloomFilter> bloom_;
    SSTableReadStats read_stats_;
};

}  // namespace kvstore
