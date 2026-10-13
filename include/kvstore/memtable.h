#pragma once

#include <map>
#include <optional>
#include <string>
#include <vector>

namespace kvstore {

struct MemTableEntry {
    std::string key;
    std::optional<std::string> value;  // nullopt means a tombstone, not an absent entry.
};

// Single-threaded, in-memory storage. Persistence is coordinated by Store.
class MemTable {
public:
    void Put(const std::string& key, const std::string& value);
    // User-facing lookup: both tombstones and absent entries have no visible value.
    std::optional<std::string> Get(const std::string& key) const;

    // Always retain a tombstone, including for a missing key.
    // Returns true only if this MemTable previously held a visible value.
    bool Delete(const std::string& key);

    // Internal lookup: outer nullopt means absent; an entry with no value is deleted.
    std::optional<MemTableEntry> Lookup(const std::string& key) const;

    // Return an owned, sorted copy, including tombstones. Does not clear the table.
    // This is an export for flushing, not a transaction or an MVCC snapshot.
    std::vector<MemTableEntry> Snapshot() const;

    // A Store must publish the replacement data before clearing its current table.
    void Clear();

private:
    std::map<std::string, std::optional<std::string>> entries_;
};

}  // namespace kvstore
