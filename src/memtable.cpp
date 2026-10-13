#include "kvstore/memtable.h"

namespace kvstore {

void MemTable::Put(const std::string& key, const std::string& value) {
    entries_[key] = value;
}

std::optional<std::string> MemTable::Get(const std::string& key) const {
    const auto it = entries_.find(key);
    if (it == entries_.end()) {
        return std::nullopt;
    }
    return it->second;
}

bool MemTable::Delete(const std::string& key) {
    const auto it = entries_.find(key);
    const bool existed = it != entries_.end() && it->second.has_value();
    entries_[key] = std::nullopt;
    return existed;
}

std::optional<MemTableEntry> MemTable::Lookup(const std::string& key) const {
    const auto it = entries_.find(key);
    if (it == entries_.end()) {
        return std::nullopt;
    }
    return MemTableEntry{it->first, it->second};
}

std::vector<MemTableEntry> MemTable::Snapshot() const {
    std::vector<MemTableEntry> result;
    result.reserve(entries_.size());
    // std::map already keeps keys sorted; export only their latest states.
    for (const auto& entry : entries_) {
        result.push_back({entry.first, entry.second});
    }
    return result;
}

void MemTable::Clear() {
    entries_.clear();
}

}  // namespace kvstore
