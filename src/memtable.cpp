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
    return entries_.erase(key) != 0;
}

}  // namespace kvstore
