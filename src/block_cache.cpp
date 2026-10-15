#include "kvstore/block_cache.h"

#include <functional>
#include <limits>
#include <stdexcept>
#include <utility>

namespace kvstore {

BlockCache::BlockCache(std::size_t capacity_bytes) {
    stats_.capacity_bytes = capacity_bytes;
}

std::size_t BlockCache::KeyHash::operator()(BlockCacheKey key) const {
    const auto first = std::hash<std::uint64_t>{}(key.table_id);
    const auto second = std::hash<std::uint64_t>{}(key.offset);
    return first ^ (second + 0x9e3779b9U + (first << 6) + (first >> 2));
}

std::uint64_t BlockCache::NewTableId() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (next_table_id_ == std::numeric_limits<std::uint64_t>::max()) {
        throw std::length_error("Block cache table IDs exhausted");
    }
    return next_table_id_++;
}

std::shared_ptr<const std::string> BlockCache::Lookup(BlockCacheKey key) {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto found = lookup_.find(key);
    if (found == lookup_.end()) { ++stats_.misses; return {}; }
    ++stats_.hits;
    lru_.splice(lru_.begin(), lru_, found->second);
    return found->second->block;
}

void BlockCache::EvictOldest() {
    const auto& oldest = lru_.back();
    stats_.resident_bytes -= oldest.block->size();
    lookup_.erase(oldest.key);
    lru_.pop_back();
    ++stats_.evictions;
}

void BlockCache::Insert(BlockCacheKey key, std::shared_ptr<const std::string> block) {
    if (!block || block->empty()) { throw std::invalid_argument("Cache blocks must be nonempty"); }
    std::lock_guard<std::mutex> lock(mutex_);
    const auto charge = block->size();
    if (charge > stats_.capacity_bytes) { ++stats_.admission_bypasses; return; }
    const auto found = lookup_.find(key);
    if (found != lookup_.end()) {
        auto entry = found->second;
        lru_.splice(lru_.begin(), lru_, entry);
        stats_.resident_bytes -= entry->block->size();
        // This MRU entry is temporarily uncharged; only older entries can be evicted.
        while (stats_.resident_bytes > stats_.capacity_bytes - charge) { EvictOldest(); }
        entry->block = std::move(block);
    } else {
        // Allocate both nodes before eviction; undo the list insertion if map allocation fails.
        lru_.push_front({key, std::move(block)});
        try { lookup_.emplace(key, lru_.begin()); }
        catch (...) { lru_.pop_front(); throw; }
        while (stats_.resident_bytes > stats_.capacity_bytes - charge) { EvictOldest(); }
    }
    stats_.resident_bytes += charge;
}

void BlockCache::EraseTable(std::uint64_t table_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto it = lru_.begin(); it != lru_.end();) {
        if (it->key.table_id != table_id) { ++it; continue; }
        stats_.resident_bytes -= it->block->size();
        lookup_.erase(it->key);
        it = lru_.erase(it);
    }
}

BlockCacheStats BlockCache::stats() const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto result = stats_;
    result.entries = lru_.size();
    return result;
}

}  // namespace kvstore
