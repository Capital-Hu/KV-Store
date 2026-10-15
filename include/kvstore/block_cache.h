#pragma once

#include <cstddef>
#include <cstdint>
#include <list>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

namespace kvstore {

constexpr std::size_t kDefaultBlockCacheBytes = 8 * 1024 * 1024;

struct BlockCacheKey {
    std::uint64_t table_id;
    std::uint64_t offset;
    bool operator==(const BlockCacheKey& other) const {
        return table_id == other.table_id && offset == other.offset;
    }
};

struct BlockCacheStats {
    std::size_t capacity_bytes = 0;
    std::size_t resident_bytes = 0;
    std::size_t entries = 0;
    std::uint64_t hits = 0;
    std::uint64_t misses = 0;
    std::uint64_t evictions = 0;
    std::uint64_t admission_bypasses = 0;
};

// Thread-safe byte-budgeted LRU. Values are immutable encoded data blocks.
// The budget charges resident payload bytes, not metadata or snapshots held by callers.
class BlockCache {
public:
    explicit BlockCache(std::size_t capacity_bytes = kDefaultBlockCacheBytes);
    BlockCache(const BlockCache&) = delete;
    BlockCache& operator=(const BlockCache&) = delete;

    // Unique within this cache lifetime, independent of paths and Store file IDs.
    std::uint64_t NewTableId();
    std::shared_ptr<const std::string> Lookup(BlockCacheKey key);
    void Insert(BlockCacheKey key, std::shared_ptr<const std::string> block);
    void EraseTable(std::uint64_t table_id);
    BlockCacheStats stats() const;

private:
    struct Entry {
        BlockCacheKey key;
        std::shared_ptr<const std::string> block;
    };
    struct KeyHash {
        std::size_t operator()(BlockCacheKey key) const;
    };
    using Entries = std::list<Entry>;
    void EvictOldest();

    mutable std::mutex mutex_;
    Entries lru_;  // Most recently used at the front; splice preserves iterators.
    std::unordered_map<BlockCacheKey, Entries::iterator, KeyHash> lookup_;
    BlockCacheStats stats_;
    std::uint64_t next_table_id_ = 1;
};

}  // namespace kvstore
