#include "kvstore/block_cache.h"

#include <algorithm>
#include <future>
#include <iostream>
#include <random>
#include <stdexcept>
#include <utility>
#include <vector>

void Check(bool condition, const std::string& message) {
    if (!condition) { throw std::runtime_error(message); }
}
auto Bytes(const std::string& value) { return std::make_shared<const std::string>(value); }

void TestHitMissAndCharge() {
    kvstore::BlockCache cache(8);
    const auto table = cache.NewTableId();
    Check(!cache.Lookup({table, 0}), "Uncached key misses");
    cache.Insert({table, 0}, Bytes("abcd"));
    const auto found = cache.Lookup({table, 0});
    const auto stats = cache.stats();
    Check(found && *found == "abcd" && stats.hits == 1 && stats.misses == 1 &&
          stats.resident_bytes == 4 && stats.entries == 1 && stats.capacity_bytes == 8, "Charge payload bytes and count lookups");
}

void TestLeastRecentlyUsedEviction() {
    kvstore::BlockCache cache(8);
    const auto id = cache.NewTableId();
    cache.Insert({id, 0}, Bytes("aaaa")); cache.Insert({id, 4}, Bytes("bbbb"));
    Check(cache.Lookup({id, 0}) != nullptr, "Touch first block");
    cache.Insert({id, 8}, Bytes("cccc"));
    Check(!cache.Lookup({id, 4}) && cache.Lookup({id, 0}) && cache.Lookup({id, 8}), "Evict untouched second block");
    Check(cache.stats().evictions == 1 && cache.stats().resident_bytes == 8, "Capacity is enforced after insertion");
}

void TestMultipleByteBudgetEvictions() {
    kvstore::BlockCache cache(10);
    const auto id = cache.NewTableId();
    for (std::uint64_t i = 0; i < 3; ++i) { cache.Insert({id, i}, Bytes("abc")); }
    cache.Insert({id, 3}, Bytes("12345678"));
    const auto stats = cache.stats();
    Check(stats.entries == 1 && stats.resident_bytes == 8 && stats.evictions == 3,
          "One large admission can evict several smaller blocks");
}

void TestReplacementAndPinnedValues() {
    kvstore::BlockCache cache(10);
    const auto id = cache.NewTableId();
    cache.Insert({id, 0}, Bytes("aa"));
    const auto pinned = cache.Lookup({id, 0});
    cache.Insert({id, 1}, Bytes("bbb")); cache.Insert({id, 2}, Bytes("ccc"));
    cache.Insert({id, 0}, Bytes("1234567"));
    Check(*pinned == "aa" && *cache.Lookup({id, 0}) == "1234567" && !cache.Lookup({id, 1}) && cache.Lookup({id, 2}),
          "Replace atomically and keep already returned immutable snapshots alive");
    Check(cache.stats().resident_bytes == 10, "Replacement updates byte charge");
    cache.Insert({id, 0}, Bytes("x"));
    Check(cache.stats().resident_bytes == 4, "Smaller replacement releases charge");
}

void TestDisabledOversizedAndInvalidBlocks() {
    for (const std::size_t capacity : {0, 3}) {
        kvstore::BlockCache cache(capacity);
        const auto id = cache.NewTableId();
        cache.Insert({id, 0}, Bytes("large"));
        Check(!cache.Lookup({id, 0}) && cache.stats().resident_bytes == 0 && cache.stats().admission_bypasses == 1,
              "Disabled or oversized admissions retain no bytes");
        for (const auto& bad : {std::shared_ptr<const std::string>{}, Bytes("")}) {
            bool failed = false;
            try { cache.Insert({id, 0}, bad); } catch (const std::invalid_argument&) { failed = true; }
            Check(failed && cache.stats().entries == 0, "Reject empty blocks rather than accumulating zero-charge nodes");
        }
    }
}

void TestTableIsolationAndRetirement() {
    kvstore::BlockCache cache(20);
    const auto first = cache.NewTableId(), second = cache.NewTableId();
    Check(first != second, "Each reader gets an independent namespace");
    cache.Insert({first, 8}, Bytes("first")); cache.Insert({second, 8}, Bytes("second"));
    const auto pinned = cache.Lookup({first, 8});
    cache.EraseTable(first);
    Check(!cache.Lookup({first, 8}) && *cache.Lookup({second, 8}) == "second" && *pinned == "first",
          "Retire one reader without disturbing another or invalidating a caller's snapshot");
    Check(cache.stats().entries == 1 && cache.stats().resident_bytes == 6, "Retirement releases resident charge");
    cache.EraseTable(second); cache.EraseTable(second);
    Check(cache.stats().entries == 0 && cache.stats().resident_bytes == 0, "Repeated retirement is harmless");
}

void TestConcurrentClients() {
    kvstore::BlockCache cache(64);
    std::vector<std::future<void>> clients;
    for (int client = 0; client < 4; ++client) {
        const auto id = cache.NewTableId();
        clients.push_back(std::async(std::launch::async, [&, id, client] {
            for (int i = 0; i < 100; ++i) {
                const auto block = static_cast<std::uint64_t>(i % 8);
                const auto value = std::to_string(client) + ":" + std::to_string(block);
                cache.Insert({id, block}, Bytes(value));
                const auto found = cache.Lookup({id, block});
                Check(!found || *found == value, "Concurrent eviction may miss but cannot mix up values");
                Check(cache.stats().resident_bytes <= 64, "Concurrent accounting stays within budget");
            }
            cache.EraseTable(id);
        }));
    }
    for (auto& client : clients) { client.get(); }
    Check(cache.stats().entries == 0 && cache.stats().resident_bytes == 0, "Concurrent clients clean up their namespaces");
}

void TestMixedReferenceModel() {
    using Item = std::pair<kvstore::BlockCacheKey, std::string>;
    for (const std::size_t capacity : {0, 1, 7, 32}) {
        kvstore::BlockCache cache(capacity);
        std::vector<Item> model;  // Oldest at the front; deliberately use a linear reference model.
        std::mt19937 random(42);
        for (int step = 0; step < 300; ++step) {
            const kvstore::BlockCacheKey key{1 + random() % 3, random() % 5};
            const auto found = std::find_if(model.begin(), model.end(), [&](const auto& item) { return item.first == key; });
            const auto operation = random() % 3;
            if (operation == 0) {
                const auto actual = cache.Lookup(key);
                Check(static_cast<bool>(actual) == (found != model.end()), "Reference model agrees on hits");
                if (found != model.end()) {
                    Check(*actual == found->second, "Reference model agrees on bytes");
                    auto item = *found; model.erase(found); model.push_back(std::move(item));
                }
            } else if (operation == 1) {
                const std::string value(1 + random() % 8, static_cast<char>('a' + step % 26));
                cache.Insert(key, Bytes(value));
                if (value.size() <= capacity) {
                    if (found != model.end()) { model.erase(found); }
                    std::size_t total = value.size();
                    for (const auto& item : model) { total += item.second.size(); }
                    while (total > capacity) { total -= model.front().second.size(); model.erase(model.begin()); }
                    model.push_back({key, value});
                }
            } else {
                cache.EraseTable(key.table_id);
                model.erase(std::remove_if(model.begin(), model.end(), [&](const auto& item) {
                    return item.first.table_id == key.table_id;
                }), model.end());
            }
            std::size_t total = 0;
            for (const auto& item : model) { total += item.second.size(); }
            Check(cache.stats().entries == model.size() && cache.stats().resident_bytes == total,
                  "Mixed operations preserve reference membership and byte accounting");
        }
    }
}

int main() {
    try {
        TestHitMissAndCharge(); TestLeastRecentlyUsedEviction(); TestMultipleByteBudgetEvictions();
        TestReplacementAndPinnedValues(); TestDisabledOversizedAndInvalidBlocks(); TestTableIsolationAndRetirement();
        TestConcurrentClients(); TestMixedReferenceModel();
        std::cout << "All 8 block cache tests passed.\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "Block cache test failed: " << error.what() << '\n';
        return 1;
    }
}
