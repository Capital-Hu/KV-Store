#include "kvstore/compaction.h"
#include "kvstore/store.h"
#include "kvstore/wal_record.h"

#include <filesystem>
#include <fstream>
#include <future>
#include <iomanip>
#include <iostream>
#include <iterator>
#include <sstream>
#include <stdexcept>

#include <unistd.h>

using Type = kvstore::SSTableEntryType;
void Check(bool condition, const std::string& message) {
    if (!condition) { throw std::runtime_error(message); }
}
void Reject(const std::function<void()>& action) {
    bool failed = false;
    try { action(); } catch (const std::exception&) { failed = true; }
    Check(failed, "Invalid or incomplete block must not be admitted");
}
class TestDirectory {
public:
    TestDirectory() {
        char pattern[] = "/tmp/kvstore-index-XXXXXX";
        const auto path = ::mkdtemp(pattern);
        Check(path != nullptr, "Create test directory");
        path_ = path;
    }
    ~TestDirectory() {
        std::error_code ignored;
        std::filesystem::remove_all(path_, ignored);
    }
    std::string File(const std::string& name = "table.sst") const { return (path_ / name).string(); }
private:
    std::filesystem::path path_;
};
std::string Key(int i) {
    std::ostringstream out; out << 'k' << std::setw(4) << std::setfill('0') << i; return out.str();
}
std::vector<kvstore::SSTableEntry> Entries(int count, std::size_t value_bytes = 64) {
    std::vector<kvstore::SSTableEntry> entries;
    for (int i = 0; i < count; ++i) { entries.push_back({Type::Value, Key(i), std::string(value_bytes, 'a' + i % 26)}); }
    return entries;
}
std::string Read(const std::string& path) {
    std::ifstream input(path, std::ios::binary);
    Check(input.is_open(), "Read fixture");
    return {std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
}
void Write(const std::string& path, const std::string& bytes) {
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    output.write(bytes.data(), static_cast<std::streamsize>(bytes.size())); output.close();
    Check(!output.fail(), "Write fixture");
}
kvstore::SSTableReadOptions Options(std::shared_ptr<kvstore::BlockCache> cache = {}) {
    kvstore::SSTableReadOptions options;
    options.use_bloom_filter = false;  // Isolate index/cache behavior from negative filtering.
    options.block_cache = std::move(cache);
    return options;
}
void Value(kvstore::SSTableReader& reader, int i, std::size_t bytes = 64) {
    const auto entry = reader.Get(Key(i));
    Check(entry && entry->type == Type::Value && entry->value == std::string(bytes, 'a' + i % 26), "Exact key/value lookup");
}

void TestIndexReducesLookupWork() {
    TestDirectory directory;
    kvstore::WriteSSTable(directory.File(), Entries(256));
    kvstore::SSTableReader indexed(directory.File(), Options());
    auto baseline_options = Options(); baseline_options.use_block_index = false;
    kvstore::SSTableReader baseline(directory.File(), baseline_options);
    Value(indexed, 255); Value(baseline, 255);
    Check(indexed.block_count() > 1 && indexed.read_stats().block_reads == 1 &&
          indexed.read_stats().entries_examined < baseline.read_stats().entries_examined / 2,
          "Late key lookup reads just one candidate block");
    Check(baseline.read_stats().entries_examined == 256, "Sequential reference visits all earlier entries");
    std::cout << "Indexed sample: " << indexed.read_stats().entries_examined << " vs "
              << baseline.read_stats().entries_examined << " lookup entries; "
              << indexed.read_stats().block_reads << " candidate block read.\n";
    for (int i = 0; i < 256; ++i) { Value(indexed, i); Value(baseline, i); }
    for (int i = 0; i < 256; ++i) {
        Check(!indexed.Get(Key(i) + "x") && !baseline.Get(Key(i) + "x"), "Gap queries agree with sequential reference");
    }
}

void TestBlockBoundariesAndCursor() {
    TestDirectory directory;
    kvstore::WriteSSTable(directory.File(), Entries(8, 2000));
    auto cache = std::make_shared<kvstore::BlockCache>(20000);
    kvstore::SSTableReader reader(directory.File(), Options(cache));
    Check(reader.block_count() == 4, "Two complete records fit in each 4 KiB logical block");
    Value(reader, 1, 2000); Check(reader.Next()->key == Key(2), "Next crosses the block boundary after Get");
    Value(reader, 0, 2000); Check(reader.Next()->key == Key(1), "Cache-hit Get preserves the original cursor contract");
    Check(!reader.Get(Key(1) + "x") && reader.Next()->key == Key(3), "Gap lookup consumes first greater entry in next block");
    Check(!reader.Get("") && reader.Next()->key == Key(1), "Before-first lookup has the same cursor position as a scan");
    const auto reads = reader.read_stats().block_reads;
    Check(!reader.Get("zzzz") && !reader.Next() && reader.read_stats().block_reads == reads,
          "After-last key is rejected by index without block I/O");
    reader.Rewind();
    for (int i = 0; i < 8; ++i) { Check(reader.Next()->key == Key(i), "Rewind/Next still returns the full ordered table"); }
    Check(!reader.Next(), "Iteration ends after all entries");
}

void TestCacheHitsAndBloomBypass() {
    TestDirectory directory;
    const auto entries = Entries(8, 2000);
    kvstore::WriteSSTable(directory.File(), entries);
    auto cache = std::make_shared<kvstore::BlockCache>(20000);
    auto options = Options(cache); options.use_bloom_filter = true;
    kvstore::SSTableReader reader(directory.File(), options);
    Value(reader, 0, 2000); Value(reader, 1, 2000);
    Check(reader.read_stats().block_reads == 1 && reader.read_stats().cache_hits == 1,
          "Different keys in the same block reuse its bytes");
    kvstore::BloomFilter model(entries.size());
    for (const auto& entry : entries) { model.Add(entry.key); }
    std::string absent;
    for (int i = 0; i < 1000; ++i) {
        const auto candidate = "absent" + std::to_string(i);
        if (!model.MayContain(candidate)) { absent = candidate; break; }
    }
    Check(!absent.empty(), "Find a definite negative");
    const auto before = reader.read_stats(); const auto cache_before = cache->stats();
    Check(!reader.Get(absent) && reader.read_stats().bloom_negatives == before.bloom_negatives + 1 &&
          reader.read_stats().index_lookups == before.index_lookups &&
          cache->stats().hits == cache_before.hits && cache->stats().misses == cache_before.misses,
          "Bloom negative bypasses index, cache and data reads");
}

void TestLRUEvictionAcrossBlocks() {
    TestDirectory directory;
    kvstore::WriteSSTable(directory.File(), Entries(8, 2000));
    constexpr std::size_t block_bytes = 2 * (17 + 5 + 2000);
    auto cache = std::make_shared<kvstore::BlockCache>(2 * block_bytes);
    kvstore::SSTableReader reader(directory.File(), Options(cache));
    for (const int i : {0, 2, 0, 4, 0, 2}) { Value(reader, i, 2000); }
    Check(reader.read_stats().block_reads == 4 && reader.read_stats().cache_hits == 2 &&
          cache->stats().evictions == 2 && cache->stats().resident_bytes == 2 * block_bytes,
          "Touches determine eviction and evicted blocks are read again");
}

void TestDisabledAndOversizedBlocks() {
    TestDirectory directory;
    kvstore::WriteSSTable(directory.File(), Entries(4, 2000));
    for (const std::size_t capacity : {0, 10}) {
        auto cache = std::make_shared<kvstore::BlockCache>(capacity);
        kvstore::SSTableReader reader(directory.File(), Options(cache));
        Value(reader, 0, 2000); Value(reader, 0, 2000);
        Check(reader.read_stats().block_reads == 2 && reader.read_stats().cache_hits == 0 &&
              cache->stats().entries == 0 && cache->stats().admission_bypasses == 2,
              "Disabled and too-small caches still return exact data without retaining blocks");
    }
    const std::string maximum(kvstore::kMaxWalPayloadBytes - 1, 'v');
    kvstore::WriteSSTable(directory.File("large.sst"), {{Type::Value, "a", maximum}, {Type::Value, "b", "small"}});
    auto cache = std::make_shared<kvstore::BlockCache>(4096);
    kvstore::SSTableReader reader(directory.File("large.sst"), Options(cache));
    Check(reader.block_count() == 2, "An oversized record gets its own unsplit block");
    for (int i = 0; i < 2; ++i) {
        const auto entry = reader.Get("a"); Check(entry && entry->value == maximum, "Maximum record reads correctly");
    }
    const auto small = reader.Get("b"), again = reader.Get("b");
    Check(small && again && again->value == "small" && reader.read_stats().cache_hits == 1 &&
          cache->stats().entries == 1 && cache->stats().resident_bytes <= 4096, "Large record bypass does not prevent small-block caching");
}

void TestBinaryTombstonesAndEmptyTables() {
    TestDirectory directory;
    const std::string key("a\0b", 3), value("v\0\xff", 3);
    kvstore::WriteSSTable(directory.File(), {{Type::Value, "", ""}, {Type::Value, key, value}, {Type::Tombstone, "deleted", ""}});
    auto cache = std::make_shared<kvstore::BlockCache>(4096);
    kvstore::SSTableReader reader(directory.File(), Options(cache));
    const auto empty = reader.Get(""), binary = reader.Get(key), deleted = reader.Get("deleted");
    Check(empty && empty->value.empty() && binary && binary->value == value && deleted && deleted->type == Type::Tombstone,
          "Indexed cached lookup preserves all three record states and binary bytes");
    kvstore::WriteSSTable(directory.File("empty.sst"), {});
    kvstore::SSTableReader empty_reader(directory.File("empty.sst"), Options(cache));
    const auto before = cache->stats();
    Check(empty_reader.block_count() == 0 && !empty_reader.Get("") && !empty_reader.Get("missing") &&
          cache->stats().hits == before.hits && cache->stats().misses == before.misses,
          "Empty index answers without data blocks or cache lookups");
}

void TestNamespacesRetirementAndPathReuse() {
    TestDirectory directory;
    const auto a = directory.File("a.sst"), b = directory.File("b.sst");
    kvstore::WriteSSTable(a, {{Type::Value, "key", "first"}});
    kvstore::WriteSSTable(b, {{Type::Value, "key", "second"}});
    auto cache = std::make_shared<kvstore::BlockCache>(4096);
    {
        auto first = std::make_unique<kvstore::SSTableReader>(a, Options(cache));
        kvstore::SSTableReader second(b, Options(cache));
        Check(first->Get("key")->value == "first" && second.Get("key")->value == "second", "Same offsets in different files stay isolated");
        Check(cache->stats().entries == 2, "Both readers have a resident block");
        first.reset();
        Check(cache->stats().entries == 1, "Closing one reader retires only its own cache namespace");
        std::filesystem::remove(a);
        kvstore::WriteSSTable(a, {{Type::Value, "key", "replacement"}});
        kvstore::SSTableReader replaced(a, Options(cache));
        Check(replaced.Get("key")->value == "replacement" && second.Get("key")->value == "second",
              "Reused path cannot retrieve a closed reader's stale block");
    }
    Check(cache->stats().entries == 0 && cache->stats().resident_bytes == 0, "Closing all readers releases the cache payloads");
}

void TestCorruptMissIsNotCached() {
    TestDirectory directory;
    kvstore::WriteSSTable(directory.File(), Entries(8, 2000));
    const auto original = Read(directory.File());
    auto cache = std::make_shared<kvstore::BlockCache>(10000);
    kvstore::SSTableReader reader(directory.File(), Options(cache));
    auto corrupted = original; corrupted.back() ^= 1;
    Write(directory.File(), corrupted);  // Fault injection; live files must normally be immutable.
    Reject([&] { reader.Get(Key(7)); });
    Check(cache->stats().entries == 0, "CRC failure cannot populate the cache");
    Write(directory.File(), original);
    Value(reader, 7, 2000);
    Check(cache->stats().entries == 1, "Failed cache admission leaves no poisoned entry");
    for (const auto& bytes : {corrupted, original.substr(0, original.size() - 1)}) {
        Write(directory.File("broken.sst"), bytes);
        Reject([&] { kvstore::SSTableReader broken(directory.File("broken.sst"), Options(cache)); });
    }
}

void TestIndexBudgetFallback() {
    TestDirectory directory;
    kvstore::WriteSSTable(directory.File(), Entries(10, 4000));
    for (const std::size_t budget : {0, 128}) {
        auto cache = std::make_shared<kvstore::BlockCache>(10000);
        auto options = Options(cache); options.index_budget_bytes = budget;
        kvstore::SSTableReader reader(directory.File(), options);
        Value(reader, 9, 4000);
        Check(!reader.block_index_enabled() && reader.block_count() == 0 && reader.read_stats().entries_examined == 10 &&
              cache->stats().hits == 0 && cache->stats().misses == 0, "Discard partial index and fall back to exact sequential lookup");
    }
}

void TestStorePublicationInvalidatesOldBlocks() {
    TestDirectory directory;
    const auto base = directory.File("store.wal");
    {
        kvstore::Store store(base, kvstore::WalRecoveryMode::Strict, 0, 0, 8192);
        store.Put("key", "old"); store.Flush(); Check(store.Get("key") == "old", "Warm original reader");
        store.Delete("key"); store.Flush(); Check(!store.Get("key"), "New tombstone overrides a cached old value");
        store.Put("key", "new"); store.Flush(); Check(store.Get("key") == "new", "New file does not alias an old cached offset");
        Check(store.block_cache_stats().entries == 3, "One Store cache is shared by three table readers");
        store.Compact();
        Check(store.block_cache_stats().entries == 0, "Compaction retires old namespaces without warming its output");
        Check(store.Get("key") == "new" && store.Get("key") == "new" && store.block_cache_stats().entries == 1,
              "Replacement reader loads and reuses its own block");
        store.Delete("key"); store.Flush(); store.Compact();
        Check(!store.Get("key") && store.block_cache_stats().entries == 0, "All-deleted compaction leaves no stale cached value");
        store.Put("key", "reborn"); store.RequestFlush(); store.WaitForBackgroundWork();
        Check(store.Get("key") == "reborn", "Background publication installs a new cache namespace");
    }
    kvstore::Store reopened(base);
    Check(reopened.block_cache_stats().entries == 0 && reopened.Get("key") == "reborn", "Restart begins cold and rebuilds metadata correctly");
}

void TestConcurrentStoreWithCacheOnAndOff() {
    for (const std::size_t capacity : {0, 4096}) {
        TestDirectory directory;
        const auto base = directory.File("store.wal");
        {
            kvstore::Store store(base, kvstore::WalRecoveryMode::Strict, 256, 4, capacity);
            std::vector<std::future<void>> clients;
            for (int client = 0; client < 3; ++client) {
                clients.push_back(std::async(std::launch::async, [&, client] {
                    for (int i = 0; i < 12; ++i) {
                        const auto key = std::to_string(client) + ":" + std::to_string(i);
                        store.Put(key, "old"); store.Delete(key); store.Put(key, "final");
                        Check(store.Get(key) == "final", "Concurrent indexed/cached read agrees with own latest write");
                        if (i % 4 == 0) { store.RequestFlush(); store.RequestCompaction(); }
                        Check(store.block_cache_stats().resident_bytes <= capacity, "Shared budget holds during publication and eviction");
                    }
                }));
            }
            for (auto& client : clients) { client.get(); }
            store.Flush(); store.Compact(); store.WaitForBackgroundWork();
            Check(store.Get("0:0") == "final" && store.Get("0:0") == "final", "Final reader returns exact data");
            Check(capacity != 0 || (store.block_cache_stats().entries == 0 && store.block_cache_stats().hits == 0),
                  "Zero capacity never retains payloads or reports hits");
        }
        kvstore::Store reopened(base);
        for (int client = 0; client < 3; ++client) {
            for (int i = 0; i < 12; ++i) { Check(reopened.Get(std::to_string(client) + ":" + std::to_string(i)) == "final", "All 36 keys recover"); }
        }
    }
}

void TestSequentialScansAndMergeDoNotPolluteCache() {
    TestDirectory directory;
    kvstore::WriteSSTable(directory.File(), Entries(8, 2000));
    auto cache = std::make_shared<kvstore::BlockCache>(5000);
    kvstore::SSTableReader reader(directory.File(), Options(cache));
    Value(reader, 0, 2000);
    const auto before = cache->stats();
    reader.Rewind();
    for (int i = 0; i < 8; ++i) { Check(reader.Next()->key == Key(i), "Sequential scan returns every key"); }
    kvstore::MergeSSTables({directory.File()}, directory.File("merged.sst"));
    kvstore::SSTableReader merged(directory.File("merged.sst"), Options(cache));
    Check(cache->stats().hits == before.hits && cache->stats().misses == before.misses &&
          cache->stats().entries == before.entries, "Validation, Next and merge leave the hot block undisturbed");
    Value(reader, 0, 2000);
    Check(cache->stats().hits == before.hits + 1, "The original hot block survived full scans");
}

int main() {
    try {
        TestIndexReducesLookupWork(); TestBlockBoundariesAndCursor(); TestCacheHitsAndBloomBypass();
        TestLRUEvictionAcrossBlocks(); TestDisabledAndOversizedBlocks(); TestBinaryTombstonesAndEmptyTables();
        TestNamespacesRetirementAndPathReuse(); TestCorruptMissIsNotCached(); TestIndexBudgetFallback();
        TestStorePublicationInvalidatesOldBlocks(); TestConcurrentStoreWithCacheOnAndOff();
        TestSequentialScansAndMergeDoNotPolluteCache();
        std::cout << "All 12 SSTable index/cache tests passed.\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "SSTable index/cache test failed: " << error.what() << '\n';
        return 1;
    }
}
