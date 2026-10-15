#include "kvstore/compaction.h"
#include "kvstore/store.h"

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <future>
#include <iomanip>
#include <iostream>
#include <iterator>
#include <sstream>
#include <stdexcept>
#include <utility>

#include <unistd.h>

namespace kvstore {
struct StoreTestAccess {
    static SSTableReadStats Stats(const Store& store) {
        std::lock_guard<std::mutex> lock(store.mutex_);
        SSTableReadStats total;
        for (const auto& reader : store.sstables_) {
            const auto& stats = reader->read_stats();
            total.get_calls += stats.get_calls;
            total.bloom_negatives += stats.bloom_negatives;
            total.table_scans += stats.table_scans;
            total.entries_examined += stats.entries_examined;
        }
        return total;
    }
    static bool AllFiltered(const Store& store) {
        std::lock_guard<std::mutex> lock(store.mutex_);
        return !store.sstables_.empty() && std::all_of(store.sstables_.begin(), store.sstables_.end(),
            [](const auto& reader) { return reader->bloom_filter_enabled(); });
    }
};
}  // namespace kvstore

using Type = kvstore::SSTableEntryType;

void Check(bool condition, const std::string& message) {
    if (!condition) { throw std::runtime_error(message); }
}
void Reject(const std::function<void()>& action) {
    bool rejected = false;
    try { action(); } catch (const std::bad_alloc&) { throw; }
    catch (const std::exception&) { rejected = true; }
    Check(rejected, "Corrupt input must be rejected before serving any filtered lookup");
}
class TestDirectory {
public:
    TestDirectory() {
        char pattern[] = "/tmp/kvstore-bloom-XXXXXX";
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
std::string Read(const std::string& path) {
    std::ifstream input(path, std::ios::binary);
    Check(input.is_open(), "Read file");
    return {std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
}
void Write(const std::string& path, const std::string& bytes) {
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    output.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    output.close();
    Check(!output.fail(), "Write test fixture");
}
std::string Key(int number) {
    std::ostringstream out;
    out << 'k' << std::setw(4) << std::setfill('0') << number;
    return out.str();
}
std::vector<kvstore::SSTableEntry> Entries(int count) {
    std::vector<kvstore::SSTableEntry> entries;
    for (int i = 0; i < count; ++i) { entries.push_back({Type::Value, Key(i), std::to_string(i)}); }
    return entries;
}

void TestEnabledDisabledEquivalenceAndScanCounts() {
    TestDirectory directory;
    kvstore::WriteSSTable(directory.File(), Entries(128));
    const auto original = Read(directory.File());
    kvstore::SSTableReader filtered(directory.File()), baseline(directory.File(), false);
    Check(filtered.bloom_filter_enabled() && !baseline.bloom_filter_enabled(), "Reader can disable filtering for comparison");
    Check(filtered.read_stats().entries_examined == 0, "Opening validation is not counted as query work");
    for (int i = 0; i < 128; ++i) {
        const auto a = filtered.Get(Key(i)), b = baseline.Get(Key(i));
        Check(a && b && a->key == b->key && a->value == b->value, "All present keys match the exact baseline");
    }
    const auto before = filtered.read_stats(), base_before = baseline.read_stats();
    for (int i = 0; i < 200; ++i) {
        const auto key = "z-missing:" + std::to_string(i);
        Check(!filtered.Get(key) && !baseline.Get(key), "Both paths report every absent key as absent");
    }
    const auto skipped = filtered.read_stats().bloom_negatives - before.bloom_negatives;
    const auto scans = filtered.read_stats().table_scans - before.table_scans;
    const auto examined = filtered.read_stats().entries_examined - before.entries_examined;
    const auto base_examined = baseline.read_stats().entries_examined - base_before.entries_examined;
    Check(skipped > 180 && scans + skipped == 200, "Most missing lookups avoid scanning");
    Check(baseline.read_stats().table_scans - base_before.table_scans == 200 && base_examined == 200 * 128,
          "Disabled baseline scans all entries for keys after the last key");
    Check(examined == scans * 128 && examined < base_examined / 10, "Counters demonstrate less decoding with identical results");
    Check(Read(directory.File()) == original, "Rebuilding and querying filters never change SSTable bytes");
    std::cout << "SSTable sample: " << skipped << "/200 lookups skipped; " << examined
              << " vs " << base_examined << " entries examined.\n";
}

void TestFalsePositiveStillUsesExactLookup() {
    TestDirectory directory;
    kvstore::WriteSSTable(directory.File(), Entries(32));
    kvstore::SSTableReader reader(directory.File());
    bool observed = false;
    for (int i = 0; i < 5000; ++i) {
        const auto before = reader.read_stats().table_scans;
        Check(!reader.Get("z-absent:" + std::to_string(i)), "Bloom positive must not manufacture a stored value");
        if (reader.read_stats().table_scans > before) { observed = true; break; }
    }
    Check(observed, "Fixed sample exercises a false positive and the exact fallback");
}

void TestNegativeLeavesCursorAndNextUnfiltered() {
    TestDirectory directory;
    kvstore::WriteSSTable(directory.File(), Entries(3));
    kvstore::BloomFilter model(3);
    for (int i = 0; i < 3; ++i) { model.Add(Key(i)); }
    std::string absent;
    for (int i = 0; i < 1000; ++i) {
        auto candidate = "absent:" + std::to_string(i);
        if (!model.MayContain(candidate)) { absent = std::move(candidate); break; }
    }
    Check(!absent.empty(), "Find a definite negative");
    kvstore::SSTableReader reader(directory.File());
    Check(reader.Next()->key == Key(0), "Cursor reads first entry");
    Check(!reader.Get(absent) && reader.read_stats().bloom_negatives == 1, "Definite negative performs no scan");
    Check(reader.Next()->key == Key(1) && reader.Next()->key == Key(2) && !reader.Next(),
          "Bloom negative leaves shared cursor unchanged, and Next returns all records");
    Check(reader.read_stats().entries_examined == 0, "Next-based iteration does not enter lookup statistics");
}

void TestTombstonesBinaryAndEmptyTables() {
    TestDirectory directory;
    const std::string key("a\0b", 3), value("v\0\xff", 3);
    kvstore::WriteSSTable(directory.File(), {{Type::Value, "", ""}, {Type::Value, key, value},
                                          {Type::Tombstone, "deleted", ""}});
    kvstore::SSTableReader reader(directory.File());
    const auto empty_value = reader.Get(""), binary_value = reader.Get(key);
    Check(empty_value && empty_value->value.empty() && binary_value && binary_value->value == value,
          "Empty and binary keys are included");
    const auto deleted = reader.Get("deleted");
    Check(deleted && deleted->type == Type::Tombstone && reader.read_stats().bloom_negatives == 0,
          "Tombstones count as present keys and cannot be filtered out");
    kvstore::WriteSSTable(directory.File("empty.sst"), {});
    kvstore::SSTableReader empty(directory.File("empty.sst"));
    Check(!empty.Get("") && !empty.Get("missing") && empty.read_stats().bloom_negatives == 2 &&
          empty.read_stats().table_scans == 0, "Empty tables skip every point query");
}

void TestReopenRebuildAndCorruptionValidation() {
    TestDirectory directory;
    kvstore::WriteSSTable(directory.File(), Entries(32));
    const auto original = Read(directory.File());
    for (int generation = 0; generation < 3; ++generation) {
        kvstore::SSTableReader reader(directory.File());
        for (int i = 0; i < 32; ++i) {
            const auto entry = reader.Get(Key(i));
            Check(entry && entry->value == std::to_string(i), "Reopen rebuilds a complete filter");
        }
    }
    auto corrupt = original; corrupt.back() ^= 1;
    Write(directory.File("corrupt.sst"), corrupt);
    for (const bool enabled : {false, true}) {
        Reject([&] { kvstore::SSTableReader reader(directory.File("corrupt.sst"), enabled); });
    }
    auto excessive = original;
    excessive.replace(4, 4, 4, static_cast<char>(0xff));
    Write(directory.File("count.sst"), excessive);
    Reject([&] { kvstore::SSTableReader reader(directory.File("count.sst")); });
    Check(Read(directory.File()) == original && Read(directory.File("corrupt.sst")) == corrupt,
          "Opening neither rewrites valid files nor repairs corrupt files");
}

void TestStoreTombstonePreventsResurrection() {
    TestDirectory directory;
    const auto base = directory.File("store.wal");
    {
        kvstore::Store store(base, kvstore::WalRecoveryMode::Strict, 0, 0);
        store.Put("gone", "old"); store.Put("keep", "yes"); store.Flush();
        store.Delete("gone"); store.Flush();
        const auto before = kvstore::StoreTestAccess::Stats(store);
        Check(!store.Get("gone"), "Newer SSTable tombstone must hide the older value");
        const auto after = kvstore::StoreTestAccess::Stats(store);
        Check(after.get_calls - before.get_calls == 1 && after.table_scans - before.table_scans == 1,
              "Tombstone is read from the newest table, without visiting older files");
        store.Compact();
        Check(!store.Get("gone") && store.Get("keep") == "yes", "Full compaction can remove deletion history safely");
        store.Put("gone", "reborn"); store.RequestFlush(); store.WaitForBackgroundWork();
        Check(store.Get("gone") == "reborn" && kvstore::StoreTestAccess::AllFiltered(store),
              "Background publication installs filters for new SSTables");
    }
    kvstore::Store reopened(base);
    Check(reopened.Get("gone") == "reborn" && reopened.Get("keep") == "yes" &&
          kvstore::StoreTestAccess::AllFiltered(reopened), "Store reopening rebuilds filters without changing data");
}

void TestStoreSkipsMultipleAbsentTables() {
    TestDirectory directory;
    kvstore::Store store(directory.File("store.wal"), kvstore::WalRecoveryMode::Strict, 0, 0);
    for (int group = 0; group < 3; ++group) {
        for (int i = 0; i < 8; ++i) { store.Put("group" + std::to_string(group) + ":" + std::to_string(i), "v"); }
        store.Flush();
    }
    const auto before = kvstore::StoreTestAccess::Stats(store);
    for (int i = 0; i < 200; ++i) { Check(!store.Get("z-missing:" + std::to_string(i)), "Missing Store key"); }
    const auto after = kvstore::StoreTestAccess::Stats(store);
    Check(after.get_calls - before.get_calls == 600 && after.bloom_negatives - before.bloom_negatives > 540,
          "Each missing Store query checks all three table filters and usually skips every scan");
    Check(kvstore::StoreTestAccess::AllFiltered(store), "Every published reader uses filtering by default");
}

void TestPartialAndFullMergeOutputFilters() {
    TestDirectory directory;
    const auto old = directory.File("old.sst"), newer = directory.File("new.sst");
    kvstore::WriteSSTable(old, {{Type::Value, "k", "old"}, {Type::Value, "x", "keep"}});
    kvstore::WriteSSTable(newer, {{Type::Tombstone, "k", ""}, {Type::Value, "y", "new"}});
    const auto partial = directory.File("partial.sst"), full = directory.File("full.sst");
    kvstore::MergeSSTables({newer}, partial);
    kvstore::SSTableReader partial_reader(partial);
    const auto tombstone = partial_reader.Get("k");
    Check(tombstone && tombstone->type == Type::Tombstone, "Partial output filter includes retained tombstones");
    kvstore::MergeSSTables({old, partial}, full, true);
    kvstore::SSTableReader full_reader(full);
    const auto x = full_reader.Get("x"), y = full_reader.Get("y");
    Check(!full_reader.Get("k") && x && x->value == "keep" && y && y->value == "new",
          "Full output filter follows the actual deduplicated output keys");
}

void TestConcurrentLookupsWithFlushAndCompaction() {
    TestDirectory directory;
    const auto base = directory.File("store.wal");
    {
        kvstore::Store store(base, kvstore::WalRecoveryMode::Strict, 128, 4);
        std::vector<std::future<void>> clients;
        for (int client = 0; client < 3; ++client) {
            clients.push_back(std::async(std::launch::async, [&, client] {
                for (int i = 0; i < 12; ++i) {
                    const auto key = "client" + std::to_string(client) + ":" + std::to_string(i);
                    store.Put(key, "old"); store.Delete(key); store.Put(key, "final");
                    Check(store.Get(key) == "final" && !store.Get("never-written"), "Filtered positive and negative queries stay exact");
                    if (i % 4 == 0) { store.RequestFlush(); store.RequestCompaction(); }
                }
            }));
        }
        for (auto& client : clients) { client.get(); }
        store.Flush(); store.Compact(); store.WaitForBackgroundWork();
        Check(kvstore::StoreTestAccess::AllFiltered(store), "Compaction replacement readers build filters");
    }
    kvstore::Store reopened(base);
    for (int client = 0; client < 3; ++client) {
        for (int i = 0; i < 12; ++i) {
            Check(reopened.Get("client" + std::to_string(client) + ":" + std::to_string(i)) == "final",
                  "All 36 concurrent client results survive reopening");
        }
    }
}

int main() {
    try {
        TestEnabledDisabledEquivalenceAndScanCounts(); TestFalsePositiveStillUsesExactLookup();
        TestNegativeLeavesCursorAndNextUnfiltered(); TestTombstonesBinaryAndEmptyTables();
        TestReopenRebuildAndCorruptionValidation(); TestStoreTombstonePreventsResurrection();
        TestStoreSkipsMultipleAbsentTables(); TestPartialAndFullMergeOutputFilters();
        TestConcurrentLookupsWithFlushAndCompaction();
        std::cout << "All 9 SSTable Bloom tests passed.\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "SSTable Bloom test failed: " << error.what() << '\n';
        return 1;
    }
}
