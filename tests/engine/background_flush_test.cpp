#include "kvstore/store.h"

#include <chrono>
#include <filesystem>
#include <fstream>
#include <future>
#include <iostream>
#include <iterator>
#include <stdexcept>
#include <utility>

#include <unistd.h>

namespace kvstore {
struct StoreTestAccess {
    using Stage = Store::FlushStage;
    using FreezeStage = Store::FreezeStage;
    static void Hook(Store& store, std::function<void(Stage)> callback) {
        std::lock_guard<std::mutex> lock(store.mutex_);
        store.flush_hook_ = std::move(callback);
    }
    static void FreezeHook(Store& store, std::function<void(FreezeStage)> callback) {
        std::lock_guard<std::mutex> lock(store.mutex_);
        store.freeze_hook_ = std::move(callback);
    }
};
}  // namespace kvstore

using Stage = kvstore::StoreTestAccess::Stage;
using FreezeStage = kvstore::StoreTestAccess::FreezeStage;
using namespace std::chrono_literals;

void Check(bool condition, const std::string& message) {
    if (!condition) { throw std::runtime_error(message); }
}
void Reject(const std::function<void()>& action) {
    bool failed = false;
    try { action(); } catch (const std::exception&) { failed = true; }
    Check(failed, "Unusable Store must report an error");
}
class TestDirectory {
public:
    TestDirectory() {
        char pattern[] = "/tmp/kvstore-background-flush-XXXXXX";
        const auto path = ::mkdtemp(pattern);
        Check(path != nullptr, "Create test directory");
        path_ = path;
    }
    ~TestDirectory() {
        std::error_code ignored;
        std::filesystem::remove_all(path_, ignored);
    }
    std::string Base() const { return (path_ / "store.wal").string(); }
private:
    std::filesystem::path path_;
};
std::string Read(const std::string& path) {
    std::ifstream input(path, std::ios::binary);
    Check(input.is_open(), "Read file");
    return {std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
}
kvstore::Manifest Metadata(const std::string& base) {
    return kvstore::DecodeManifest(Read(base + ".manifest"));
}
std::string Wal(const std::string& base, std::uint64_t id) {
    return id == 0 ? base : base + ".wal." + std::to_string(id);
}

// The gate outlives its callback and always releases the worker during unwinding.
class PauseFlush {
public:
    explicit PauseFlush(kvstore::Store& store) : state_(std::make_shared<State>()) {
        kvstore::StoreTestAccess::Hook(store, [state = state_](Stage stage) {
            if (stage != Stage::SSTableWritten) { return; }
            std::unique_lock<std::mutex> lock(state->mutex);
            if (state->entered) { return; }
            state->entered = true;
            state->condition.notify_all();
            state->condition.wait(lock, [&] { return state->released; });
        });
    }
    ~PauseFlush() { Release(); }
    void Wait() {
        std::unique_lock<std::mutex> lock(state_->mutex);
        Check(state_->condition.wait_for(lock, 5s, [&] { return state_->entered; }), "Worker reaches flush gate");
    }
    void Release() {
        std::lock_guard<std::mutex> lock(state_->mutex);
        state_->released = true;
        state_->condition.notify_all();
    }
private:
    struct State {
        std::mutex mutex;
        std::condition_variable condition;
        bool entered = false;
        bool released = false;
    };
    std::shared_ptr<State> state_;
};

void TestFrozenReadPrecedenceAndRecovery() {
    TestDirectory directory;
    const auto base = directory.Base();
    const std::string binary("v\0x", 3);
    {
        kvstore::Store store(base, kvstore::WalRecoveryMode::Strict, 0, 0);
        store.Put("k", "disk"); store.Put("gone", "disk"); store.Flush();
        store.Put("k", "frozen"); store.Put("frozen-only", "saved"); store.Delete("gone");
        store.Put("empty", ""); store.Put("binary", binary);
        const auto frozen_bytes = store.memtable_bytes();
        PauseFlush pause(store);
        store.RequestFlush(); pause.Wait();
        const auto during = Metadata(base);
        const auto old_wal = Wal(base, *during.frozen_wal_id);
        const auto old_bytes = Read(old_wal);
        Check(store.memtable_bytes() == 0 && store.frozen_memtable_bytes() == frozen_bytes &&
              store.sstable_count() == 1, "Freeze transfers ownership without publishing SSTable");
        Check(store.Get("k") == "frozen" && !store.Get("gone") && store.Get("empty") == "" &&
              store.Get("binary") == binary, "Frozen table overrides older SSTables including tombstones and binary data");
        store.Put("k", "active"); store.Delete("frozen-only"); store.Put("gone", "reborn");
        Check(Read(old_wal) == old_bytes, "New writes use only the active WAL");
        Check(store.Get("k") == "active" && !store.Get("frozen-only") && store.Get("gone") == "reborn",
              "Active values and tombstones override frozen table");
        pause.Release(); store.WaitForBackgroundWork();
        Check(!Metadata(base).frozen_wal_id && !std::filesystem::exists(old_wal) &&
              store.frozen_memtable_bytes() == 0, "Only durable SSTable publication permits WAL retirement");
    }
    kvstore::Store reopened(base);
    Check(reopened.Get("k") == "active" && !reopened.Get("frozen-only") && reopened.Get("gone") == "reborn" &&
          reopened.Get("empty") == "" && reopened.Get("binary") == binary, "Frozen and active states survive reopening");
}

void TestFlushWaitsForExistingFrozenTable() {
    TestDirectory directory;
    kvstore::Store store(directory.Base(), kvstore::WalRecoveryMode::Strict, 0, 0);
    store.Put("k", "v");
    PauseFlush pause(store);
    store.RequestFlush(); pause.Wait();
    auto waiter = std::async(std::launch::async, [&] { store.Flush(); });
    const bool waiting = waiter.wait_for(20ms) == std::future_status::timeout;
    pause.Release(); waiter.get();
    Check(waiting && store.sstable_count() == 1, "Flush with empty active table still waits for frozen table");
}

void TestOneFrozenSlot() {
    TestDirectory directory;
    const auto base = directory.Base();
    kvstore::Store store(base, kvstore::WalRecoveryMode::Strict, 0, 0);
    store.Put("a", "first");
    PauseFlush pause(store);
    store.RequestFlush(); pause.Wait();
    const auto before = Read(base + ".manifest");
    const auto bytes = store.frozen_memtable_bytes();
    store.Put("b", "second");
    auto next = std::async(std::launch::async, [&] { store.RequestFlush(); });
    const bool waiting = next.wait_for(20ms) == std::future_status::timeout;
    const bool unchanged = Read(base + ".manifest") == before && store.frozen_memtable_bytes() == bytes &&
                           store.Get("a") == "first" && store.Get("b") == "second";
    pause.Release(); next.get(); store.WaitForBackgroundWork();
    Check(waiting && unchanged && store.sstable_count() == 2, "Second freeze waits without creating a third recovery WAL");
}

void TestAutomaticBackpressure() {
    TestDirectory directory;
    kvstore::Store store(directory.Base(), kvstore::WalRecoveryMode::Strict, 40, 0);
    PauseFlush pause(store);
    store.Put("a", "x"); store.Put("b", "x"); store.Put("c", "x");
    pause.Wait();
    store.Put("d", "x"); store.Put("e", "x");
    auto first = std::async(std::launch::async, [&] { store.Put("f", "x"); });
    bool first_waiting = first.wait_for(20ms) == std::future_status::timeout;
    // The full active table is visible before the writer can freeze it.
    bool bounded = store.memtable_bytes() == 57 && store.frozen_memtable_bytes() == 57;
    auto second = std::async(std::launch::async, [&] { store.Put("g", "x"); });
    const bool second_waiting = second.wait_for(20ms) == std::future_status::timeout;
    bounded = bounded && !store.Get("g") && store.memtable_bytes() == 57;
    pause.Release(); first.get(); second.get(); store.WaitForBackgroundWork();
    Check(first_waiting && second_waiting && bounded, "Full active and frozen tables apply backpressure before further WAL writes");
    for (const auto& key : {"a", "b", "c", "d", "e", "f", "g"}) {
        Check(store.Get(key) == "x", "Backpressure preserves every write");
    }
}

void TestWaitIncludesFlushAndItsCompaction() {
    TestDirectory directory;
    kvstore::Store store(directory.Base(), kvstore::WalRecoveryMode::Strict, 0, 4);
    for (int i = 0; i < 4; ++i) {
        store.Put("k", std::to_string(i)); store.RequestFlush();
    }
    store.WaitForBackgroundWork();
    Check(store.frozen_memtable_bytes() == 0 && store.sstable_count() == 1 && store.Get("k") == "3",
          "Background wait includes the flush target and resulting automatic compaction");
}

void TestCompactionPublishesBeforeFrozenFlush() {
    TestDirectory directory;
    const auto base = directory.Base();
    {
        kvstore::Store store(base, kvstore::WalRecoveryMode::Strict, 0, 0);
        store.Put("k", "old"); store.Put("gone", "old"); store.Flush();
        store.Put("k", "disk-new"); store.Flush();
        const auto output_id = Metadata(base).next_file_id;
        store.Put("k", "frozen"); store.Delete("gone");
        PauseFlush pause(store);
        store.RequestFlush(); pause.Wait();
        const auto during = Metadata(base);
        store.Compact();
        const auto compacted = Metadata(base);
        Check(compacted.sstable_ids.size() == 1 && compacted.frozen_wal_id == during.frozen_wal_id &&
              compacted.wal_id == during.wal_id, "Compaction preserves the live frozen WAL");
        Check(store.Get("k") == "frozen" && !store.Get("gone"), "Frozen table wins during compaction publication");
        store.Put("k", "active");
        pause.Release(); store.WaitForBackgroundWork();
        const auto after = Metadata(base);
        Check(after.sstable_ids == std::vector<std::uint64_t>{compacted.sstable_ids[0], output_id},
              "Flush appends to current compacted metadata; lookup order can differ from numeric IDs");
    }
    kvstore::Store reopened(base);
    Check(reopened.Get("k") == "active" && !reopened.Get("gone"), "Both publication orders preserve latest values");
}

void TestBackgroundFlushFailures() {
    for (const auto failure : {Stage::SSTableWritten, Stage::ManifestRenamed, Stage::OldWalRemoved}) {
        TestDirectory directory;
        const auto base = directory.Base();
        {
            kvstore::Store store(base, kvstore::WalRecoveryMode::Strict, 0, 0);
            store.Put("k", "v"); store.Delete("gone");
            kvstore::StoreTestAccess::Hook(store, [failure](Stage stage) {
                if (stage == failure) { throw std::runtime_error("Injected background flush failure"); }
            });
            store.RequestFlush();
            Reject([&] { store.WaitForBackgroundWork(); });
            Reject([&] { store.Get("k"); }); Reject([&] { store.Put("bad", "v"); });
            Reject([&] { store.Delete("k"); }); Reject([&] { store.Flush(); });
            Reject([&] { store.RequestFlush(); }); Reject([&] { store.Compact(); });
            Reject([&] { store.RequestCompaction(); });
        }
        kvstore::Store reopened(base); reopened.WaitForBackgroundWork();
        Check(reopened.Get("k") == "v" && !reopened.Get("gone") && !reopened.Get("bad"),
              "Worker failure preserves acknowledged state and stops both workers");
    }
}

void TestFreezeFailures() {
    for (const auto failure : {FreezeStage::WalSynced, FreezeStage::ManifestTempSynced, FreezeStage::ManifestRenamed,
                              FreezeStage::ManifestDurable, FreezeStage::MemTableFrozen}) {
        TestDirectory directory;
        const auto base = directory.Base();
        {
            kvstore::Store store(base, kvstore::WalRecoveryMode::Strict, 0, 0);
            store.Put("k", "v"); store.Delete("gone");
            kvstore::StoreTestAccess::FreezeHook(store, [failure](FreezeStage stage) {
                if (stage == failure) { throw std::runtime_error("Injected freeze failure"); }
            });
            Reject([&] { store.RequestFlush(); });
            Reject([&] { store.Get("k"); }); Reject([&] { store.Put("bad", "v"); });
            Reject([&] { store.WaitForBackgroundWork(); });
        }
        kvstore::Store reopened(base); reopened.WaitForBackgroundWork();
        Check(reopened.Get("k") == "v" && !reopened.Get("gone") && !reopened.Get("bad"),
              "Freeze failure recovers from either authoritative WAL set");
    }
}

void TestDestructorDrainsFlushThenCompaction() {
    TestDirectory directory;
    const auto base = directory.Base();
    auto store = std::make_unique<kvstore::Store>(base, kvstore::WalRecoveryMode::Strict, 0, 2);
    store->Put("k", "one"); store->Flush();
    PauseFlush pause(*store);
    store->Put("k", "two"); store->RequestFlush(); pause.Wait();
    store->Put("pending", "wal");
    auto closer = std::async(std::launch::async, [owned = std::move(store)]() mutable { owned.reset(); });
    const bool waiting = closer.wait_for(20ms) == std::future_status::timeout;
    pause.Release(); closer.get();
    kvstore::Store reopened(base);
    Check(waiting && reopened.sstable_count() == 1 && reopened.Get("k") == "two" && reopened.Get("pending") == "wal",
          "Shutdown drains flush, then its automatic compaction, leaving unrequested active entries in WAL");
}

void TestConcurrentAutomaticFlushRequests() {
    TestDirectory directory;
    const auto base = directory.Base();
    {
        kvstore::Store store(base, kvstore::WalRecoveryMode::Strict, 128, 4);
        std::vector<std::future<void>> clients;
        for (int client = 0; client < 3; ++client) {
            clients.push_back(std::async(std::launch::async, [&, client] {
                for (int i = 0; i < 16; ++i) {
                    const auto key = std::to_string(client) + ":" + std::to_string(i);
                    store.Put(key, "old"); store.Delete(key); store.Put(key, "final");
                    if (i % 5 == 0) { store.RequestFlush(); store.RequestCompaction(); }
                    Check(store.Get(key) == "final", "Concurrent read after writes into active or frozen tables");
                    (void)store.frozen_memtable_bytes();
                }
                store.Flush();
            }));
        }
        for (auto& client : clients) { client.get(); }
        store.WaitForBackgroundWork();
    }
    kvstore::Store reopened(base);
    for (int client = 0; client < 3; ++client) {
        for (int i = 0; i < 16; ++i) {
            Check(reopened.Get(std::to_string(client) + ":" + std::to_string(i)) == "final",
                  "Concurrent automatic flush and manual requests recover all 48 keys");
        }
    }
}

int main() {
    try {
        TestFrozenReadPrecedenceAndRecovery(); TestFlushWaitsForExistingFrozenTable(); TestOneFrozenSlot();
        TestAutomaticBackpressure(); TestWaitIncludesFlushAndItsCompaction(); TestCompactionPublishesBeforeFrozenFlush();
        TestBackgroundFlushFailures(); TestFreezeFailures(); TestDestructorDrainsFlushThenCompaction();
        TestConcurrentAutomaticFlushRequests();
        std::cout << "All 10 background flush tests passed.\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "Background flush test failed: " << error.what() << '\n';
        return 1;
    }
}
