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
    using Stage = Store::CompactStage;
    static void Hook(Store& store, std::function<void(Stage)> callback) {
        std::lock_guard<std::mutex> lock(store.mutex_);
        store.compact_hook_ = std::move(callback);
    }
};
}  // namespace kvstore

using Stage = kvstore::StoreTestAccess::Stage;
using namespace std::chrono_literals;

void Check(bool condition, const std::string& message) {
    if (!condition) { throw std::runtime_error(message); }
}
void Reject(const std::function<void()>& action) {
    bool failed = false;
    try { action(); } catch (const std::runtime_error&) { failed = true; }
    Check(failed, "Background error must reach caller");
}
class TestDirectory {
public:
    TestDirectory() {
        char pattern[] = "/tmp/kvstore-background-XXXXXX";
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

// Declare after Store so unwinding releases the gate before Store joins its worker.
class PauseMerge {
public:
    explicit PauseMerge(kvstore::Store& store) : state_(std::make_shared<State>()) {
        kvstore::StoreTestAccess::Hook(store, [state = state_](Stage stage) {
            if (stage != Stage::SSTableWritten) { return; }
            std::unique_lock<std::mutex> lock(state->mutex);
            if (state->entered) { return; }
            state->entered = true;
            state->condition.notify_all();
            state->condition.wait(lock, [&] { return state->released; });
        });
    }
    ~PauseMerge() { Release(); }
    void Wait() {
        std::unique_lock<std::mutex> lock(state_->mutex);
        Check(state_->condition.wait_for(lock, 5s, [&] { return state_->entered; }), "Worker must reach merge gate");
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

void Seed(kvstore::Store& store) {
    store.Put("k", "old"); store.Put("keep", "yes"); store.Flush();
    store.Put("k", "new"); store.Delete("gone"); store.Flush();
}

void TestForegroundDuringMerge() {
    TestDirectory directory;
    const auto base = directory.Base();
    {
        kvstore::Store store(base, kvstore::WalRecoveryMode::Strict, 0, 0);
        Seed(store);
        PauseMerge pause(store);
        store.RequestCompaction(); pause.Wait();
        Check(store.Get("k") == "new", "Old readers serve queries during merge");
        store.Put("k", "pending");
        Check(store.Delete("keep"), "Delete sees complete disk view");
        store.Put("fresh", "value");
        const auto before = Metadata(base);
        const auto wal_path = base + ".wal." + std::to_string(before.wal_id);
        const auto wal_bytes = Read(wal_path);
        const auto memory_bytes = store.memtable_bytes();
        pause.Release(); store.WaitForBackgroundWork();
        Check(store.sstable_count() == 1 && store.memtable_bytes() == memory_bytes,
              "Merge compacts disk only");
        Check(Read(wal_path) == wal_bytes && Metadata(base).wal_id == before.wal_id, "Pending WAL stays unchanged");
        Check(store.Get("k") == "pending" && !store.Get("keep"), "Pending values and tombstones win");
    }
    kvstore::Store reopened(base);
    Check(reopened.Get("k") == "pending" && !reopened.Get("keep") && reopened.Get("fresh") == "value",
          "Acknowledged writes during merge recover");
}

void TestFlushDuringMerge() {
    TestDirectory directory;
    const auto base = directory.Base();
    {
        kvstore::Store store(base, kvstore::WalRecoveryMode::Strict, 0, 0);
        Seed(store);
        const auto original = Metadata(base);
        PauseMerge pause(store);
        store.RequestCompaction(); pause.Wait();
        store.Put("k", "flushed"); store.Delete("keep"); store.Put("gone", "reborn"); store.Flush();
        store.Put("extra", "tail"); store.Flush();
        const auto during = Metadata(base);
        Check(during.sstable_ids[2] > original.next_file_id, "Flush skips reserved compaction output ID");
        pause.Release(); store.WaitForBackgroundWork();
        const auto after = Metadata(base);
        Check(after.sstable_ids == std::vector<std::uint64_t>{original.next_file_id,
              during.sstable_ids[2], during.sstable_ids[3]}, "Publish preserves newer SSTable suffix in order");
        Check(after.wal_id == during.wal_id && after.next_file_id == during.next_file_id,
              "Publish keeps current WAL and file counter");
        Check(store.Get("k") == "flushed" && !store.Get("keep") && store.Get("gone") == "reborn",
              "New files override merged prefix");
        for (const auto id : original.sstable_ids) {
            Check(!std::filesystem::exists(base + ".sst." + std::to_string(id)), "Only merged inputs retire");
        }
    }
    kvstore::Store reopened(base);
    Check(reopened.Get("k") == "flushed" && !reopened.Get("keep") && reopened.Get("gone") == "reborn" &&
          reopened.Get("extra") == "tail", "Concurrent flush and compaction recover together");
}

void TestRequestDuringMerge() {
    TestDirectory directory;
    kvstore::Store store(directory.Base(), kvstore::WalRecoveryMode::Strict, 0, 0);
    Seed(store);
    PauseMerge pause(store);
    store.RequestCompaction(); pause.Wait();
    store.Put("k", "last"); store.Flush();
    store.RequestCompaction(); store.RequestCompaction();
    pause.Release(); store.WaitForBackgroundWork();
    Check(store.sstable_count() == 1 && store.Get("k") == "last", "Requests arriving during merge trigger a later pass");
}

void TestAutomaticThreshold() {
    for (const std::size_t threshold : {0, 2, 4}) {
        TestDirectory directory;
        kvstore::Store store(directory.Base(), kvstore::WalRecoveryMode::Strict, 0, threshold);
        PauseMerge pause(store);
        const auto count = threshold == 0 ? 4 : threshold;
        for (std::size_t i = 0; i < count; ++i) {
            store.Put("k", std::to_string(i)); store.Flush();
        }
        if (threshold != 0) { pause.Wait(); pause.Release(); }
        store.WaitForBackgroundWork();
        Check(store.sstable_count() == (threshold == 0 ? count : 1), "Automatic threshold or explicit disable");
        Check(store.Get("k") == std::to_string(count - 1), "Automatic merge keeps latest value");
    }
}

void TestAutomaticFlushSchedulesMerge() {
    TestDirectory directory;
    kvstore::Store store(directory.Base(), kvstore::WalRecoveryMode::Strict, 1, 2);
    PauseMerge pause(store);
    store.Put("k", "one"); store.Put("k", "two");
    pause.Wait();
    Check(store.memtable_bytes() == 0 && store.Get("k") == "two", "Threshold Flush feeds background merge");
    pause.Release(); store.WaitForBackgroundWork();
    Check(store.sstable_count() == 1, "Automatic flush and compaction cooperate");
}

void TestBackgroundFailures() {
    for (const auto failure : {Stage::SSTableWritten, Stage::ManifestRenamed, Stage::OldTableRemoved}) {
        TestDirectory directory;
        const auto base = directory.Base();
        {
            kvstore::Store store(base, kvstore::WalRecoveryMode::Strict, 0, 0);
            Seed(store);
            kvstore::StoreTestAccess::Hook(store, [failure](Stage stage) {
                if (stage == failure) { throw std::runtime_error("Injected background failure"); }
            });
            store.RequestCompaction();
            Reject([&] { store.WaitForBackgroundWork(); });
            Reject([&] { store.Get("k"); });
            Reject([&] { store.Put("rejected", "v"); });
            Reject([&] { store.Delete("k"); });
            Reject([&] { store.Flush(); });
            Reject([&] { store.Compact(); });
            Reject([&] { store.RequestCompaction(); });
        }
        kvstore::Store reopened(base);
        Check(reopened.Get("k") == "new" && reopened.Get("keep") == "yes" && !reopened.Get("rejected"),
              "Old or published file set recovers after worker error");
        reopened.Compact();
    }
}

void TestConcurrentCalls() {
    TestDirectory directory;
    const auto base = directory.Base();
    {
        kvstore::Store store(base, kvstore::WalRecoveryMode::Strict, 0, 4);
        std::vector<std::future<void>> clients;
        for (int client = 0; client < 3; ++client) {
            clients.push_back(std::async(std::launch::async, [&, client] {
                for (int i = 0; i < 20; ++i) {
                    const auto key = std::to_string(client) + ":" + std::to_string(i);
                    store.Put(key, "first");
                    Check(store.Get(key) == "first", "Concurrent disjoint-key read after write");
                    store.Delete(key); Check(!store.Get(key), "Concurrent delete");
                    store.Put(key, "final");
                    if (i % 7 == 0) { store.Flush(); store.RequestCompaction(); }
                    (void)store.sstable_count(); (void)store.memtable_bytes();
                }
                store.Compact();
            }));
        }
        for (auto& client : clients) { client.get(); }
        store.WaitForBackgroundWork();
    }
    kvstore::Store reopened(base);
    for (int client = 0; client < 3; ++client) {
        for (int i = 0; i < 20; ++i) {
            Check(reopened.Get(std::to_string(client) + ":" + std::to_string(i)) == "final",
                  "All concurrent client results recover");
        }
    }
}

void TestDestructorDrainsWork() {
    TestDirectory directory;
    const auto base = directory.Base();
    auto store = std::make_unique<kvstore::Store>(base, kvstore::WalRecoveryMode::Strict, 0, 0);
    Seed(*store);
    PauseMerge pause(*store);
    store->RequestCompaction(); pause.Wait();
    store->Put("k", "last"); store->Flush(); store->RequestCompaction();
    std::promise<void> started;
    auto ready = started.get_future();
    auto closer = std::async(std::launch::async, [owned = std::move(store), &started]() mutable {
        started.set_value(); owned.reset();
    });
    ready.wait();
    const bool waiting = closer.wait_for(20ms) == std::future_status::timeout;
    pause.Release(); closer.get();
    Check(waiting, "Destruction waits for active worker before releasing resources");
    kvstore::Store reopened(base);
    Check(reopened.sstable_count() == 1 && reopened.Get("k") == "last", "Destruction drains queued later pass");
}

void TestEmptyRequestsAndIdleShutdown() {
    TestDirectory directory;
    const auto base = directory.Base();
    {
        kvstore::Store store(base);
        const auto before = Read(base + ".manifest");
        store.WaitForBackgroundWork(); store.RequestCompaction(); store.Compact(); store.WaitForBackgroundWork();
        Check(Read(base + ".manifest") == before, "Empty jobs do not allocate or publish files");
        store.Put("pending", "wal");
    }
    kvstore::Store reopened(base);
    Check(reopened.sstable_count() == 0 && reopened.Get("pending") == "wal", "Exit does not implicitly Flush memory");
}

int main() {
    try {
        TestForegroundDuringMerge(); TestFlushDuringMerge(); TestRequestDuringMerge();
        TestAutomaticThreshold(); TestAutomaticFlushSchedulesMerge(); TestBackgroundFailures();
        TestConcurrentCalls(); TestDestructorDrainsWork(); TestEmptyRequestsAndIdleShutdown();
        std::cout << "All 9 background tests passed.\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "Background test failed: " << error.what() << '\n';
        return 1;
    }
}
