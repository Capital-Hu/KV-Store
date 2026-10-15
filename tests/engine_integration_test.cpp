#include "kvstore/store.h"

#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <csignal>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <iterator>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <set>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <poll.h>
#include <sys/wait.h>
#include <unistd.h>

namespace kvstore {
// Hooks exist only in this test executable, never in the production API.
struct StoreTestAccess {
    using FreezeStage = Store::FreezeStage;
    using FlushStage = Store::FlushStage;
    using CompactStage = Store::CompactStage;
    static void FreezeHook(Store& store, std::function<void(FreezeStage)> hook) {
        std::lock_guard<std::mutex> lock(store.mutex_);
        store.freeze_hook_ = std::move(hook);
    }
    static void FlushHook(Store& store, std::function<void(FlushStage)> hook) {
        std::lock_guard<std::mutex> lock(store.mutex_);
        store.flush_hook_ = std::move(hook);
    }
    static void CompactHook(Store& store, std::function<void(CompactStage)> hook) {
        std::lock_guard<std::mutex> lock(store.mutex_);
        store.compact_hook_ = std::move(hook);
    }
};
}  // namespace kvstore

namespace {
using Model = std::map<std::string, std::string>;
using FreezeStage = kvstore::StoreTestAccess::FreezeStage;
using FlushStage = kvstore::StoreTestAccess::FlushStage;
using CompactStage = kvstore::StoreTestAccess::CompactStage;
using namespace std::chrono_literals;

void Check(bool condition, const std::string& message) {
    if (!condition) { throw std::runtime_error(message); }
}

class TestDirectory {
public:
    TestDirectory() {
        char pattern[] = "/tmp/kvstore-integration-XXXXXX";
        const auto path = ::mkdtemp(pattern);
        Check(path != nullptr, "Create integration directory");
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

const std::vector<std::string>& Keys() {
    static const auto keys = [] {
        std::vector<std::string> result{"", std::string("bin\0key", 7)};
        for (int i = 2; i < 32; ++i) { result.push_back("key-" + std::to_string(i)); }
        return result;
    }();
    return keys;
}

std::optional<std::string> Expected(const Model& model, const std::string& key) {
    const auto found = model.find(key);
    return found == model.end() ? std::nullopt : std::optional<std::string>(found->second);
}

// The oracle uses only ordinary map operations, never engine codecs or file readers.
void CheckModel(kvstore::Store& store, const Model& model, const std::string& where) {
    for (std::size_t i = 0; i < Keys().size(); ++i) {
        Check(store.Get(Keys()[i]) == Expected(model, Keys()[i]), where + ": key " + std::to_string(i));
    }
    Check(!store.Get("never-written"), where + ": missing key");
    const auto stats = store.block_cache_stats();
    Check(stats.resident_bytes <= stats.capacity_bytes, where + ": cache budget");
    if (stats.capacity_bytes == 0) {
        Check(stats.entries == 0 && stats.hits == 0, where + ": disabled cache stays empty");
    }
}

struct Mutation {
    std::string key;
    std::optional<std::string> value;  // nullopt means Delete; an empty string still means Put.
};

void ApplyModel(Model& model, const Mutation& mutation) {
    if (mutation.value) { model[mutation.key] = *mutation.value; }
    else { model.erase(mutation.key); }
}

void ApplyBoth(kvstore::Store& store, Model& model, const Mutation& mutation) {
    if (mutation.value) { store.Put(mutation.key, *mutation.value); }
    else { Check(store.Delete(mutation.key) == (model.count(mutation.key) != 0), "Delete result matches model"); }
    ApplyModel(model, mutation);
}

std::string ReadFile(const std::string& path) {
    std::ifstream input(path, std::ios::binary);
    Check(input.is_open(), "Read integration metadata");
    return {std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
}

// Call only after workers are idle; compare the whole reserved directory to the manifest.
void CheckLiveFiles(const std::string& base) {
    const auto manifest = kvstore::DecodeManifest(ReadFile(base + ".manifest"));
    Check(!manifest.frozen_wal_id, "Recovery drains the frozen WAL");
    std::set<std::string> expected{base + ".lock", base + ".manifest"};
    expected.insert(manifest.wal_id == 0 ? base : base + ".wal." + std::to_string(manifest.wal_id));
    for (const auto id : manifest.sstable_ids) { expected.insert(base + ".sst." + std::to_string(id)); }
    std::set<std::string> actual;
    for (const auto& file : std::filesystem::directory_iterator(std::filesystem::path(base).parent_path())) {
        actual.insert(file.path().string());
    }
    Check(actual == expected, "Only manifest-referenced data files remain");
}

// Callback state is shared so it survives unwinding until Store joins its workers.
class Gate {
public:
    Gate() : state_(std::make_shared<State>()) {}
    ~Gate() { Release(); }
    std::function<void()> Pause() const {
        return [state = state_] {
            std::unique_lock<std::mutex> lock(state->mutex);
            state->entered = true;
            state->condition.notify_all();
            state->condition.wait(lock, [&] { return state->released; });
        };
    }
    void Wait() {
        std::unique_lock<std::mutex> lock(state_->mutex);
        Check(state_->condition.wait_for(lock, 10s, [&] { return state_->entered; }), "Worker reaches gate");
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

Model SeedHistory(const std::string& base, std::size_t capacity) {
    Model model;
    kvstore::Store store(base, kvstore::WalRecoveryMode::Strict, 0, 0, capacity);
    for (std::size_t i = 0; i < Keys().size(); ++i) {
        ApplyBoth(store, model, {Keys()[i], std::string(500, static_cast<char>('a' + i % 26))});
    }
    store.Flush();
    ApplyBoth(store, model, {Keys()[2], "newer-on-disk"});
    ApplyBoth(store, model, {Keys()[3], std::nullopt});
    store.Flush();
    Check(store.sstable_count() == 2, "Seed two overlapping SSTables");
    return model;
}

void TestMixedModelAcrossReopens() {
    for (const std::size_t capacity : {std::size_t{0}, std::size_t{8192}}) {
        for (const unsigned int seed : {7U, 20261015U}) {
            TestDirectory directory;
            Model model;
            std::mt19937 random(seed);
            for (int epoch = 0; epoch < 3; ++epoch) {
                kvstore::Store store(directory.Base(), kvstore::WalRecoveryMode::Strict, 2048, 3, capacity);
                Check(store.block_cache_stats().entries == 0, "Reopen starts with a cold cache");
                CheckModel(store, model, "Before mixed epoch");
                for (int i = 0; i < 180; ++i) {
                    const auto& key = Keys()[random() % Keys().size()];
                    const auto operation = random() % 10;
                    if (operation < 6) {
                        std::string value(random() % 900, static_cast<char>(random() % 256));
                        ApplyBoth(store, model, {key, std::move(value)});
                    } else if (operation < 8) {
                        ApplyBoth(store, model, {key, std::nullopt});
                    } else {
                        Check(store.Get(key) == Expected(model, key), "Mixed Get matches independent model");
                    }
                    if (i % 23 == 0) { store.RequestFlush(); }
                    if (i % 37 == 0) { store.RequestCompaction(); }
                    if (i % 41 == 0) { CheckModel(store, model, "During mixed epoch"); }
                }
                store.WaitForBackgroundWork();
                Check(store.sstable_count() != 0, "Mixed workload reaches SSTables");
                CheckModel(store, model, "After mixed epoch");
                // Leave a known pending WAL write for the next open, independent of automatic thresholds.
                store.Flush(); store.WaitForBackgroundWork();
                ApplyBoth(store, model, {Keys()[31], "pending-epoch-" + std::to_string(epoch)});
                Check(store.memtable_bytes() != 0, "Epoch ends with an unflushed WAL write");
            }
            kvstore::Store reopened(directory.Base(), kvstore::WalRecoveryMode::Strict, 0, 0, capacity);
            CheckModel(reopened, model, "Final mixed reopen");
            reopened.Flush(); reopened.Compact();
            CheckModel(reopened, model, "Final mixed merge");
            CheckLiveFiles(directory.Base());
        }
    }
}

void TestFourLayerPrecedenceAndPublication() {
    for (const std::size_t capacity : {std::size_t{0}, std::size_t{8192}}) {
        TestDirectory directory;
        auto model = SeedHistory(directory.Base(), capacity);
        {
            kvstore::Store store(directory.Base(), kvstore::WalRecoveryMode::Strict, 0, 0, capacity);
            // Read a small block twice before other blocks can evict it.
            Check(store.Get(Keys()[2]) == Expected(model, Keys()[2]), "Warm newer SSTable");
            Check(store.Get(Keys()[2]) == Expected(model, Keys()[2]), "Repeat cached lookup");
            if (capacity != 0) { Check(store.block_cache_stats().hits != 0, "Cache path is exercised"); }
            Gate gate;
            kvstore::StoreTestAccess::FlushHook(store, [pause = gate.Pause()](FlushStage stage) {
                if (stage == FlushStage::SSTableWritten) { pause(); }
            });
            ApplyBoth(store, model, {Keys()[2], "frozen-new-value"});
            ApplyBoth(store, model, {Keys()[4], std::nullopt});
            ApplyBoth(store, model, {Keys()[3], "frozen-reinsert"});
            store.RequestFlush(); gate.Wait();
            Check(store.frozen_memtable_bytes() != 0, "Frozen layer is present");
            ApplyBoth(store, model, {Keys()[2], std::nullopt});
            ApplyBoth(store, model, {Keys()[4], "active-reinsert"});
            ApplyBoth(store, model, {Keys()[5], "active-only-update"});
            Check(store.memtable_bytes() != 0 && store.sstable_count() == 2, "Four read layers coexist");
            CheckModel(store, model, "Active over frozen over newer and older disk");
            store.Compact();
            Check(store.sstable_count() == 1, "Old disk layers merge while flush is paused");
            Check(store.block_cache_stats().entries == 0, "Retired readers release cached blocks");
            CheckModel(store, model, "After compaction before flush publication");
            gate.Release(); store.WaitForBackgroundWork();
            CheckModel(store, model, "After frozen flush publication");
            store.Flush(); store.Compact();
            CheckModel(store, model, "After all layers reach compacted disk");
            CheckLiveFiles(directory.Base());
        }
        kvstore::Store reopened(directory.Base(), kvstore::WalRecoveryMode::Strict, 0, 0, capacity);
        CheckModel(reopened, model, "Four-layer reopen");
    }
}

class Pipe {
public:
    Pipe() { Check(::pipe(fds_) == 0, "Create acknowledgement pipe"); }
    ~Pipe() { CloseRead(); CloseWrite(); }
    void CloseRead() { if (fds_[0] != -1) { ::close(fds_[0]); fds_[0] = -1; } }
    void CloseWrite() { if (fds_[1] != -1) { ::close(fds_[1]); fds_[1] = -1; } }
    void Send(unsigned char event) const {
        ssize_t written;
        do { written = ::write(fds_[1], &event, 1); } while (written == -1 && errno == EINTR);
        Check(written == 1, "Send acknowledged operation");
    }
    std::optional<unsigned char> Receive() const {
        pollfd descriptor{fds_[0], POLLIN, 0};
        int ready;
        do { ready = ::poll(&descriptor, 1, 10000); } while (ready == -1 && errno == EINTR);
        Check(ready > 0, "Acknowledgement pipe deadline");
        unsigned char event = 0;
        ssize_t count;
        do { count = ::read(fds_[0], &event, 1); } while (count == -1 && errno == EINTR);
        Check(count >= 0, "Read acknowledged operation");
        return count == 0 ? std::nullopt : std::optional<unsigned char>(event);
    }
private:
    int fds_[2]{-1, -1};
};

class Child {
public:
    explicit Child(pid_t pid) : pid_(pid) {}
    ~Child() {
        if (pid_ > 0) {
            ::kill(pid_, SIGKILL);
            while (::waitpid(pid_, nullptr, 0) == -1 && errno == EINTR) {}
        }
    }
    void WaitForKill() {
        int status = 0;
        pid_t result;
        do { result = ::waitpid(pid_, &status, 0); } while (result == -1 && errno == EINTR);
        Check(result == pid_, "Reap integration child");
        pid_ = -1;
        Check(WIFSIGNALED(status) && WTERMSIG(status) == SIGKILL, "Child dies at selected crash boundary");
    }
private:
    pid_t pid_;
};

constexpr unsigned char kBoundaryReached = 255;

[[noreturn]] void Crash(const Pipe& pipe) {
    pipe.Send(kBoundaryReached);
    ::kill(::getpid(), SIGKILL);
    for (;;) { ::pause(); }
}

std::vector<Mutation> CrashBatch(int phase) {
    std::vector<Mutation> batch;
    for (std::size_t i = 0; i < 16; ++i) {
        const auto& key = Keys()[(i + static_cast<std::size_t>(phase) * 8) % Keys().size()];
        if ((i + static_cast<std::size_t>(phase)) % 4 == 0) { batch.push_back({key, std::nullopt}); }
        else if (i % 7 == 0) { batch.push_back({key, ""}); }
        else { batch.push_back({key, std::string(600, static_cast<char>(phase * 64 + i))}); }
    }
    return batch;
}

enum class CrashKind { Freeze, Flush, Compaction };

void RunCrashCase(CrashKind kind, std::size_t stage_index, std::size_t capacity) {
    const std::vector<FreezeStage> freeze_stages{
        FreezeStage::WalSynced, FreezeStage::ManifestTempSynced, FreezeStage::ManifestRenamed,
        FreezeStage::ManifestDurable, FreezeStage::MemTableFrozen};
    const std::vector<FlushStage> flush_stages{
        FlushStage::SSTableWritten, FlushStage::SSTableSynced, FlushStage::ManifestTempSynced,
        FlushStage::ManifestRenamed, FlushStage::ManifestDurable, FlushStage::FrozenCleared,
        FlushStage::OldWalRemoved};
    const std::vector<CompactStage> compact_stages{
        CompactStage::SSTableWritten, CompactStage::SSTableSynced, CompactStage::ManifestTempSynced,
        CompactStage::ManifestRenamed, CompactStage::ManifestDurable, CompactStage::ReadersSwitched,
        CompactStage::OldTableRemoved};
    TestDirectory directory;
    const auto base = directory.Base();
    auto expected = SeedHistory(base, capacity);
    const auto first = CrashBatch(0), second = CrashBatch(1);
    auto operations = first;
    if (kind != CrashKind::Freeze) { operations.insert(operations.end(), second.begin(), second.end()); }
    Pipe pipe;
    // Every Store in the parent has been destroyed before fork; the child starts fresh workers.
    const auto pid = ::fork();
    Check(pid != -1, "Fork integrated crash case");
    if (pid == 0) {
        pipe.CloseRead();
        ::alarm(20);
        try {
            kvstore::Store store(base, kvstore::WalRecoveryMode::Strict, 0, 0, capacity);
            Gate gate;
            CheckModel(store, expected, "Child warms disk blocks");
            if (capacity != 0) {
                Check(store.block_cache_stats().entries != 0, "Crash starts with populated disk-block cache");
            }
            unsigned char sequence = 0;
            const auto apply = [&](const std::vector<Mutation>& batch) {
                for (const auto& mutation : batch) {
                    ApplyBoth(store, expected, mutation);
                    // Notify the parent only AFTER Put/Delete has returned successfully.
                    pipe.Send(sequence++);
                }
            };
            if (kind == CrashKind::Freeze) {
                apply(first);
                kvstore::StoreTestAccess::FreezeHook(store, [&](FreezeStage stage) {
                    if (stage == freeze_stages[stage_index]) { Crash(pipe); }
                });
                store.RequestFlush();
            } else if (kind == CrashKind::Flush) {
                apply(first);
                kvstore::StoreTestAccess::FlushHook(store, [&, pause = gate.Pause()](FlushStage stage) {
                    if (stage == FlushStage::SSTableWritten) { pause(); }
                    if (stage == flush_stages[stage_index]) { Crash(pipe); }
                });
                store.RequestFlush(); gate.Wait();
                apply(second);  // New active WAL writes coexist with the frozen WAL and SSTables.
                CheckModel(store, expected, "Child checks active and frozen layers");
                gate.Release();
            } else {
                kvstore::StoreTestAccess::CompactHook(store, [&, pause = gate.Pause()](CompactStage stage) {
                    if (stage == CompactStage::SSTableWritten) { pause(); }
                    if (stage == compact_stages[stage_index]) { Crash(pipe); }
                });
                store.RequestCompaction(); gate.Wait();
                apply(first); store.Flush();  // Append a newer SSTable outside the merge inputs.
                apply(second);              // Leave an even newer state in the active WAL.
                CheckModel(store, expected, "Child checks newer disk and active WAL");
                gate.Release();
            }
            store.WaitForBackgroundWork();
            ::_exit(4);  // The chosen hook must terminate the process before this point.
        } catch (const std::exception& error) {
            std::cerr << "Integration child failed: " << error.what() << '\n';
            ::_exit(2);
        }
    }
    Child child(pid);
    pipe.CloseWrite();
    std::size_t acknowledged = 0;
    bool reached = false;
    while (const auto event = pipe.Receive()) {
        Check(!reached, "No writes occur after the selected boundary");
        if (*event == kBoundaryReached) { reached = true; }
        else {
            Check(*event == acknowledged && acknowledged < operations.size(), "Ordered acknowledgements");
            ApplyModel(expected, operations[acknowledged++]);
        }
    }
    child.WaitForKill();
    Check(reached && acknowledged == operations.size(), "Parent observed all successful mutations and crash boundary");
    {
        kvstore::Store recovered(base, kvstore::WalRecoveryMode::Strict, 0, 0, capacity);
        Check(recovered.block_cache_stats().entries == 0, "Crash recovery starts with a cold cache");
        CheckModel(recovered, expected, "Immediately after process crash");
        recovered.WaitForBackgroundWork();
        CheckModel(recovered, expected, "After recovered frozen WAL drains");
        CheckLiveFiles(base);
        ApplyBoth(recovered, expected, {Keys()[0], "write-after-recovery"});
        ApplyBoth(recovered, expected, {Keys()[31], std::nullopt});
        recovered.Flush(); recovered.Compact();
        CheckModel(recovered, expected, "Recovered engine continues writing and merging");
        CheckLiveFiles(base);
    }
    // Swap cache configuration to ensure persistence never depends on a surviving cache.
    kvstore::Store reopened(base, kvstore::WalRecoveryMode::Strict, 0, 0, capacity == 0 ? 8192 : 0);
    CheckModel(reopened, expected, "Second reopen with opposite cache setting");
    CheckLiveFiles(base);
}

void TestKilledFreezeWithAcknowledgedWrites() {
    for (const std::size_t capacity : {std::size_t{0}, std::size_t{8192}}) {
        for (std::size_t stage = 0; stage < 5; ++stage) { RunCrashCase(CrashKind::Freeze, stage, capacity); }
    }
}

void TestKilledFlushWithAcknowledgedActiveWrites() {
    for (const std::size_t capacity : {std::size_t{0}, std::size_t{8192}}) {
        for (std::size_t stage = 0; stage < 7; ++stage) { RunCrashCase(CrashKind::Flush, stage, capacity); }
    }
}

void TestKilledCompactionWithNewerDiskAndWal() {
    for (const std::size_t capacity : {std::size_t{0}, std::size_t{8192}}) {
        for (std::size_t stage = 0; stage < 7; ++stage) { RunCrashCase(CrashKind::Compaction, stage, capacity); }
    }
}
}  // namespace

int main(int argc, char* argv[]) {
    const bool in_process = argc == 2 && std::string(argv[1]) == "--in-process";
    if (argc != 1 && !in_process) {
        std::cerr << "Usage: engine_integration_test [--in-process]\n";
        return 2;
    }
    try {
        TestMixedModelAcrossReopens();
        TestFourLayerPrecedenceAndPublication();
        if (in_process) {
            std::cout << "All 2 in-process engine integration tests passed.\n";
        } else {
            TestKilledFreezeWithAcknowledgedWrites();
            TestKilledFlushWithAcknowledgedActiveWrites();
            TestKilledCompactionWithNewerDiskAndWal();
            std::cout << "All 5 engine integration tests passed (38 process-crash scenarios).\n";
        }
    } catch (const std::exception& error) {
        std::cerr << "Engine integration test failed: " << error.what() << '\n';
        return 1;
    }
    return 0;
}
