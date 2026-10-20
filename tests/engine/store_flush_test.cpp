#include "kvstore/store.h"
#include "kvstore/wal_record.h"

#include <cerrno>
#include <csignal>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <functional>
#include <future>
#include <iostream>
#include <iterator>
#include <stdexcept>
#include <system_error>

#include <sys/resource.h>
#include <sys/wait.h>
#include <unistd.h>

namespace kvstore {
// Only this test executable can install callbacks at private protocol boundaries.
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
const std::vector<Stage> stages = {
    Stage::SSTableWritten, Stage::SSTableSynced, Stage::ManifestTempSynced,
    Stage::ManifestRenamed, Stage::ManifestDurable, Stage::FrozenCleared, Stage::OldWalRemoved,
};

void Check(bool condition, const std::string& message) {
    if (!condition) { throw std::runtime_error(message); }
}
void Reject(const std::function<void()>& action, const std::string& message) {
    bool failed = false;
    try { action(); } catch (const std::exception&) { failed = true; }
    Check(failed, message);
}
class TestDirectory {
public:
    TestDirectory() {
        char pattern[] = "/tmp/kvstore-flush-XXXXXX";
        const char* path = ::mkdtemp(pattern);
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
class Child {
public:
    explicit Child(pid_t pid) : pid_(pid) { Check(pid > 0, "Create child process"); }
    ~Child() {
        if (pid_ > 0) { ::kill(pid_, SIGKILL); while (::waitpid(pid_, nullptr, 0) == -1 && errno == EINTR) {} }
    }
    int Wait() {
        int status = 0;
        pid_t result;
        do { result = ::waitpid(pid_, &status, 0); } while (result == -1 && errno == EINTR);
        Check(result == pid_, "Wait for child");
        pid_ = -1;
        return status;
    }
private:
    pid_t pid_;
};
std::string Read(const std::string& path) {
    std::ifstream input(path, std::ios::binary);
    Check(input.is_open(), "Read test file");
    return {std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
}
void Write(const std::string& path, const std::string& bytes, bool append = false) {
    std::ofstream output(path, std::ios::binary | (append ? std::ios::app : std::ios::trunc));
    output.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    output.close();
    Check(!output.fail(), "Write test file");
}
kvstore::Manifest Metadata(const std::string& base) { return kvstore::DecodeManifest(Read(base + ".manifest")); }
std::string Wal(const std::string& base, const kvstore::Manifest& manifest) {
    return manifest.wal_id == 0 ? base : base + ".wal." + std::to_string(manifest.wal_id);
}
std::string Table(const std::string& base, std::uint64_t id) { return base + ".sst." + std::to_string(id); }

void TestManualFlushAndReopen() {
    TestDirectory directory;
    const auto base = directory.Base();
    {
        kvstore::Store store(base);
        store.Put("name", "Alice");
        store.Flush();
        Check(store.sstable_count() == 1 && store.memtable_bytes() == 0, "Flush must publish and clear pending memory");
        Check(store.Get("name") == "Alice", "Read must now find the SSTable value");
        const auto manifest = Metadata(base);
        Check(manifest.wal_id == 2 && manifest.sstable_ids == std::vector<std::uint64_t>{1}, "First file IDs");
        Check(!std::filesystem::exists(base), "Original WAL must be retired only after publication");
        Check(std::filesystem::file_size(Wal(base, manifest)) == 0, "New WAL starts empty");
        store.Put("next", "pending");
    }
    kvstore::Store reopened(base);
    Check(reopened.Get("name") == "Alice" && reopened.Get("next") == "pending", "Combine SSTable and replayed WAL");
    reopened.Flush();
    Check(reopened.sstable_count() == 2, "Second flush must add a newer SSTable");
}

void TestNewestValueAndDeletion() {
    TestDirectory directory;
    const auto base = directory.Base();
    {
        kvstore::Store store(base);
        store.Put("name", "Alice"); store.Put("city", "Vancouver"); store.Flush();
        store.Put("name", "Bob");
        Check(store.Get("name") == "Bob", "MemTable must override older files");
        Check(store.Delete("city"), "Delete must see a value stored only on disk");
        Check(!store.Get("city"), "Memory tombstone must hide older disk value");
        store.Flush();
        Check(store.Get("name") == "Bob" && !store.Get("city"), "Newest SSTable wins and tombstone stops lookup");
        store.Put("name", "Charlie");
    }
    kvstore::Store store(base);
    Check(store.Get("name") == "Charlie" && !store.Get("city"), "Replayed WAL is newer than every SSTable");
    store.Flush();
    Check(store.sstable_count() == 3 && store.Get("name") == "Charlie", "Multiple immutable files preserve order");
}

void TestDeleteAndReinsertAcrossFlushes() {
    TestDirectory directory;
    const auto base = directory.Base();
    {
        kvstore::Store store(base);
        store.Put("key", "old"); store.Flush();
        Check(store.Delete("key"), "Delete disk value must report true");
        Check(!store.Delete("key") && !store.Delete("absent"), "Repeated or absent deletion reports false");
        store.Flush();
        store.Put("key", "new"); store.Flush();
    }
    kvstore::Store store(base);
    Check(store.Get("key") == "new" && !store.Get("absent"), "Reinsertion must override older tombstones");
}

void TestEmptyAndBinaryAcrossFlushes() {
    TestDirectory directory;
    const std::string key("k\0\xff", 3), value("v\0\x80", 3);
    {
        kvstore::Store store(directory.Base());
        store.Put("", ""); store.Put(key, value); store.Flush();
    }
    kvstore::Store store(directory.Base());
    Check(store.Get("").has_value() && store.Get("")->empty() && store.Get(key) == value, "Binary values survive flush/reopen");
    Check(store.Delete(key), "Binary key can be deleted from disk");
    store.Flush();
    Check(!store.Get(key) && store.Get("").has_value(), "Tombstone must not affect another empty value");
}

void TestEmptyFlushIsNoOp() {
    TestDirectory directory;
    kvstore::Store store(directory.Base());
    const auto before = Read(directory.Base() + ".manifest");
    store.Flush();
    Check(Read(directory.Base() + ".manifest") == before && store.sstable_count() == 0, "Empty flush must allocate no IDs");
    store.Delete("missing"); store.Flush();
    Check(store.sstable_count() == 1, "Tombstone-only MemTable is not empty");
    const auto after = Read(directory.Base() + ".manifest");
    store.Flush();
    Check(Read(directory.Base() + ".manifest") == after, "Repeated empty flush remains a no-op");
}

void TestThresholdAndByteAccounting() {
    TestDirectory directory;
    kvstore::Store store(directory.Base(), kvstore::WalRecoveryMode::Strict, 40);
    store.Put("a", "x");
    Check(store.memtable_bytes() == 19, "Account for one encoded entry");
    store.Put("a", "yy");
    Check(store.memtable_bytes() == 20, "Overwrite replaces previous bytes rather than accumulating history");
    store.Delete("a"); store.Delete("b");
    Check(store.memtable_bytes() == 36 && store.sstable_count() == 0, "Tombstones contribute key and record overhead");
    store.Put("c", "");
    store.WaitForBackgroundWork();
    Check(store.memtable_bytes() == 0 && store.sstable_count() == 1, "Reaching threshold queues a background flush");
    Check(!store.Get("a") && !store.Get("b") && store.Get("c") == "", "Automatic flush preserves all states");
}

void TestAutomaticDefaultAndDisabledThreshold() {
    for (bool automatic : {false, true}) {
        TestDirectory directory;
        kvstore::Store store(directory.Base(), kvstore::WalRecoveryMode::Strict, automatic ? 4 * 1024 * 1024 : 0);
        const std::string value(kvstore::kMaxWalPayloadBytes - 1, 'v');
        for (const auto& key : {"a", "b", "c", "d"}) { store.Put(key, value); }
        store.WaitForBackgroundWork();
        Check(store.sstable_count() == (automatic ? 1U : 0U), "Default threshold or manual-only option");
        if (!automatic) { store.Flush(); }
        Check(store.Get("d") == value, "Large automatic or manual flush reads back correctly");
    }
}

void TestOrphanCleanup() {
    TestDirectory directory;
    const auto base = directory.Base();
    { kvstore::Store store(base); store.Put("name", "Alice"); store.Flush(); }
    Write(base + ".sst.999", "broken unpublished table");
    Write(base + ".wal.998", kvstore::EncodeWalRecord({kvstore::WalRecordType::Put, "name", "wrong"}));
    Write(base + ".manifest.tmp", "unpublished metadata");
    Write(base + ".notes", "unrelated");
    Write(base + ".sst.0009", "not a canonical owned file");
    kvstore::Store store(base);
    Check(store.Get("name") == "Alice", "Unlisted files must never become live data");
    Check(!std::filesystem::exists(base + ".sst.999") && !std::filesystem::exists(base + ".wal.998") &&
          !std::filesystem::exists(base + ".manifest.tmp"), "Cleanup must remove orphan files");
    Check(Read(base + ".notes") == "unrelated" && std::filesystem::exists(base + ".sst.0009"), "Do not remove unrelated files");
}

void TestMissingLiveFilesNeverRecreated() {
    for (const auto& missing : {"wal", "sst", "manifest"}) {
        TestDirectory directory;
        const auto base = directory.Base();
        { kvstore::Store store(base); store.Put("name", "Alice"); store.Flush(); }
        const auto manifest = Metadata(base);
        const auto path = std::string(missing) == "wal" ? Wal(base, manifest) :
                          std::string(missing) == "sst" ? Table(base, manifest.sstable_ids[0]) : base + ".manifest";
        std::filesystem::remove(path);
        Reject([&] { kvstore::Store store(base); }, "Missing authoritative file must fail opening");
        Check(!std::filesystem::exists(path), "Do not silently replace missing data or metadata with an empty file");
    }
}

void TestCorruptLiveFilesPreserveEvidence() {
    for (const auto& target : {"wal", "sst", "manifest"}) {
        TestDirectory directory;
        const auto base = directory.Base();
        { kvstore::Store store(base); store.Put("name", "Alice"); store.Flush(); store.Put("pending", "value"); }
        const auto manifest = Metadata(base);
        const auto path = std::string(target) == "wal" ? Wal(base, manifest) :
                          std::string(target) == "sst" ? Table(base, manifest.sstable_ids[0]) : base + ".manifest";
        auto damaged = Read(path); damaged.back() ^= 1; Write(path, damaged);
        Write(base + ".sst.999", "keep evidence until live files validate");
        Reject([&] { kvstore::Store store(base); }, "Corrupt authoritative file must fail opening");
        Reject([&] { kvstore::Store store(base, kvstore::WalRecoveryMode::TruncateIncompleteTail); }, "Tail mode must not hide corruption");
        Check(Read(path) == damaged && std::filesystem::exists(base + ".sst.999"), "Failed recovery must not clean evidence");
    }
}

void TestTailRecoveryOnRotatedWal() {
    TestDirectory directory;
    const auto base = directory.Base();
    { kvstore::Store store(base); store.Put("disk", "value"); store.Flush(); store.Put("memory", "value"); }
    const auto path = Wal(base, Metadata(base));
    const auto partial = kvstore::EncodeWalRecord({kvstore::WalRecordType::Delete, "disk", ""});
    Write(path, partial.substr(0, partial.size() - 1), true);
    const auto before = Read(path);
    Reject([&] { kvstore::Store store(base); }, "Strict mode rejects rotated WAL tail");
    Check(Read(path) == before, "Strict mode preserves rotated log bytes");
    kvstore::Store store(base, kvstore::WalRecoveryMode::TruncateIncompleteTail);
    Check(store.truncated_tail_bytes() == partial.size() - 1 && store.Get("disk") == "value" && store.Get("memory") == "value",
          "Tail repair must preserve disk state and valid active WAL prefix");
    store.Flush();
    Check(!std::filesystem::exists(path), "Repaired WAL can later retire safely");
}

void TestStableLockAcrossRotation() {
    TestDirectory directory;
    const auto base = directory.Base();
    {
        kvstore::Store first(base); first.Put("key", "value"); first.Flush();
        Reject([&] { kvstore::Store second(base); }, "Rotation must not release the Store-wide lock");
        first.Put("next", "value"); first.Flush();
        Reject([&] { kvstore::Store second(base); }, "Lock must still protect later generations");
        Check(std::filesystem::exists(base + ".lock"), "Stable lock inode must remain in place");
    }
    kvstore::Store reopened(base);
    Check(reopened.Get("key") == "value" && reopened.Get("next") == "value", "Destruction releases stable lock");
}

void TestLegacyWalBootstrap() {
    TestDirectory directory;
    const auto base = directory.Base();
    Write(base, kvstore::EncodeWalRecord({kvstore::WalRecordType::Put, "name", "Alice"}));
    kvstore::Store store(base);
    Check(store.Get("name") == "Alice" && Metadata(base).wal_id == 0, "Old WAL-only Store must bootstrap without losing data");
    store.Flush();
    Check(store.Get("name") == "Alice" && !std::filesystem::exists(base), "Bootstrapped store can safely retire old WAL");
}

void TestRelativePathSurvivesDirectoryChange() {
    TestDirectory directory;
    const auto relative = std::filesystem::relative(directory.Base(), std::filesystem::current_path()).string();
    const auto pid = ::fork();
    Check(pid != -1, "Fork working-directory test");
    if (pid == 0) {
        try {
            kvstore::Store store(relative);
            Check(::chdir("/") == 0, "Change only the child working directory");
            store.Put("key", "value");
            store.Flush();
            Check(store.Get("key") == "value", "Flush must use the originally opened namespace");
            ::_exit(0);
        } catch (...) { ::_exit(2); }
    }
    Child child(pid);
    const auto status = child.Wait();
    Check(WIFEXITED(status) && WEXITSTATUS(status) == 0, "Relative Store path must stay anchored");
    kvstore::Store reopened(directory.Base());
    Check(reopened.Get("key") == "value", "Reopen at the original path must find flushed data");
}

void CheckRecovered(kvstore::Store& store) {
    Check(store.Get("name") == "Bob" && !store.Get("removed") && store.Get("empty") == "",
          "All acknowledged updates, deletion and empty values must survive interrupted flush");
}
void Prepare(kvstore::Store& store) {
    store.Put("name", "Bob"); store.Delete("removed"); store.Put("empty", "");
}

void TestKilledFlushAtEveryStage() {
    for (bool already_flushed : {false, true}) {
        for (std::size_t index = 0; index < stages.size(); ++index) {
            TestDirectory directory;
            const auto base = directory.Base();
            {
                kvstore::Store store(base); store.Put("name", "Alice"); store.Put("removed", "old");
                if (already_flushed) { store.Flush(); }
            }
            const auto old_wal = Wal(base, Metadata(base));
            const auto pid = ::fork();
            Check(pid != -1, "Fork interrupted-flush test");
            if (pid == 0) {
                try {
                    kvstore::Store store(base); Prepare(store);
                    kvstore::StoreTestAccess::Hook(store, [index](Stage stage) {
                        if (stage == stages[index]) {
                            ::kill(::getpid(), SIGKILL);
                            for (;;) { ::pause(); }
                        }
                    });
                    store.Flush();
                } catch (...) { ::_exit(2); }
                ::_exit(4);
            }
            Child child(pid);
            const int status = child.Wait();
            Check(WIFSIGNALED(status) && WTERMSIG(status) == SIGKILL, "Child must reach the chosen stage and be killed");
            const std::size_t expected_count = (already_flushed ? 1 : 0) + (index >= 3 ? 1 : 0);
            Check(Metadata(base).sstable_ids.size() == expected_count, "Visible manifest selects old or new complete file set");
            Check(std::filesystem::exists(old_wal) == (index < 6),
                  "Old WAL must remain through durable publication and disappear only at retirement");
            {
                kvstore::Store store(base); CheckRecovered(store); store.Flush();
                store.Put("next", "ok"); store.Flush();
            }
            kvstore::Store reopened(base); CheckRecovered(reopened);
            Check(reopened.Get("next") == "ok", "Recovery must allow safe append and flush after cleanup");
        }
    }
}

void TestExceptionsStopStoreAndRecover() {
    for (const auto target : stages) {
        TestDirectory directory;
        const auto base = directory.Base();
        {
            kvstore::Store store(base); store.Put("removed", "old"); Prepare(store);
            kvstore::StoreTestAccess::Hook(store, [target](Stage stage) {
                if (stage == target) { throw std::runtime_error("Injected flush boundary failure"); }
            });
            Reject([&] { store.Flush(); }, "Boundary failure must escape Flush");
            Reject([&] { store.Get("name"); }, "Reads must stop after uncertain flush");
            Reject([&] { store.Put("later", "value"); }, "Writes must stop after uncertain flush");
            Reject([&] { store.Flush(); }, "Further flush attempts must stop");
        }
        kvstore::Store recovered(base); CheckRecovered(recovered); recovered.Flush();
    }
}

void TestRealPartialFlushWrites() {
    for (const bool during_freeze : {true, false}) {
        for (const auto limit_bytes : {4, 35}) {
            TestDirectory directory;
            const auto base = directory.Base();
            { kvstore::Store store(base); store.Put("k", "v"); }
            const auto metadata_before = Read(base + ".manifest");
            const auto wal_before = Read(base);
            const auto pid = ::fork();
            Check(pid != -1, "Fork failed-flush test");
            if (pid == 0) {
                try {
                    kvstore::Store store(base);
                    Check(::signal(SIGXFSZ, SIG_IGN) != SIG_ERR, "Ignore file-size signal");
                    const auto install_limit = [limit_bytes] {
                        struct rlimit limit {};
                        Check(::getrlimit(RLIMIT_FSIZE, &limit) == 0, "Read resource limit");
                        limit.rlim_cur = limit_bytes;
                        Check(::setrlimit(RLIMIT_FSIZE, &limit) == 0, "Set child-only resource limit");
                    };
                    if (during_freeze) { install_limit(); }
                    else {
                        kvstore::StoreTestAccess::FreezeHook(store, [install_limit](auto stage) {
                            if (stage == kvstore::StoreTestAccess::FreezeStage::MemTableFrozen) { install_limit(); }
                        });
                    }
                    bool failed = false;
                    try { store.Flush(); } catch (const std::system_error&) { failed = true; }
                    Check(failed, "OS must report a real partial write");
                    Reject([&] { store.Get("k"); }, "Failed flush must disable Store");
                    ::_exit(0);
                } catch (...) { ::_exit(2); }
            }
            Child child(pid);
            const auto status = child.Wait();
            Check(WIFEXITED(status) && WEXITSTATUS(status) == 0, "Child must verify partial-write error");
            const auto partial_path = !during_freeze && limit_bytes == 4 ? Table(base, 1) : base + ".manifest.tmp";
            Check(std::filesystem::file_size(partial_path) == static_cast<std::size_t>(limit_bytes), "Real partial file must exist");
            const auto expected_metadata = during_freeze ? metadata_before : kvstore::EncodeManifest({2, 3, {}, 0});
            Check(Read(base + ".manifest") == expected_metadata && Read(base) == wal_before,
                  "Failure preserves either the old WAL or the published two-WAL recovery set");
            kvstore::Store recovered(base);
            Check(recovered.Get("k") == "v", "Recovery WAL preserves acknowledged data");
            recovered.Flush();
            Check(recovered.Get("k") == "v", "Retry after orphan cleanup must work");
        }
    }
}

void TestKilledFreezeAtEveryStage() {
    using FreezeStage = kvstore::StoreTestAccess::FreezeStage;
    const std::vector<FreezeStage> freeze_stages = {
        FreezeStage::WalSynced, FreezeStage::ManifestTempSynced, FreezeStage::ManifestRenamed,
        FreezeStage::ManifestDurable, FreezeStage::MemTableFrozen,
    };
    for (bool already_flushed : {false, true}) {
        for (std::size_t index = 0; index < freeze_stages.size(); ++index) {
            TestDirectory directory;
            const auto base = directory.Base();
            {
                kvstore::Store store(base); store.Put("name", "Alice"); store.Put("removed", "old");
                if (already_flushed) { store.Flush(); }
            }
            const auto before = Metadata(base);
            const auto old_wal = Wal(base, before);
            const auto pid = ::fork();
            Check(pid != -1, "Fork interrupted freeze");
            if (pid == 0) {
                try {
                    kvstore::Store store(base); Prepare(store);
                    kvstore::StoreTestAccess::FreezeHook(store, [&, index](FreezeStage stage) {
                        if (stage == freeze_stages[index]) {
                            ::kill(::getpid(), SIGKILL);
                            for (;;) { ::pause(); }
                        }
                    });
                    store.RequestFlush();
                    ::_exit(4);
                } catch (...) { ::_exit(2); }
            }
            Child child(pid);
            const auto status = child.Wait();
            Check(WIFSIGNALED(status) && WTERMSIG(status) == SIGKILL, "Child reaches freeze boundary");
            const auto after = Metadata(base);
            Check(after.sstable_ids == before.sstable_ids, "Freeze does not publish an SSTable");
            Check(after.frozen_wal_id == (index >= 2 ? std::optional<std::uint64_t>{before.wal_id} : std::nullopt),
                  "Manifest chooses one-WAL or two-WAL recovery");
            Check(std::filesystem::exists(old_wal), "Old WAL remains throughout freeze");
            kvstore::Store reopened(base); CheckRecovered(reopened); reopened.WaitForBackgroundWork();
            reopened.Put("next", "ok"); reopened.Flush();
            Check(reopened.Get("next") == "ok", "Recovery can continue after interrupted freeze");
        }
    }
}

void TestFrozenWalValidation() {
    for (const auto& damage : {"missing", "checksum", "tail", "empty"}) {
        TestDirectory directory;
        const auto base = directory.Base();
        auto frozen = kvstore::EncodeWalRecord({kvstore::WalRecordType::Put, "k", "old"});
        if (std::string(damage) == "checksum") { frozen.back() ^= 1; }
        if (std::string(damage) == "tail") { frozen += "KVW"; }
        if (std::string(damage) == "empty") { frozen.clear(); }
        if (std::string(damage) != "missing") { Write(base, frozen); }
        Write(base + ".wal.2", kvstore::EncodeWalRecord({kvstore::WalRecordType::Put, "k", "new"}));
        Write(base + ".manifest", kvstore::EncodeManifest({2, 3, {}, 0}));
        Write(base + ".sst.999", "evidence");
        for (const auto mode : {kvstore::WalRecoveryMode::Strict, kvstore::WalRecoveryMode::TruncateIncompleteTail}) {
            Reject([&] { kvstore::Store store(base, mode); }, "Frozen WAL must exist and be fully valid in every recovery mode");
        }
        Check(Read(base + ".sst.999") == "evidence", "Validate live WALs before orphan cleanup");
        if (std::string(damage) != "missing") { Check(Read(base) == frozen, "Never repair frozen WAL"); }
        else { Check(!std::filesystem::exists(base), "Never recreate missing frozen WAL"); }
    }
}

void TestTwoWalRecoveryAndActiveTail() {
    TestDirectory directory;
    const auto base = directory.Base();
    kvstore::WriteSSTable(Table(base, 1), {{kvstore::SSTableEntryType::Value, "gone", "old"}});
    const auto frozen = kvstore::EncodeWalRecord({kvstore::WalRecordType::Put, "k", "older"}) +
                        kvstore::EncodeWalRecord({kvstore::WalRecordType::Put, "frozen-only", "saved"}) +
                        kvstore::EncodeWalRecord({kvstore::WalRecordType::Delete, "gone", ""});
    const auto active = kvstore::EncodeWalRecord({kvstore::WalRecordType::Put, "k", "newer"});
    Write(base + ".wal.2", frozen);
    Write(base + ".wal.4", active + "KVW");
    Write(base + ".manifest", kvstore::EncodeManifest({4, 5, {1}, 2}));
    Reject([&] { kvstore::Store store(base); }, "Strict recovery rejects active tail with frozen WAL present");
    Check(Read(base + ".wal.2") == frozen && Read(base + ".wal.4") == active + "KVW", "Strict failure preserves both WALs");
    {
        kvstore::Store store(base, kvstore::WalRecoveryMode::TruncateIncompleteTail);
        Check(store.truncated_tail_bytes() == 3 && store.Get("k") == "newer" && !store.Get("gone") &&
              store.Get("frozen-only") == "saved", "Replay order is active, frozen, then older SSTables for reads");
        store.WaitForBackgroundWork();
        Check(!Metadata(base).frozen_wal_id && !std::filesystem::exists(base + ".wal.2"), "Recovered frozen table finishes flushing");
    }
    kvstore::Store reopened(base);
    Check(reopened.Get("k") == "newer" && !reopened.Get("gone") && reopened.Get("frozen-only") == "saved",
          "Both recovered WAL generations survive a second reopen");
}

void TestKilledFlushWithActiveWrites() {
    for (std::size_t index = 0; index < stages.size(); ++index) {
        TestDirectory directory;
        const auto base = directory.Base();
        { kvstore::Store store(base); store.Put("name", "Alice"); store.Put("removed", "old"); store.Flush(); }
        const auto before = Metadata(base);
        const auto pid = ::fork();
        Check(pid != -1, "Fork flush with newer active writes");
        if (pid == 0) {
            try {
                kvstore::Store store(base, kvstore::WalRecoveryMode::Strict, 0, 0);
                auto ready = std::make_shared<std::promise<void>>();
                auto entered = ready->get_future();
                auto release = std::make_shared<std::promise<void>>();
                auto released = release->get_future().share();
                struct ReleaseOnExit {
                    std::shared_ptr<std::promise<void>> value;
                    ~ReleaseOnExit() { if (value) { value->set_value(); } }
                } guard{release};
                Prepare(store);
                kvstore::StoreTestAccess::Hook(store, [ready, released, index](Stage stage) {
                    if (stage == Stage::SSTableWritten) { ready->set_value(); released.wait(); }
                    if (stage == stages[index]) {
                        ::kill(::getpid(), SIGKILL);
                        for (;;) { ::pause(); }
                    }
                });
                store.RequestFlush();
                Check(entered.wait_for(std::chrono::seconds(5)) == std::future_status::ready, "Pause flush output");
                store.Put("name", "Charlie"); store.Delete("empty"); store.Put("active-only", "saved");
                release->set_value(); guard.value.reset();
                store.WaitForBackgroundWork();
                ::_exit(4);
            } catch (...) { ::_exit(2); }
        }
        Child child(pid);
        const auto status = child.Wait();
        Check(WIFSIGNALED(status) && WTERMSIG(status) == SIGKILL, "Child reaches flush boundary after newer writes");
        Check(Metadata(base).wal_id == before.next_file_id + 1, "Flush publication preserves the newer active WAL");
        kvstore::Store reopened(base);
        Check(reopened.Get("name") == "Charlie" && !reopened.Get("empty") && !reopened.Get("removed") &&
              reopened.Get("active-only") == "saved", "New active writes and tombstones survive every frozen-flush boundary");
        reopened.Flush();
    }
}

int main() {
    try {
        TestManualFlushAndReopen(); TestNewestValueAndDeletion(); TestDeleteAndReinsertAcrossFlushes();
        TestEmptyAndBinaryAcrossFlushes(); TestEmptyFlushIsNoOp(); TestThresholdAndByteAccounting();
        TestAutomaticDefaultAndDisabledThreshold(); TestOrphanCleanup(); TestMissingLiveFilesNeverRecreated();
        TestCorruptLiveFilesPreserveEvidence(); TestTailRecoveryOnRotatedWal(); TestStableLockAcrossRotation();
        TestLegacyWalBootstrap(); TestRelativePathSurvivesDirectoryChange(); TestKilledFlushAtEveryStage();
        TestExceptionsStopStoreAndRecover(); TestRealPartialFlushWrites();
        TestKilledFreezeAtEveryStage(); TestFrozenWalValidation(); TestTwoWalRecoveryAndActiveTail();
        TestKilledFlushWithActiveWrites();
        std::cout << "All 21 Store flush tests passed.\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "Store flush test failed: " << error.what() << '\n';
        return 1;
    }
}
