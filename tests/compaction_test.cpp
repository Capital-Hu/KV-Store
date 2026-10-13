#include "kvstore/compaction.h"
#include "kvstore/store.h"
#include "kvstore/wal_record.h"

#include <cerrno>
#include <csignal>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <iterator>
#include <map>
#include <random>
#include <stdexcept>
#include <system_error>
#include <utility>

#include <sys/resource.h>
#include <sys/wait.h>
#include <unistd.h>

namespace kvstore {
struct StoreTestAccess {
    using Stage = Store::CompactStage;
    static void Hook(Store& store, std::function<void(Stage)> callback) {
        store.compact_hook_ = std::move(callback);
    }
};
}  // namespace kvstore

using Stage = kvstore::StoreTestAccess::Stage;
using Type = kvstore::SSTableEntryType;
const std::vector<Stage> stages = {
    Stage::SSTableWritten, Stage::SSTableSynced, Stage::ManifestTempSynced,
    Stage::ManifestRenamed, Stage::ManifestDurable, Stage::ReadersSwitched,
    Stage::OldTableRemoved, Stage::OldTableRemoved,
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

void TestMergeLatestByInputOrder() {
    TestDirectory directory;
    const auto a = directory.Base() + ".old", b = directory.Base() + ".new";
    kvstore::WriteSSTable(a, {{Type::Value, "a", "old"}, {Type::Value, "c", "keep"}, {Type::Value, "z", "old"}});
    kvstore::WriteSSTable(b, {{Type::Value, "a", "new"}, {Type::Value, "b", "added"}, {Type::Value, "z", "new"}});
    const auto output = directory.Base() + ".out";
    Check(kvstore::MergeSSTables({a, b}, output) == 4, "Merge must deduplicate overlapping keys");
    kvstore::SSTableReader reader(output);
    Check(reader.Next()->key == "a" && reader.Next()->key == "b" && reader.Next()->key == "c" && reader.Next()->key == "z",
          "Merged output must remain sorted");
    Check(reader.Get("a")->value == "new" && reader.Get("z")->value == "new", "Later input wins equal-key ties");
    const auto reversed = directory.Base() + ".reversed";
    kvstore::MergeSSTables({b, a}, reversed);
    kvstore::SSTableReader reverse(reversed);
    Check(reverse.Get("a")->value == "old", "Priority follows supplied order, not filename ordering");
}

void TestPartialMergePreservesTombstones() {
    TestDirectory directory;
    const auto oldest = directory.Base() + ".outside", middle = directory.Base() + ".middle", newest = directory.Base() + ".newest";
    kvstore::WriteSSTable(oldest, {{Type::Value, "key", "ancient"}});
    kvstore::WriteSSTable(middle, {{Type::Value, "key", "old"}});
    kvstore::WriteSSTable(newest, {{Type::Value, "empty", ""}, {Type::Tombstone, "key", ""}});
    const auto output = directory.Base() + ".out";
    kvstore::MergeSSTables({middle, newest}, output);
    kvstore::SSTableReader reader(output), outside(oldest);
    Check(reader.Get("key")->type == Type::Tombstone && outside.Get("key")->value == "ancient",
          "Subset merge must keep a marker that hides a value outside its inputs");
    Check(reader.Get("empty")->type == Type::Value && reader.Get("empty")->value.empty(), "Empty value must remain a value");
}

void TestFullMergeDropsOnlyLatestDeletions() {
    TestDirectory directory;
    const auto a = directory.Base() + ".a", b = directory.Base() + ".b", output = directory.Base() + ".out";
    kvstore::WriteSSTable(a, {{Type::Value, "deleted", "old"}, {Type::Tombstone, "reinserted", ""}});
    kvstore::WriteSSTable(b, {{Type::Tombstone, "deleted", ""}, {Type::Value, "reinserted", "new"}});
    Check(kvstore::MergeSSTables({a, b}, output, true) == 1, "Full merge can remove obsolete tombstones and shadowed values");
    kvstore::SSTableReader reader(output);
    Check(!reader.Get("deleted") && reader.Get("reinserted")->value == "new", "New value must override older deletion");
}

void TestBinaryAndMaximumRecords() {
    TestDirectory directory;
    const auto a = directory.Base() + ".a", b = directory.Base() + ".b", output = directory.Base() + ".out";
    const std::string key("\0k", 2), value("v\0\xff", 3), high("\x80", 1);
    const std::string large(kvstore::kMaxWalPayloadBytes - 1, 'v');
    kvstore::WriteSSTable(a, {{Type::Value, "", ""}, {Type::Value, key, "old"}, {Type::Value, "a", large}});
    kvstore::WriteSSTable(b, {{Type::Value, key, value}, {Type::Value, "b", large}, {Type::Value, high, "high"}});
    Check(kvstore::MergeSSTables({a, b}, output) == 5, "Binary and maximum-sized records merge");
    kvstore::SSTableReader reader(output);
    Check(reader.Get("")->value.empty() && reader.Get(key)->value == value && reader.Get(high)->value == "high", "Binary order and content");
    Check(reader.Get("a")->value == large && reader.Get("b")->value == large, "Large records survive streaming output");
}

void TestEmptyAndInvalidMergeInputs() {
    TestDirectory directory;
    const auto empty = directory.Base() + ".empty", output = directory.Base() + ".out";
    Check(kvstore::MergeSSTables({}, empty) == 0, "No inputs produce a valid empty standalone output");
    Check(kvstore::MergeSSTables({empty, empty}, output) == 0, "Empty input cursors are skipped");
    const auto before = Read(output);
    Reject([&] { kvstore::MergeSSTables({empty}, output); }, "Never overwrite output");
    Check(Read(output) == before, "Existing output is preserved");
    Write(directory.Base() + ".bad", "bad input");
    Reject([&] { kvstore::MergeSSTables({empty, directory.Base() + ".bad"}, directory.Base()); }, "Validate every input before output");
    Check(!std::filesystem::exists(directory.Base()), "Bad input must not create output");
    Reject([&] { kvstore::MergeSSTables({directory.Base() + ".missing"}, directory.Base()); }, "Missing input must fail");
    Reject([&] { kvstore::MergeSSTables({empty}, empty); }, "Output cannot overwrite an input");
}

void Seed(const std::string& base, bool pending) {
    kvstore::Store store(base, kvstore::WalRecoveryMode::Strict, 0);
    store.Put("name", "Alice"); store.Put("removed", "old"); store.Put("keep", "yes"); store.Flush();
    store.Put("name", "Bob"); store.Delete("removed"); store.Put("empty", ""); store.Flush();
    if (pending) { store.Put("name", "Charlie"); store.Delete("keep"); }
}
void CheckView(kvstore::Store& store, bool pending) {
    Check(store.Get("name") == (pending ? "Charlie" : "Bob") && !store.Get("removed") && store.Get("empty") == "",
          "Latest values, deletion and empty values must survive compaction");
    Check(pending ? !store.Get("keep") : store.Get("keep") == "yes", "Pending memory must keep its priority");
}

void TestStorePublishAndReopen() {
    TestDirectory directory;
    const auto base = directory.Base();
    Seed(base, false);
    const auto before = Metadata(base);
    const auto wal = Read(Wal(base, before));
    {
        kvstore::Store store(base); store.Compact(); CheckView(store, false);
        const auto after = Metadata(base);
        Check(after.sstable_ids == std::vector<std::uint64_t>{5} && after.next_file_id == 6 && after.wal_id == before.wal_id,
              "Compaction allocates only one SSTable ID, not a new WAL");
        Check(Read(Wal(base, after)) == wal, "Active WAL bytes must not change");
        for (const auto id : before.sstable_ids) { Check(!std::filesystem::exists(Table(base, id)), "Old inputs must retire"); }
        kvstore::SSTableReader output(Table(base, 5));
        Check(output.size() == 3 && !output.Get("removed"), "Full output drops deletion history and retains live keys");
    }
    kvstore::Store reopened(base); CheckView(reopened, false);
}

void TestPendingMemoryAndWalRemain() {
    TestDirectory directory;
    const auto base = directory.Base();
    Seed(base, true);
    const auto before = Metadata(base);
    const auto bytes = Read(Wal(base, before));
    {
        kvstore::Store store(base);
        const auto memory_bytes = store.memtable_bytes();
        store.Compact();
        Check(store.memtable_bytes() == memory_bytes && Read(Wal(base, before)) == bytes, "Do not flush or clear pending state");
        kvstore::SSTableReader output(Table(base, Metadata(base).sstable_ids[0]));
        Check(output.Get("name")->value == "Bob" && output.Get("keep")->value == "yes", "Compaction output contains disk state only");
        CheckView(store, true);
    }
    kvstore::Store reopened(base); CheckView(reopened, true);
    reopened.Flush(); reopened.Compact(); CheckView(reopened, true);
    Check(!std::filesystem::exists(Wal(base, before)), "A later flush, rather than compaction, retires the old active WAL");
}

void TestNoTablesAndTombstoneOnlyOutput() {
    TestDirectory directory;
    const auto base = directory.Base();
    {
        kvstore::Store store(base); store.Put("pending", "value");
        const auto before = Read(base + ".manifest");
        store.Compact();
        Check(Read(base + ".manifest") == before && store.Get("pending") == "value", "No SSTables means a no-op");
        store.Delete("pending"); store.Flush();
        store.Compact();
        kvstore::SSTableReader output(Table(base, Metadata(base).sstable_ids[0]));
        Check(output.size() == 0 && store.sstable_count() == 1 && !store.Get("pending"), "Single tombstone file compacts to a valid empty table");
        store.Compact();
        store.Put("pending", "new"); store.Flush(); store.Compact();
        Check(store.Get("pending") == "new", "Reinsert after deletion GC remains correct");
    }
    kvstore::Store reopened(base);
    Check(reopened.Get("pending") == "new", "Reinserted state survives reopen");
}

void TestStableLockAndRepeatedCompaction() {
    TestDirectory directory;
    const auto base = directory.Base();
    Seed(base, false);
    {
        kvstore::Store store(base);
        for (int i = 0; i < 3; ++i) {
            const auto old_ids = Metadata(base).sstable_ids;
            store.Compact();
            Check(store.sstable_count() == 1, "Repeated compaction leaves one active SSTable");
            for (const auto id : old_ids) { Check(!std::filesystem::exists(Table(base, id)), "Each superseded generation retires"); }
            Reject([&] { kvstore::Store second(base); }, "Compaction must retain Store lock");
            store.Put("counter", std::to_string(i)); store.Flush();
        }
    }
    kvstore::Store reopened(base);
    Check(reopened.Get("counter") == "2", "Flushing after compaction continues using unique file IDs");
}

void TestDamagedInputCannotPublish() {
    TestDirectory directory;
    const auto base = directory.Base();
    Seed(base, false);
    const auto manifest = Metadata(base);
    const auto path = Table(base, manifest.sstable_ids[0]);
    const auto original = Read(path);
    const auto metadata = Read(base + ".manifest");
    {
        kvstore::Store store(base);
        auto damaged = original; damaged.back() ^= 1; Write(path, damaged);
        Reject([&] { store.Compact(); }, "Fresh compaction readers must validate inputs before producing output");
        Reject([&] { store.Get("name"); }, "Input failure must stop current Store");
        Check(Read(base + ".manifest") == metadata && !std::filesystem::exists(Table(base, manifest.next_file_id)),
              "Bad input must not publish metadata or create output");
    }
    Write(path, original);
    kvstore::Store reopened(base); CheckView(reopened, false);
}

void TestKilledCompactionAtEveryStage() {
    for (bool pending : {false, true}) {
        for (std::size_t index = 0; index < stages.size(); ++index) {
            TestDirectory directory;
            const auto base = directory.Base();
            Seed(base, pending);
            const auto old = Metadata(base);
            const auto wal_bytes = Read(Wal(base, old));
            const auto pid = ::fork();
            Check(pid != -1, "Fork interrupted compaction");
            if (pid == 0) {
                try {
                    kvstore::Store store(base);
                    int removed = 0;
                    kvstore::StoreTestAccess::Hook(store, [index, &removed](Stage stage) {
                        if (stage == Stage::OldTableRemoved) { ++removed; }
                        const bool selected_removal = index == 7 ? removed == 2 : removed == 1;
                        if (stage == stages[index] && (stage != Stage::OldTableRemoved || selected_removal)) {
                            ::kill(::getpid(), SIGKILL); ::_exit(3);
                        }
                    });
                    store.Compact();
                } catch (...) { ::_exit(2); }
                ::_exit(4);
            }
            Child child(pid);
            const auto status = child.Wait();
            Check(WIFSIGNALED(status) && WTERMSIG(status) == SIGKILL, "Child must reach selected boundary");
            Check(Metadata(base).sstable_ids.size() == (index >= 3 ? 1U : 2U), "Visible manifest selects complete old or new file set");
            const auto removed_count = index < 6 ? 0U : index == 6 ? 1U : 2U;
            for (std::size_t i = 0; i < old.sstable_ids.size(); ++i) {
                Check(std::filesystem::exists(Table(base, old.sstable_ids[i])) == (i >= removed_count),
                      "Inputs must survive durable publication and retire only afterwards");
            }
            Check(Read(Wal(base, old)) == wal_bytes, "Interrupted compaction must not alter active WAL");
            {
                kvstore::Store store(base); CheckView(store, pending);
                store.Compact(); store.Put("later", "ok"); store.Flush(); store.Compact();
            }
            kvstore::Store reopened(base); CheckView(reopened, pending);
            Check(reopened.Get("later") == "ok", "Recovery must permit subsequent writes, flushes and compactions");
        }
    }
}

void TestExceptionsStopStoreAndRecover() {
    for (std::size_t index = 0; index < stages.size(); ++index) {
        TestDirectory directory;
        const auto base = directory.Base();
        Seed(base, true);
        {
            kvstore::Store store(base);
            int removed = 0;
            kvstore::StoreTestAccess::Hook(store, [index, &removed](Stage stage) {
                if (stage == Stage::OldTableRemoved) { ++removed; }
                const bool selected_removal = index == 7 ? removed == 2 : removed == 1;
                if (stage == stages[index] && (stage != Stage::OldTableRemoved || selected_removal)) {
                    throw std::runtime_error("Injected compaction boundary failure");
                }
            });
            Reject([&] { store.Compact(); }, "Compaction boundary failure must escape");
            Reject([&] { store.Get("name"); }, "Reads stop after uncertain compaction");
            Reject([&] { store.Put("new", "value"); }, "Writes stop after uncertain compaction");
            Reject([&] { store.Flush(); }, "Flush stops after uncertain compaction");
            Reject([&] { store.Compact(); }, "Further compaction stops");
        }
        kvstore::Store reopened(base); CheckView(reopened, true); reopened.Compact();
    }
}

void TestRealPartialOutputAndMetadataWrites() {
    for (const auto limit_bytes : {4, 20, 35}) {
        TestDirectory directory;
        const auto base = directory.Base();
        { kvstore::Store store(base); store.Put("k", "v"); store.Flush(); }
        const auto before = Metadata(base);
        const auto metadata_bytes = Read(base + ".manifest");
        const auto original = Read(Table(base, before.sstable_ids[0]));
        const auto pid = ::fork();
        Check(pid != -1, "Fork partial compaction test");
        if (pid == 0) {
            try {
                kvstore::Store store(base);
                Check(::signal(SIGXFSZ, SIG_IGN) != SIG_ERR, "Ignore file-size signal");
                struct rlimit limit {};
                Check(::getrlimit(RLIMIT_FSIZE, &limit) == 0, "Read resource limit");
                limit.rlim_cur = limit_bytes;
                Check(::setrlimit(RLIMIT_FSIZE, &limit) == 0, "Set child-only resource limit");
                bool failed = false;
                try { store.Compact(); } catch (const std::system_error&) { failed = true; }
                Check(failed, "Operating system must report a real partial write");
                Reject([&] { store.Get("k"); }, "Partial write makes Store unusable");
                ::_exit(0);
            } catch (...) { ::_exit(2); }
        }
        Child child(pid);
        const auto status = child.Wait();
        Check(WIFEXITED(status) && WEXITSTATUS(status) == 0, "Child must verify failed compaction");
        const auto partial = limit_bytes < 27 ? Table(base, before.next_file_id) : base + ".manifest.tmp";
        Check(std::filesystem::file_size(partial) == static_cast<std::size_t>(limit_bytes), "Real partial file exists");
        Check(Read(base + ".manifest") == metadata_bytes && Read(Table(base, before.sstable_ids[0])) == original,
              "Unpublished compaction retains authoritative metadata and input");
        kvstore::Store reopened(base);
        Check(reopened.Get("k") == "v", "Old input must recover after failed output");
        reopened.Compact();
        Check(reopened.Get("k") == "v" && Metadata(base).wal_id == before.wal_id, "Retry cleans orphans and keeps WAL");
    }
}

void TestPendingWalTailAfterCompaction() {
    TestDirectory directory;
    const auto base = directory.Base();
    Seed(base, true);
    { kvstore::Store store(base); store.Compact(); }
    const auto wal = Wal(base, Metadata(base));
    Write(wal, "KVW", true);
    const auto before = Read(wal);
    Reject([&] { kvstore::Store store(base); }, "Compaction does not eliminate strict active-WAL validation");
    Check(Read(wal) == before, "Strict recovery preserves incomplete tail");
    kvstore::Store reopened(base, kvstore::WalRecoveryMode::TruncateIncompleteTail);
    Check(reopened.truncated_tail_bytes() == 3, "Tail repair applies only to active WAL");
    CheckView(reopened, true);
}

void TestMixedModelAcrossFlushCompactAndReopen() {
    TestDirectory directory;
    const auto base = directory.Base();
    std::map<std::string, std::string> expected;
    std::mt19937 random(42);
    auto store = std::make_unique<kvstore::Store>(base, kvstore::WalRecoveryMode::Strict, 0);
    for (int step = 0; step < 96; ++step) {
        const auto key = "key" + std::to_string(random() % 12);
        if (random() % 3 != 0) {
            const auto value = random() % 4 == 0 ? "" : std::to_string(step);
            store->Put(key, value); expected[key] = value;
        } else {
            const bool existed = expected.erase(key) != 0;
            Check(store->Delete(key) == existed, "Delete bool must match complete model view");
        }
        if (step % 7 == 0) { store->Flush(); }
        if (step % 13 == 0) { store->Compact(); }
        if (step % 19 == 0) { store.reset(); store = std::make_unique<kvstore::Store>(base); }
        for (int i = 0; i < 12; ++i) {
            const auto checked = "key" + std::to_string(i);
            const auto found = expected.find(checked);
            const auto actual = store->Get(checked);
            Check(found == expected.end() ? !actual : actual == found->second, "Mixed operation model must match every key");
        }
    }
    store->Flush(); store->Compact(); store.reset();
    kvstore::Store reopened(base);
    for (const auto& entry : expected) { Check(reopened.Get(entry.first) == entry.second, "Final reopen matches model"); }
}

int main() {
    try {
        TestMergeLatestByInputOrder(); TestPartialMergePreservesTombstones(); TestFullMergeDropsOnlyLatestDeletions();
        TestBinaryAndMaximumRecords(); TestEmptyAndInvalidMergeInputs(); TestStorePublishAndReopen();
        TestPendingMemoryAndWalRemain(); TestNoTablesAndTombstoneOnlyOutput(); TestStableLockAndRepeatedCompaction();
        TestDamagedInputCannotPublish(); TestKilledCompactionAtEveryStage(); TestExceptionsStopStoreAndRecover();
        TestRealPartialOutputAndMetadataWrites(); TestPendingWalTailAfterCompaction(); TestMixedModelAcrossFlushCompactAndReopen();
        std::cout << "All 15 compaction tests passed.\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "Compaction test failed: " << error.what() << '\n';
        return 1;
    }
}
