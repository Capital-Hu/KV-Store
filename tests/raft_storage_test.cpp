#include "kvstore/raft/node.h"
#include "kvstore/raft/storage.h"
#include "../src/raft/storage_codec.h"
#include "../src/coding.h"

#include <cerrno>
#include <csignal>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <iterator>
#include <memory>
#include <random>
#include <stdexcept>
#include <string>
#include <system_error>
#include <vector>

#include <sys/resource.h>
#include <sys/wait.h>
#include <unistd.h>

namespace kvstore::raft {
struct StorageTestAccess {
    using Stage = Storage::Stage;
    static void Hook(Storage& storage, std::function<void(Stage)> hook) { storage.hook_ = std::move(hook); }
};
}  // namespace kvstore::raft

namespace {
namespace raft = kvstore::raft;
namespace codec = kvstore::raft::detail;
namespace coding = kvstore::detail;
using Stage = raft::StorageTestAccess::Stage;
constexpr auto kNew = raft::StorageOpenMode::CreateNew;
constexpr auto kExisting = raft::StorageOpenMode::OpenExisting;
const raft::NodeConfig kConfig{1, {2, 3}};

void Check(bool condition, const std::string& message) {
    if (!condition) { throw std::runtime_error(message); }
}

template <typename Action>
void Reject(Action action, const std::string& message) {
    try { action(); } catch (const std::exception&) { return; }
    throw std::runtime_error(message);
}

class TestDirectory {
public:
    TestDirectory() {
        char pattern[] = "/tmp/kvstore-raft-XXXXXX";
        const auto path = ::mkdtemp(pattern);
        Check(path != nullptr, "Create Raft test directory");
        path_ = path;
    }
    ~TestDirectory() { std::error_code ignored; std::filesystem::remove_all(path_, ignored); }
    std::string Path(const std::string& name = "node.raft") const { return (path_ / name).string(); }
private:
    std::filesystem::path path_;
};

std::string Read(const std::string& path) {
    std::ifstream input(path, std::ios::binary);
    Check(input.is_open(), "Read Raft fixture");
    return {std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
}

void Write(const std::string& path, const std::string& bytes) {
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    output.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    output.close();
    Check(!output.fail(), "Write Raft fixture");
}

raft::LogEntry Put(raft::LogIndex index, raft::Term term, std::string key, std::string value) {
    return {index, term, {raft::CommandType::Put, std::move(key), std::move(value)}};
}

raft::LogEntry Delete(raft::LogIndex index, raft::Term term, std::string key) {
    return {index, term, {raft::CommandType::Delete, std::move(key), ""}};
}

bool SameState(const raft::PersistentState& left, const raft::PersistentState& right) {
    if (left.current_term != right.current_term || left.voted_for != right.voted_for ||
        left.log.size() != right.log.size()) { return false; }
    for (std::size_t i = 0; i < left.log.size(); ++i) {
        const auto& a = left.log[i]; const auto& b = right.log[i];
        if (a.index != b.index || a.term != b.term || a.command.type != b.command.type ||
            a.command.key != b.command.key || a.command.value != b.command.value) { return false; }
    }
    return true;
}

raft::PersistentState Seed(const std::string& path, raft::Term term = 1) {
    raft::Storage storage(path, kConfig, kNew);
    storage.SetTermAndVote(term, 1);
    storage.Append({Put(1, 1, "a", "old-a"), Put(2, 1, "b", "old-b")});
    return storage.state();
}

class Child {
public:
    explicit Child(pid_t pid) : pid_(pid) { Check(pid > 0, "Fork Raft test child"); }
    ~Child() {
        if (pid_ > 0) {
            ::kill(pid_, SIGKILL);
            while (::waitpid(pid_, nullptr, 0) == -1 && errno == EINTR) {}
        }
    }
    int Wait() {
        int status = 0;
        pid_t result;
        do { result = ::waitpid(pid_, &status, 0); } while (result == -1 && errno == EINTR);
        Check(result == pid_, "Wait for Raft test child");
        pid_ = -1;
        return status;
    }
private:
    pid_t pid_;
};

[[noreturn]] void KillSelf() {
    ::kill(::getpid(), SIGKILL);
    for (;;) { ::pause(); }
}

void TestCreateReopenAndIdentity() {
    TestDirectory directory;
    const auto path = directory.Path();
    {
        raft::Storage storage(path, kConfig, kNew);
        Check(storage.state().current_term == 0 && !storage.state().voted_for && storage.state().log.empty(),
              "New durable state starts empty");
        Check(Read(path).size() == 32 && Read(path).substr(0, 4) == "KVR1", "Published identity header");
        Check(std::filesystem::exists(path + ".lock") && !std::filesystem::exists(path + ".tmp"), "Bootstrap publication");
    }
    { raft::Storage storage(path, {1, {3, 2}}, kExisting); Check(storage.state().log.empty(), "Peer order is not identity"); }
    const auto before = Read(path);
    Reject([&] { raft::Storage storage(path, {2, {1, 3}}, kExisting); }, "Reject another node identity");
    Reject([&] { raft::Storage storage(path, {1, {2, 4}}, kExisting); }, "Reject another cluster identity");
    Reject([&] { raft::Storage storage(path, kConfig, kNew); }, "CreateNew cannot overwrite existing state");
    Check(Read(path) == before, "Identity/open-mode errors leave evidence unchanged");
}

void TestTermAndVoteRules() {
    TestDirectory directory;
    const auto path = directory.Path();
    {
        raft::Storage storage(path, kConfig, kNew);
        Reject([&] { storage.SetTermAndVote(0, 1); }, "Cannot vote before a nonzero term");
        storage.SetTermAndVote(1, std::nullopt);
        storage.SetTermAndVote(1, 2);
        const auto bytes = Read(path);
        storage.SetTermAndVote(1, 2);
        Check(Read(path) == bytes, "Repeated vote is idempotent");
        Reject([&] { storage.SetTermAndVote(1, 3); }, "Cannot vote twice within a term");
        Reject([&] { storage.SetTermAndVote(1, std::nullopt); }, "Cannot clear a vote within a term");
        Reject([&] { storage.SetTermAndVote(0, std::nullopt); }, "Cannot decrease term");
        Reject([&] { storage.SetTermAndVote(2, 0); }, "Zero is not a member");
        Reject([&] { storage.SetTermAndVote(2, 4); }, "Non-member is not a vote target");
        Check(storage.usable() && Read(path) == bytes, "Invalid requests neither append nor poison storage");
    }
    {
        raft::Storage storage(path, kConfig, kExisting);
        Check(storage.state().current_term == 1 && storage.state().voted_for == 2, "Restart retains prior vote");
        Reject([&] { storage.SetTermAndVote(1, 1); }, "Restart cannot enable a second vote");
        storage.SetTermAndVote(2, std::nullopt);
        storage.SetTermAndVote(2, 1);
    }
    raft::Storage storage(path, kConfig, kExisting);
    Check(storage.state().current_term == 2 && storage.state().voted_for == 1, "New term can choose a fresh vote");
}

void TestAppendBinaryAndMaximumCommand() {
    TestDirectory directory;
    raft::PersistentState expected;
    {
        raft::Storage storage(directory.Path(), kConfig, kNew);
        storage.SetTermAndVote(4, 1);
        storage.Append({Put(1, 1, "", ""), Put(2, 2, std::string("k\0x", 3), std::string("v\0y", 3)),
                        Delete(3, 2, "missing"), Put(4, 4, "max", std::string(raft::kMaxRaftCommandBytes - 3, 'x'))});
        expected = storage.state();
        const auto bytes = Read(directory.Path());
        storage.Append({});
        Check(Read(directory.Path()) == bytes, "Empty append does not emit a record");
    }
    raft::Storage storage(directory.Path(), kConfig, kExisting);
    Check(SameState(storage.state(), expected), "Binary, empty, deletion and maximum commands survive restart");
}

void TestReplaceAndTruncateSuffix() {
    TestDirectory directory;
    const auto path = directory.Path();
    Seed(path, 3);
    raft::PersistentState expected;
    {
        raft::Storage storage(path, kConfig, kExisting);
        const auto before_bytes = std::filesystem::file_size(path);
        storage.ReplaceSuffix(2, {Delete(2, 2, "b"), Put(3, 3, "c", "new-c")});
        Check(storage.state().log[0].command.value == "old-a", "Suffix replacement preserves the prefix");
        Check(std::filesystem::file_size(path) > before_bytes, "Logical suffix replacement appends to the journal");
        storage.ReplaceSuffix(3, {});
        Check(storage.state().log.size() == 2, "Empty replacement truncates the logical suffix");
        storage.Append({Put(3, 3, "d", "new-d")});
        const auto bytes = Read(path);
        storage.ReplaceSuffix(2, {storage.state().log[1], storage.state().log[2]});
        Check(Read(path) == bytes, "Identical suffix replacement is idempotent");
        expected = storage.state();
    }
    raft::Storage storage(path, kConfig, kExisting);
    Check(SameState(storage.state(), expected), "Replayed replacements never resurrect old suffix entries");
    storage.ReplaceSuffix(1, {});
    Check(storage.state().log.empty(), "Whole uncommitted log can be logically removed");
}

void TestInvalidMutationsPreserveState() {
    TestDirectory directory;
    const auto path = directory.Path();
    const auto expected = Seed(path, 3);
    raft::Storage storage(path, kConfig, kExisting);
    const auto bytes = Read(path);
    const auto reject = [&](raft::LogIndex start, std::vector<raft::LogEntry> entries) {
        Reject([&] { storage.ReplaceSuffix(start, entries); }, "Reject invalid suffix");
        Check(storage.usable() && SameState(storage.state(), expected) && Read(path) == bytes,
              "Invalid suffix preserves memory and disk");
    };
    reject(0, {}); reject(4, {}); reject(3, {Put(4, 3, "gap", "x")});
    reject(3, {Put(3, 0, "term-zero", "x")}); reject(3, {Put(3, 4, "future-term", "x")});
    reject(3, {Put(3, 3, "a", "x"), Put(4, 2, "b", "y")});
    reject(2, {Put(2, 1, "b", "different-command-same-term")});
    auto bad_delete = Delete(3, 3, "a"); bad_delete.command.value = "invalid";
    reject(3, {bad_delete});
    auto bad_type = Put(3, 3, "a", "x"); bad_type.command.type = static_cast<raft::CommandType>(99);
    reject(3, {bad_type});
    reject(3, {Put(3, 3, "x", std::string(raft::kMaxRaftCommandBytes, 'v'))});
    storage.Append({Put(3, 3, "valid", "still-usable")});
}

void TestRecordBatchLimit() {
    TestDirectory directory;
    raft::Storage storage(directory.Path(), kConfig, kNew);
    storage.SetTermAndVote(1, 1);
    std::vector<raft::LogEntry> entries;
    for (std::size_t i = 0; i < 16; ++i) {
        entries.push_back(Put(i + 1, 1, "", std::string(raft::kMaxRaftCommandBytes, 'x')));
    }
    const auto bytes = Read(directory.Path());
    Reject([&] { storage.Append(entries); }, "16 MiB commands plus metadata exceed a 16 MiB record");
    Check(storage.state().log.empty() && storage.usable() && Read(directory.Path()) == bytes,
          "Oversized batch is rejected before any write");
    storage.Append({Put(1, 1, "small", "allowed")});
}

void CheckRefusesBothModes(const std::string& path, const std::string& bytes) {
    Write(path, bytes);
    for (const auto mode : {raft::StorageRecoveryMode::Strict, raft::StorageRecoveryMode::TruncateIncompleteTail}) {
        Reject([&] { raft::Storage storage(path, kConfig, kExisting, mode); }, "Reject corrupt journal");
        Check(Read(path) == bytes, "Corruption is never silently repaired");
    }
}

void TestHeaderAndChecksumCorruption() {
    TestDirectory directory;
    const auto path = directory.Path();
    Seed(path);
    const auto valid = Read(path);
    for (const std::size_t offset : {std::size_t{0}, std::size_t{12}, std::size_t{28}, std::size_t{32},
                                   std::size_t{36}, std::size_t{40}, std::size_t{44}, valid.size() - 1}) {
        auto bytes = valid; bytes[offset] ^= 1;
        CheckRefusesBothModes(path, bytes);
    }
    CheckRefusesBothModes(path, valid.substr(0, 31));
    CheckRefusesBothModes(path, valid + "garbage");
}

void TestMalformedChecksummedRecords() {
    TestDirectory directory;
    const auto path = directory.Path();
    Seed(path);
    const auto prefix = Read(path);
    std::vector<std::string> bad_payloads{std::string(1, '\x7f'), std::string(1, '\x01')};
    codec::Change change;
    change.term = 0; bad_payloads.push_back(codec::EncodeChange(change));
    change.term = 1; change.voted_for = 2; bad_payloads.push_back(codec::EncodeChange(change));
    change.term = 2; change.voted_for = 9; bad_payloads.push_back(codec::EncodeChange(change));
    change.term = 2; change.voted_for.reset(); bad_payloads.push_back(codec::EncodeChange(change) + "x");
    change.type = codec::ChangeType::Suffix;
    change.first_index = 3; change.entries = {Put(4, 1, "gap", "x")};
    bad_payloads.push_back(codec::EncodeChange(change));
    change.entries = {Put(3, 2, "future-term", "x")}; bad_payloads.push_back(codec::EncodeChange(change));
    change.entries = {Delete(3, 1, "bad-delete")}; change.entries[0].command.value = "x";
    bad_payloads.push_back(codec::EncodeChange(change));
    change.entries = {Put(3, 1, "key", "value")};
    auto payload = codec::EncodeChange(change); payload[29] = '\x7f';
    bad_payloads.push_back(payload);
    payload = codec::EncodeChange(change);
    payload.replace(9, 4, std::string(4, '\xff')); bad_payloads.push_back(payload);
    payload = codec::EncodeChange(change);
    payload.replace(30, 4, std::string(4, '\xff')); bad_payloads.push_back(payload);
    for (const auto& malformed : bad_payloads) {
        CheckRefusesBothModes(path, prefix + codec::EncodeFrame(malformed));
    }
    auto header = std::string("RFR1");
    coding::AppendU32(header, static_cast<std::uint32_t>(raft::kMaxRaftRecordBytes + 1));
    coding::AppendU32(header, coding::Crc32(header));
    CheckRefusesBothModes(path, prefix + header);
}

void TestEveryIncompleteTailBoundary() {
    for (const bool suffix : {false, true}) {
        TestDirectory directory;
        const auto path = directory.Path();
        const auto expected = Seed(path, 2);
        const auto prefix = Read(path);
        raft::PersistentState updated;
        {
            raft::Storage storage(path, kConfig, kExisting);
            if (suffix) { storage.ReplaceSuffix(2, {Delete(2, 2, "b"), Put(3, 2, "c", "new")}); }
            else { storage.SetTermAndVote(3, 2); }
            updated = storage.state();
        }
        const auto frame = Read(path).substr(prefix.size());
        for (std::size_t size = 0; size < frame.size(); ++size) {
            const auto truncated = prefix + frame.substr(0, size);
            Write(path, truncated);
            if (size != 0) {
                Reject([&] { raft::Storage storage(path, kConfig, kExisting); }, "Strict mode rejects every torn tail");
                Check(Read(path) == truncated, "Strict mode preserves incomplete bytes");
            }
            {
                raft::Storage storage(path, kConfig, kExisting, raft::StorageRecoveryMode::TruncateIncompleteTail);
                Check(storage.truncated_tail_bytes() == size && SameState(storage.state(), expected),
                      "Recovery retains the complete prefix at every byte boundary");
                Check(Read(path) == prefix, "Only incomplete tail bytes are removed");
                storage.Append({Put(3, 2, "after-recovery", "ok")});
            }
            { raft::Storage storage(path, kConfig, kExisting); Check(storage.state().log.size() == 3, "Recovered journal accepts writes"); }
        }
        Write(path, prefix + frame);
        raft::Storage storage(path, kConfig, kExisting);
        Check(SameState(storage.state(), updated), "A complete record is replayed as a whole");
    }
}

void TestExclusiveOwnershipAndMissingFiles() {
    TestDirectory directory;
    const auto path = directory.Path();
    {
        raft::Storage storage(path, kConfig, kNew);
        Reject([&] { raft::Storage other(path, kConfig, kExisting); }, "Exclusive owner blocks another open");
        raft::Storage independent(directory.Path("other.raft"), {2, {1, 3}}, kNew);
        independent.SetTermAndVote(1, 2);
    }
    std::filesystem::remove(path);
    Reject([&] { raft::Storage storage(path, kConfig, kExisting); }, "Missing journal is not recreated");
    Reject([&] { raft::Storage storage(path, kConfig, kNew); }, "Existing lock prevents silent reinitialization");
    Check(!std::filesystem::exists(path), "Lost journal remains missing");
    Reject([&] { raft::Storage storage(directory.Path("never-created"), kConfig, kExisting); }, "OpenExisting cannot bootstrap");
}

void TestFailureStopsStorage() {
    for (const auto failure : {Stage::RecordWritten, Stage::RecordSynced, Stage::StateUpdated}) {
        for (const bool suffix : {false, true}) {
            TestDirectory directory;
            const auto path = directory.Path();
            const auto before = Seed(path, 2);
            auto after = before;
            if (suffix) { after.log = {before.log[0], Delete(2, 2, "b")}; }
            else { after.current_term = 3; after.voted_for = 2; }
            {
                raft::Storage storage(path, kConfig, kExisting);
                raft::StorageTestAccess::Hook(storage, [&](Stage stage) {
                    Check(SameState(storage.state(), stage == Stage::StateUpdated ? after : before),
                          "In-memory state is published only after synchronization");
                    if (stage == failure) { throw std::runtime_error("Injected Raft storage failure"); }
                });
                if (suffix) { Reject([&] { storage.ReplaceSuffix(2, {Delete(2, 2, "b")}); }, "Propagate suffix write failure"); }
                else { Reject([&] { storage.SetTermAndVote(3, 2); }, "Propagate hard-state write failure"); }
                Check(!storage.usable(), "An uncertain write poisons the storage");
                Reject([&] { storage.state(); }, "Failed storage cannot expose stale state");
                Reject([&] { storage.Append({}); }, "Failed storage cannot accept further writes");
                Reject([&] { storage.SetTermAndVote(9, 1); }, "Failed storage cannot vote again");
            }
            raft::Storage reopened(path, kConfig, kExisting);
            Check(SameState(reopened.state(), after), "A complete unacknowledged record may survive an exception");
        }
    }
}

void TestKilledUpdatesAtEachStage() {
    const std::vector<Stage> stages{Stage::RecordWritten, Stage::RecordSynced, Stage::StateUpdated};
    for (const bool suffix : {false, true}) {
        for (std::size_t cut = 0; cut <= stages.size(); ++cut) {
            TestDirectory directory;
            const auto path = directory.Path();
            const auto before = Seed(path, 2);
            auto after = before;
            if (suffix) { after.log = {before.log[0], Delete(2, 2, "b"), Put(3, 2, "c", "new")}; }
            else { after.current_term = 3; after.voted_for = 2; }
            const auto pid = ::fork();
            Check(pid != -1, "Fork killed Raft update");
            if (pid == 0) {
                ::alarm(15);
                try {
                    raft::Storage storage(path, kConfig, kExisting);
                    if (cut < stages.size()) {
                        raft::StorageTestAccess::Hook(storage, [&](Stage stage) { if (stage == stages[cut]) { KillSelf(); } });
                    }
                    if (suffix) { storage.ReplaceSuffix(2, {Delete(2, 2, "b"), Put(3, 2, "c", "new")}); }
                    else { storage.SetTermAndVote(3, 2); }
                    if (cut == stages.size()) { KillSelf(); }
                    ::_exit(4);
                } catch (...) { ::_exit(2); }
            }
            Child child(pid);
            const auto status = child.Wait();
            Check(WIFSIGNALED(status) && WTERMSIG(status) == SIGKILL, "Child reaches chosen Raft persistence boundary");
            raft::Storage reopened(path, kConfig, kExisting);
            Check(SameState(reopened.state(), after) || (cut == 0 && SameState(reopened.state(), before)),
                  "Restart sees whole old/new state; synced updates must survive process crash");
            reopened.SetTermAndVote(4, std::nullopt);
            reopened.Append({Put(reopened.state().log.size() + 1, 4, "continued", "ok")});
        }
    }
}

void TestRealPartialWrites() {
    for (const bool suffix : {false, true}) {
        TestDirectory directory;
        const auto path = directory.Path();
        const auto before = Seed(path, 2);
        const auto prefix = Read(path);
        const std::size_t partial = suffix ? 18 : 11;
        const auto pid = ::fork();
        Check(pid != -1, "Fork real partial Raft write");
        if (pid == 0) {
            ::alarm(15);
            try {
                raft::Storage storage(path, kConfig, kExisting);
                ::signal(SIGXFSZ, SIG_IGN);
                const auto limit = static_cast<rlim_t>(prefix.size() + partial);
                const rlimit size_limit{limit, limit};
                Check(::setrlimit(RLIMIT_FSIZE, &size_limit) == 0, "Limit Raft child file size");
                bool failed = false;
                try {
                    if (suffix) { storage.ReplaceSuffix(2, {Delete(2, 2, "b")}); }
                    else { storage.SetTermAndVote(3, 2); }
                } catch (const std::system_error& error) { failed = error.code().value() == EFBIG; }
                Check(failed && !storage.usable(), "Real partial write fails and poisons storage");
                Reject([&] { storage.state(); }, "Uncertain state cannot be reused");
                ::_exit(0);
            } catch (...) { ::_exit(2); }
        }
        Child child(pid);
        const auto status = child.Wait();
        Check(WIFEXITED(status) && WEXITSTATUS(status) == 0, "Child observes the expected actual I/O failure");
        Check(std::filesystem::file_size(path) == prefix.size() + partial, "Actual short write reached the exact file limit");
        Reject([&] { raft::Storage storage(path, kConfig, kExisting); }, "Strict mode refuses real torn record");
        raft::Storage storage(path, kConfig, kExisting, raft::StorageRecoveryMode::TruncateIncompleteTail);
        Check(SameState(storage.state(), before) && storage.truncated_tail_bytes() == partial && Read(path) == prefix,
              "Partial hard-state/suffix writes retain the old complete state");
        storage.SetTermAndVote(3, 2);
    }
}

void TestNodeRestoreAndIndependentStorage() {
    TestDirectory directory;
    const auto path = directory.Path();
    const auto expected = Seed(path, 5);
    {
        auto storage = std::make_unique<raft::Storage>(path, kConfig, kExisting);
        raft::Node node({1, {3, 2}}, std::move(storage));
        Check(!storage && SameState(node.persistent_state(), expected), "Node owns and reads restored durable state");
        const auto status = node.status();
        Check(status.role == raft::Role::Follower && status.term == 5 && !status.leader_id &&
              status.last_log_index == 2 && status.last_log_term == 1 && status.commit_index == 0 &&
              status.last_applied == 0 && status.logical_time.count() == 0,
              "Restart restores term/log but never invents leadership, commitment or application");
    }
    Reject([&] {
        raft::Node wrong({2, {1, 3}}, std::make_unique<raft::Storage>(path, kConfig, kExisting));
    }, "Node/storage identity must agree");
    raft::Storage reopened(path, kConfig, kExisting);
    Check(SameState(reopened.state(), expected), "Ownership ends cleanly and releases the file lock");
}

void TestModelAcrossReopens() {
    TestDirectory directory;
    const auto path = directory.Path();
    { raft::Storage storage(path, kConfig, kNew); }
    raft::PersistentState model;
    std::mt19937 random(20261015);
    for (int round = 0; round < 25; ++round) {
        raft::Storage storage(path, kConfig, kExisting);
        Check(SameState(storage.state(), model), "Journal replay matches the independently maintained sequence");
        ++model.current_term; model.voted_for = 1 + random() % 3;
        storage.SetTermAndVote(model.current_term, model.voted_for);
        const auto prefix = model.log.empty() ? 0 : random() % (model.log.size() + 1);
        std::vector<raft::LogEntry> suffix;
        const auto count = random() % 4;
        for (std::size_t i = 0; i < count; ++i) {
            suffix.push_back(Put(prefix + i + 1, model.current_term, "key-" + std::to_string(i), std::to_string(round)));
        }
        storage.ReplaceSuffix(prefix + 1, suffix);
        model.log.resize(prefix); model.log.insert(model.log.end(), suffix.begin(), suffix.end());
        Check(SameState(storage.state(), model), "Logical suffix model matches before close");
    }
    raft::Storage final(path, kConfig, kExisting);
    Check(SameState(final.state(), model), "The final journal state also survives reopen");
}
}  // namespace

int main() {
    try {
        TestCreateReopenAndIdentity(); TestTermAndVoteRules(); TestAppendBinaryAndMaximumCommand();
        TestReplaceAndTruncateSuffix(); TestInvalidMutationsPreserveState(); TestRecordBatchLimit();
        TestHeaderAndChecksumCorruption(); TestMalformedChecksummedRecords(); TestEveryIncompleteTailBoundary();
        TestExclusiveOwnershipAndMissingFiles(); TestFailureStopsStorage(); TestKilledUpdatesAtEachStage();
        TestRealPartialWrites(); TestNodeRestoreAndIndependentStorage(); TestModelAcrossReopens();
        std::cout << "All 15 Raft storage tests passed (8 process crashes and 2 real partial writes).\n";
    } catch (const std::exception& error) {
        std::cerr << "Raft storage test failed: " << error.what() << '\n';
        return 1;
    }
    return 0;
}
