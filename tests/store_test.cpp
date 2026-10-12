#include "kvstore/store.h"
#include "kvstore/wal_record.h"

#include <cerrno>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <iterator>
#include <stdexcept>
#include <string>
#include <system_error>

#include <csignal>
#include <sys/resource.h>
#include <sys/wait.h>
#include <unistd.h>

void Check(bool condition, const std::string& message) {
    if (!condition) {
        throw std::runtime_error(message);
    }
}

void CheckThrows(const std::function<void()>& action, const std::string& message) {
    bool rejected = false;
    try {
        action();
    } catch (const std::exception&) {
        rejected = true;
    }
    Check(rejected, message);
}

class TestDirectory {
public:
    TestDirectory() {
        char pattern[] = "/tmp/kvstore-test-XXXXXX";
        const char* path = ::mkdtemp(pattern);
        Check(path != nullptr, "Create test directory");
        path_ = path;
    }
    ~TestDirectory() {
        std::error_code ignored;
        std::filesystem::remove_all(path_, ignored);
    }
    std::string File(const std::string& name = "store.wal") const {
        return (path_ / name).string();
    }

private:
    std::filesystem::path path_;
};

// Ensure a failed test never leaves a paused child running.
class ChildProcess {
public:
    explicit ChildProcess(pid_t pid) : pid_(pid) {}
    ~ChildProcess() {
        if (pid_ > 0) {
            ::kill(pid_, SIGKILL);
            int status;
            while (::waitpid(pid_, &status, 0) == -1 && errno == EINTR) {}
        }
    }
    int Wait() {
        int status;
        pid_t result;
        do {
            result = ::waitpid(pid_, &status, 0);
        } while (result == -1 && errno == EINTR);
        Check(result == pid_, "Wait for child process");
        pid_ = -1;
        return status;
    }

private:
    pid_t pid_;
};

void AppendBytes(const std::string& path, const std::string& bytes) {
    std::ofstream output(path, std::ios::binary | std::ios::app);
    output.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    output.close();
    Check(!output.fail(), "Append test bytes");
}

std::string ReadBytes(const std::string& path) {
    std::ifstream input(path, std::ios::binary);
    Check(input.is_open(), "Open test WAL");
    return std::string(std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>());
}

void TestReopenAndOrdering() {
    TestDirectory directory;
    {
        kvstore::Store store(directory.File());
        store.Put("name", "Alice");
        store.Put("name", "Bob");
        store.Put("city", "Vancouver");
        Check(store.Delete("city"), "Delete must report an existing key");
        Check(!store.Delete("missing"), "Deleting a missing key must be allowed");
        Check(store.Get("name") == "Bob", "Current reads must see the latest value");
    }
    {
        kvstore::Store store(directory.File());
        Check(store.Get("name") == "Bob", "Replay must preserve overwrite order");
        Check(!store.Get("city").has_value(), "Replay must preserve deletions");
        store.Put("city", "Beijing");
    }
    kvstore::Store store(directory.File());
    Check(store.Get("city") == "Beijing", "Append after reopening must survive another reopen");
}

void TestEmptyAndBinaryPersistence() {
    TestDirectory directory;
    const std::string key("k\0\xff", 3);
    const std::string value("v\0\x80", 3);
    {
        kvstore::Store store(directory.File());
        store.Put("", "");
        store.Put(key, value);
    }
    kvstore::Store store(directory.File());
    Check(store.Get("").has_value() && store.Get("").value().empty(), "Empty value must survive replay");
    Check(store.Get(key) == value, "Binary strings must survive replay");
}

void TestLargeRecordsAcrossChunks() {
    TestDirectory directory;
    const std::string value(kvstore::kMaxWalPayloadBytes - 1, 'v');
    {
        kvstore::Store store(directory.File());
        store.Put("a", value);
        store.Put("b", value);
        store.Put("end", "last");
    }
    kvstore::Store store(directory.File());
    Check(store.Get("a") == value && store.Get("b") == value, "Records spanning read chunks must replay");
    Check(store.Get("end") == "last", "Replay must continue after large records");
}

void TestRejectedInputDoesNotChangeWal() {
    TestDirectory directory;
    kvstore::Store store(directory.File());
    store.Put("good", "value");
    const auto before = std::filesystem::file_size(directory.File());
    CheckThrows([&] { store.Put("k", std::string(kvstore::kMaxWalPayloadBytes, 'v')); },
                "Oversized input must fail before appending");
    CheckThrows([&] { store.Delete(std::string(kvstore::kMaxWalPayloadBytes + 1, 'k')); },
                "Oversized delete key must fail before appending");
    Check(std::filesystem::file_size(directory.File()) == before, "Rejected input must not change the WAL");
    Check(store.Get("good") == "value", "Validation errors must leave the Store usable");
    store.Put("next", "ok");
}

void TestStrictIncompleteTailPreservesFile() {
    TestDirectory directory;
    {
        kvstore::Store store(directory.File());
        store.Put("name", "Alice");
    }
    const auto partial = kvstore::EncodeWalRecord({kvstore::WalRecordType::Put, "name", "Bob"});
    AppendBytes(directory.File(), partial.substr(0, partial.size() - 2));
    const auto before = ReadBytes(directory.File());
    CheckThrows([&] { kvstore::Store store(directory.File()); }, "Strict recovery must reject an incomplete tail");
    Check(ReadBytes(directory.File()) == before, "Strict recovery must preserve the file");
}

void TestExplicitTailRecoveryAllowsAppend() {
    TestDirectory directory;
    {
        kvstore::Store store(directory.File());
        store.Put("name", "Alice");
    }
    const auto partial = kvstore::EncodeWalRecord({kvstore::WalRecordType::Delete, "name", ""});
    const auto tail_size = partial.size() - 1;
    AppendBytes(directory.File(), partial.substr(0, tail_size));
    {
        kvstore::Store store(directory.File(), kvstore::WalRecoveryMode::TruncateIncompleteTail);
        Check(store.truncated_tail_bytes() == tail_size, "Recovery must report the truncated tail size");
        Check(store.Get("name") == "Alice", "An incomplete deletion must not be applied");
        store.Put("city", "Vancouver");
    }
    kvstore::Store store(directory.File());
    Check(store.Get("name") == "Alice" && store.Get("city") == "Vancouver",
          "New appends must follow the valid prefix, with no partial bytes in between");
}

void TestTailWithoutCompleteRecords() {
    TestDirectory directory;
    AppendBytes(directory.File(), "KVW");
    CheckThrows([&] { kvstore::Store store(directory.File()); }, "A partial first header must fail in strict mode");
    {
        kvstore::Store store(directory.File(), kvstore::WalRecoveryMode::TruncateIncompleteTail);
        Check(store.truncated_tail_bytes() == 3, "Recovery must truncate the partial first header");
        Check(std::filesystem::file_size(directory.File()) == 0, "No complete records means an empty valid prefix");
        store.Put("new", "value");
    }
    kvstore::Store store(directory.File());
    Check(store.Get("new") == "value", "Appending after an empty-prefix repair must work");
}

void TestCorruptionNeverTruncated() {
    TestDirectory directory;
    for (bool add_later_record : {false, true}) {
        const auto path = directory.File(add_later_record ? "middle.wal" : "last.wal");
        {
            kvstore::Store store(path);
            store.Put("good", "value");
        }
        auto damaged = kvstore::EncodeWalRecord({kvstore::WalRecordType::Put, "bad", "value"});
        damaged[13] ^= 1;
        AppendBytes(path, damaged);
        if (add_later_record) {
            AppendBytes(path, kvstore::EncodeWalRecord({kvstore::WalRecordType::Put, "later", "value"}));
        }
        const auto before = ReadBytes(path);
        CheckThrows([&] { kvstore::Store store(path); }, "Strict recovery must reject CRC corruption");
        CheckThrows([&] { kvstore::Store store(path, kvstore::WalRecoveryMode::TruncateIncompleteTail); },
                    "Tail recovery must also reject confirmed corruption");
        Check(ReadBytes(path) == before, "Neither mode may truncate confirmed corruption");
    }
}

void TestSingleOwnerAndRelease() {
    TestDirectory directory;
    {
        kvstore::Store first(directory.File());
        CheckThrows([&] { kvstore::Store second(directory.File()); }, "A second owner must fail to acquire the WAL");
        first.Put("name", "Alice");
    }
    kvstore::Store reopened(directory.File());
    Check(reopened.Get("name") == "Alice", "Destruction must release the lock for reopening");
}

void TestInvalidPathsAndMode() {
    TestDirectory directory;
    CheckThrows([] { kvstore::Store store(""); }, "Empty path must be rejected");
    CheckThrows([] { kvstore::Store store(std::string("bad\0path", 8)); }, "Embedded null in a path must be rejected");
    CheckThrows([&] { kvstore::Store store(directory.File("missing-parent/store.wal")); },
                "A missing parent directory must be reported");
    CheckThrows([&] { kvstore::Store store(directory.File(), static_cast<kvstore::WalRecoveryMode>(99)); },
                "Unknown recovery mode must be rejected");
    Check(!std::filesystem::exists(directory.File()), "Invalid arguments must not create a WAL");
}

void TestKilledWriterRecovery() {
    TestDirectory directory;
    int ready[2];
    Check(::pipe(ready) == 0, "Create child readiness pipe");
    const pid_t pid = ::fork();
    if (pid == -1) {
        ::close(ready[0]);
        ::close(ready[1]);
        throw std::runtime_error("Fork crash-test writer");
    }
    if (pid == 0) {
        ::close(ready[0]);
        try {
            kvstore::Store store(directory.File());
            store.Put("name", "Alice");
            store.Put("name", "Bob");
            store.Put("removed", "value");
            store.Delete("removed");
            const char signal = 'R';
            if (::write(ready[1], &signal, 1) != 1) {
                ::_exit(2);
            }
            for (;;) {
                ::pause();
            }
        } catch (...) {
            ::_exit(2);
        }
    }
    ChildProcess child(pid);
    ::close(ready[1]);
    char signal = 0;
    ssize_t count;
    do {
        count = ::read(ready[0], &signal, 1);
    } while (count == -1 && errno == EINTR);
    ::close(ready[0]);
    Check(count == 1 && signal == 'R', "Writer must acknowledge successful operations before being killed");
    Check(::kill(pid, SIGKILL) == 0, "Kill the writer without destructors");
    const int status = child.Wait();
    Check(WIFSIGNALED(status) && WTERMSIG(status) == SIGKILL, "Writer must terminate through SIGKILL");
    kvstore::Store store(directory.File());
    Check(store.Get("name") == "Bob", "Acknowledged overwrite must survive a killed process");
    Check(!store.Get("removed").has_value(), "Acknowledged deletion must survive a killed process");
}

void TestWriteFailureStopsStore() {
    TestDirectory directory;
    const pid_t pid = ::fork();
    Check(pid != -1, "Fork write-failure test");
    if (pid == 0) {
        try {
            kvstore::Store store(directory.File());
            Check(::signal(SIGXFSZ, SIG_IGN) != SIG_ERR, "Ignore file-size-limit signal");
            struct rlimit limit {};
            Check(::getrlimit(RLIMIT_FSIZE, &limit) == 0, "Read file-size limit");
            limit.rlim_cur = 4;
            Check(::setrlimit(RLIMIT_FSIZE, &limit) == 0, "Limit child file size to four bytes");
            bool failed = false;
            try {
                store.Put("name", "Alice");
            } catch (const std::system_error&) {
                failed = true;
            }
            Check(failed, "A partial append must report the operating-system failure");
            CheckThrows([&] { store.Get("name"); }, "Reads must stop after a failed append");
            CheckThrows([&] { store.Put("next", "value"); }, "Writes must stop after a failed append");
            ::_exit(0);
        } catch (...) {
            ::_exit(2);
        }
    }
    ChildProcess child(pid);
    const int status = child.Wait();
    Check(WIFEXITED(status) && WEXITSTATUS(status) == 0, "Child must verify the failed-write behavior");
    Check(std::filesystem::file_size(directory.File()) == 4, "Failure must have produced a real partial record");
    kvstore::Store repaired(directory.File(), kvstore::WalRecoveryMode::TruncateIncompleteTail);
    Check(repaired.truncated_tail_bytes() == 4, "Explicit recovery must remove the failed append");
    Check(!repaired.Get("name").has_value(), "Partial operation must not be replayed");
    repaired.Put("next", "value");
}

int main() {
    try {
        TestReopenAndOrdering();
        TestEmptyAndBinaryPersistence();
        TestLargeRecordsAcrossChunks();
        TestRejectedInputDoesNotChangeWal();
        TestStrictIncompleteTailPreservesFile();
        TestExplicitTailRecoveryAllowsAppend();
        TestTailWithoutCompleteRecords();
        TestCorruptionNeverTruncated();
        TestSingleOwnerAndRelease();
        TestInvalidPathsAndMode();
        TestKilledWriterRecovery();
        TestWriteFailureStopsStore();
        std::cout << "All 12 Store tests passed.\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "Store test failed: " << error.what() << '\n';
        return 1;
    }
}
