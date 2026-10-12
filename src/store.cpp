#include "kvstore/store.h"
#include "kvstore/wal_record.h"

#include <cerrno>
#include <filesystem>
#include <stdexcept>
#include <system_error>

#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

namespace kvstore {
namespace {

void ThrowSystemError(const char* action) {
    const int error = errno;
    throw std::system_error(error, std::generic_category(), action);
}

void SyncFd(int fd) {
    int result;
    do {
        result = ::fsync(fd);
    } while (result == -1 && errno == EINTR);
    if (result == -1) {
        ThrowSystemError("Synchronize file");
    }
}

void SyncWal(int fd) {
    SyncFd(fd);
#ifdef __APPLE__
    // macOS fsync alone does not request flushing the drive's write cache.
    int result;
    do {
        result = ::fcntl(fd, F_FULLFSYNC);
    } while (result == -1 && errno == EINTR);
    if (result == -1) {
        ThrowSystemError("Flush WAL device cache");
    }
#endif
}

void SyncParentDirectory(const std::string& wal_path) {
    auto parent = std::filesystem::path(wal_path).parent_path();
    if (parent.empty()) {
        parent = ".";
    }
    const int directory_fd = ::open(parent.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (directory_fd == -1) {
        ThrowSystemError("Open WAL parent directory");
    }
    try {
        // Persist the directory entry as well as the file's contents.
        SyncFd(directory_fd);
    } catch (...) {
        ::close(directory_fd);
        throw;
    }
    ::close(directory_fd);
}

}  // namespace

Store::Store(const std::string& wal_path, WalRecoveryMode recovery) {
    if (wal_path.empty() || wal_path.find('\0') != std::string::npos) {
        throw std::invalid_argument("WAL path must be nonempty and contain no null bytes");
    }
    if (recovery != WalRecoveryMode::Strict &&
        recovery != WalRecoveryMode::TruncateIncompleteTail) {
        throw std::invalid_argument("Unknown WAL recovery mode");
    }
    fd_ = ::open(wal_path.c_str(), O_RDWR | O_CREAT | O_APPEND | O_CLOEXEC | O_NOFOLLOW, 0600);
    if (fd_ == -1) {
        ThrowSystemError("Open WAL");
    }
    try {
        struct stat info {};
        if (::fstat(fd_, &info) == -1) {
            ThrowSystemError("Inspect WAL");
        }
        if (!S_ISREG(info.st_mode)) {
            throw std::invalid_argument("WAL must be a regular file");
        }
        int lock_result;
        do {
            lock_result = ::flock(fd_, LOCK_EX | LOCK_NB);
        } while (lock_result == -1 && errno == EINTR);
        if (lock_result == -1) {
            ThrowSystemError("Lock WAL (another Store may already have it open)");
        }
        Replay(recovery);
        SyncWal(fd_);
        SyncParentDirectory(wal_path);
    } catch (...) {
        // A failed constructor does not run Store's destructor.
        ::close(fd_);
        fd_ = -1;
        throw;
    }
}

Store::~Store() {
    // Closing the descriptor also releases its advisory lock.
    if (fd_ != -1) {
        ::close(fd_);
    }
}

void Store::Replay(WalRecoveryMode recovery) {
    std::string pending;
    off_t valid_bytes = 0;
    char chunk[64 * 1024];
    for (;;) {
        const auto count = ::read(fd_, chunk, sizeof(chunk));
        if (count == -1) {
            if (errno == EINTR) {
                continue;
            }
            ThrowSystemError("Read WAL");
        }
        if (count == 0) {
            break;
        }
        pending.append(chunk, static_cast<std::size_t>(count));
        std::size_t consumed = 0;
        while (consumed < pending.size()) {
            const auto result = DecodeWalRecord(std::string_view(pending).substr(consumed));
            if (result.status == WalDecodeStatus::Incomplete) {
                break;  // Keep the bytes and read the next chunk.
            }
            if (result.status == WalDecodeStatus::Corrupt) {
                throw std::runtime_error("Corrupt WAL record at byte " + std::to_string(valid_bytes));
            }
            const auto& record = result.record.value();
            if (record.type == WalRecordType::Put) {
                table_.Put(record.key, record.value);
            } else {
                table_.Delete(record.key);
            }
            consumed += result.bytes_consumed;
            valid_bytes += static_cast<off_t>(result.bytes_consumed);
        }
        pending.erase(0, consumed);
    }

    if (!pending.empty()) {
        if (recovery == WalRecoveryMode::Strict) {
            throw std::runtime_error("Incomplete WAL tail at byte " + std::to_string(valid_bytes));
        }
        // This explicit policy assumes an interrupted append caused the tail.
        // A corrupt length field can also look incomplete; do not repair implicitly.
        int result;
        do {
            result = ::ftruncate(fd_, valid_bytes);
        } while (result == -1 && errno == EINTR);
        if (result == -1) {
            ThrowSystemError("Truncate incomplete WAL tail");
        }
        truncated_tail_bytes_ = pending.size();
    }
}

void Store::EnsureUsable() const {
    if (!usable_) {
        throw std::logic_error("Store is unusable after a write failure; close and reopen it");
    }
}

void Store::AppendAndSync(std::string_view bytes) {
    std::size_t written = 0;
    while (written < bytes.size()) {
        const auto count = ::write(fd_, bytes.data() + written, bytes.size() - written);
        if (count == -1) {
            if (errno == EINTR) {
                continue;
            }
            ThrowSystemError("Append WAL");
        }
        if (count == 0) {
            throw std::runtime_error("WAL write made no progress");
        }
        written += static_cast<std::size_t>(count);
    }
    SyncWal(fd_);
}

void Store::Put(const std::string& key, const std::string& value) {
    EnsureUsable();
    const auto bytes = EncodeWalRecord({WalRecordType::Put, key, value});
    try {
        AppendAndSync(bytes);
        table_.Put(key, value);
    } catch (...) {
        usable_ = false;  // Disk may contain the operation even though it failed to return success.
        throw;
    }
}

std::optional<std::string> Store::Get(const std::string& key) const {
    EnsureUsable();
    return table_.Get(key);
}

bool Store::Delete(const std::string& key) {
    EnsureUsable();
    const auto bytes = EncodeWalRecord({WalRecordType::Delete, key, ""});
    try {
        AppendAndSync(bytes);
        return table_.Delete(key);
    } catch (...) {
        usable_ = false;
        throw;
    }
}

}  // namespace kvstore
