#include "file_io.h"

#include <cerrno>
#include <filesystem>
#include <stdexcept>
#include <system_error>

#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

namespace kvstore::detail {

OwnedFd::~OwnedFd() { if (fd_ != -1) { ::close(fd_); } }

[[noreturn]] void ThrowSystemError(const char* action) {
    const int error = errno;
    throw std::system_error(error, std::generic_category(), action);
}

int OpenRegular(const std::string& path, int flags) {
    OwnedFd fd(::open(path.c_str(), flags | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK, 0600));
    if (fd.get() == -1) { ThrowSystemError("Open store file"); }
    struct stat info {};
    if (::fstat(fd.get(), &info) == -1) { ThrowSystemError("Inspect store file"); }
    if (!S_ISREG(info.st_mode)) { throw std::invalid_argument("Store file must be regular"); }
    return fd.Release();
}

void LockExclusive(int fd) {
    int result;
    do { result = ::flock(fd, LOCK_EX | LOCK_NB); } while (result == -1 && errno == EINTR);
    if (result == -1) { ThrowSystemError("Lock store (another owner may have it open)"); }
}

bool PathExists(const std::string& path) {
    struct stat info {};
    if (::lstat(path.c_str(), &info) == 0) { return true; }
    if (errno == ENOENT) { return false; }
    ThrowSystemError("Inspect store path");
}

void WriteAll(int fd, std::string_view bytes) {
    std::size_t written = 0;
    while (written < bytes.size()) {
        const auto count = ::write(fd, bytes.data() + written, bytes.size() - written);
        if (count == -1) {
            if (errno == EINTR) { continue; }
            ThrowSystemError("Write store file");
        }
        if (count == 0) { throw std::runtime_error("File write made no progress"); }
        written += static_cast<std::size_t>(count);
    }
}

std::string ReadFile(const std::string& path, std::size_t max_bytes) {
    OwnedFd fd(OpenRegular(path, O_RDONLY));
    std::string bytes;
    char chunk[4096];
    for (;;) {
        const auto count = ::read(fd.get(), chunk, sizeof(chunk));
        if (count == -1) {
            if (errno == EINTR) { continue; }
            ThrowSystemError("Read store file");
        }
        if (count == 0) { return bytes; }
        if (static_cast<std::size_t>(count) > max_bytes - bytes.size()) {
            throw std::runtime_error("Store metadata exceeds size limit");
        }
        bytes.append(chunk, static_cast<std::size_t>(count));
    }
}

static void SyncFd(int fd) {
    int result;
    do { result = ::fsync(fd); } while (result == -1 && errno == EINTR);
    if (result == -1) { ThrowSystemError("Synchronize file or directory"); }
}

void SyncFile(int fd) {
    SyncFd(fd);
#ifdef __APPLE__
    int result;
    do { result = ::fcntl(fd, F_FULLFSYNC); } while (result == -1 && errno == EINTR);
    if (result == -1) { ThrowSystemError("Flush device cache"); }
#endif
}

void SyncFile(const std::string& path) {
    OwnedFd fd(OpenRegular(path, O_RDONLY));
    SyncFile(fd.get());
}

void SyncParentDirectory(const std::string& path) {
    auto parent = std::filesystem::path(path).parent_path();
    if (parent.empty()) { parent = "."; }
    OwnedFd fd(::open(parent.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC));
    if (fd.get() == -1) { ThrowSystemError("Open parent directory"); }
    SyncFd(fd.get());
}

void RemoveFile(const std::string& path) {
    if (::unlink(path.c_str()) == -1 && errno != ENOENT) { ThrowSystemError("Remove obsolete store file"); }
}

}  // namespace kvstore::detail
