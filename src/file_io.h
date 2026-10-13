#pragma once

#include <cstddef>
#include <string>
#include <string_view>

namespace kvstore::detail {

// Scoped ownership for temporary descriptors; Store owns its long-lived descriptors.
class OwnedFd {
public:
    explicit OwnedFd(int fd) : fd_(fd) {}
    ~OwnedFd();
    OwnedFd(const OwnedFd&) = delete;
    OwnedFd& operator=(const OwnedFd&) = delete;
    int get() const { return fd_; }
    int Release() { const int fd = fd_; fd_ = -1; return fd; }
private:
    int fd_;
};

[[noreturn]] void ThrowSystemError(const char* action);
int OpenRegular(const std::string& path, int flags);
void LockExclusive(int fd);
bool PathExists(const std::string& path);
void WriteAll(int fd, std::string_view bytes);
std::string ReadFile(const std::string& path, std::size_t max_bytes);
void SyncFile(int fd);
void SyncFile(const std::string& path);
void SyncParentDirectory(const std::string& path);
void RemoveFile(const std::string& path);

}  // namespace kvstore::detail
