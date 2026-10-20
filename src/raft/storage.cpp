#include "kvstore/raft/storage.h"
#include "storage_codec.h"
#include "common/coding.h"
#include "common/file_io.h"

#include <algorithm>
#include <cerrno>
#include <filesystem>
#include <stdexcept>
#include <type_traits>
#include <utility>

#include <fcntl.h>
#include <unistd.h>

namespace kvstore::raft {
namespace {
namespace io = kvstore::detail;

std::string ReadUpTo(int fd, std::size_t size) {
    std::string bytes(size, '\0');
    std::size_t consumed = 0;
    while (consumed < size) {
        const auto count = ::read(fd, bytes.data() + consumed, size - consumed);
        if (count == -1) {
            if (errno == EINTR) { continue; }
            io::ThrowSystemError("Read Raft journal");
        }
        if (count == 0) { break; }
        consumed += static_cast<std::size_t>(count);
    }
    bytes.resize(consumed);
    return bytes;
}

void ReserveSuffix(PersistentState& state, LogIndex first_index, std::size_t count) {
    const auto needed = static_cast<std::size_t>(first_index - 1) + count;
    if (needed <= state.log.capacity()) { return; }
    const auto capacity = state.log.capacity();
    const auto grown = capacity > kMaxRaftEntries / 2 ? kMaxRaftEntries : std::max(std::size_t{1}, capacity * 2);
    state.log.reserve(std::max(needed, grown));
}

void InstallSuffix(PersistentState& state, LogIndex first_index, std::vector<LogEntry>& entries) {
    static_assert(std::is_nothrow_move_constructible_v<LogEntry>);
    // The caller reserves space before touching durable state; these moves cannot allocate.
    state.log.resize(static_cast<std::size_t>(first_index - 1));
    for (auto& entry : entries) { state.log.push_back(std::move(entry)); }
}
}  // namespace

Storage::Storage(const std::string& path, NodeConfig config, StorageOpenMode mode, StorageRecoveryMode recovery)
    : config_(std::move(config)) {
    detail::ValidateConfig(config_);
    if (path.empty() || path.find('\0') != std::string::npos) { throw std::invalid_argument("Invalid Raft storage path"); }
    if (mode != StorageOpenMode::CreateNew && mode != StorageOpenMode::OpenExisting) {
        throw std::invalid_argument("Unknown Raft storage open mode");
    }
    if (recovery != StorageRecoveryMode::Strict && recovery != StorageRecoveryMode::TruncateIncompleteTail) {
        throw std::invalid_argument("Unknown Raft recovery mode");
    }
    path_ = std::filesystem::absolute(path).string();
    try {
        const auto flags = mode == StorageOpenMode::CreateNew ? O_RDWR | O_CREAT | O_EXCL : O_RDWR;
        lock_fd_ = io::OpenRegular(path_ + ".lock", flags);
        io::LockExclusive(lock_fd_);
        if (mode == StorageOpenMode::CreateNew) {
            if (io::PathExists(path_)) { throw std::runtime_error("Raft storage already exists"); }
            io::OwnedFd temporary(io::OpenRegular(path_ + ".tmp", O_RDWR | O_APPEND | O_CREAT | O_EXCL));
            io::WriteAll(temporary.get(), detail::EncodeFileHeader(config_));
            io::SyncFile(temporary.get());
            if (::rename((path_ + ".tmp").c_str(), path_.c_str()) == -1) { io::ThrowSystemError("Publish new Raft journal"); }
            io::SyncParentDirectory(path_);
            fd_ = temporary.Release();
        } else {
            fd_ = io::OpenRegular(path_, O_RDWR | O_APPEND);
            Load(recovery);
            // Complete records left by an unacknowledged write must be durable before reuse.
            io::SyncFile(fd_);
            io::SyncParentDirectory(path_);
        }
    } catch (...) {
        if (fd_ != -1) { ::close(fd_); }
        if (lock_fd_ != -1) { ::close(lock_fd_); }
        throw;
    }
}

Storage::~Storage() {
    if (fd_ != -1) { ::close(fd_); }
    if (lock_fd_ != -1) { ::close(lock_fd_); }
}

void Storage::EnsureUsable() const {
    if (!usable_) { throw std::runtime_error("Raft storage failed; close and reopen before reuse"); }
}

const PersistentState& Storage::state() const { EnsureUsable(); return state_; }

void Storage::Notify(Stage stage) { if (hook_) { hook_(stage); } }

void Storage::AppendFrame(std::string_view frame) {
    io::WriteAll(fd_, frame);
    Notify(Stage::RecordWritten);
    io::SyncFile(fd_);
    Notify(Stage::RecordSynced);
}

void Storage::SetTermAndVote(Term term, std::optional<NodeId> voted_for) {
    EnsureUsable();
    detail::ValidateTermAndVote(state_, config_, term, voted_for);
    if (term == state_.current_term && voted_for == state_.voted_for) { return; }
    detail::Change change;
    change.term = term; change.voted_for = voted_for;
    const auto frame = detail::EncodeFrame(detail::EncodeChange(change));
    try {
        AppendFrame(frame);
        state_.current_term = term;
        state_.voted_for = voted_for;
        Notify(Stage::StateUpdated);
    } catch (...) { usable_ = false; throw; }
}

void Storage::Append(const std::vector<LogEntry>& entries) {
    EnsureUsable();
    ReplaceSuffix(state_.log.size() + 1, entries);
}

void Storage::ReplaceSuffix(LogIndex first_index, const std::vector<LogEntry>& entries) {
    EnsureUsable();
    const auto bytes = detail::ValidateSuffix(state_, log_bytes_, first_index, entries);
    const auto prefix = static_cast<std::size_t>(first_index - 1);
    if (entries.size() == state_.log.size() - prefix &&
        std::equal(entries.begin(), entries.end(), state_.log.begin() + prefix, detail::SameEntry)) { return; }
    detail::Change change;
    change.type = detail::ChangeType::Suffix;
    change.first_index = first_index;
    change.entries = entries;
    const auto frame = detail::EncodeFrame(detail::EncodeChange(change));
    ReserveSuffix(state_, first_index, entries.size());
    try {
        AppendFrame(frame);
        InstallSuffix(state_, first_index, change.entries);
        log_bytes_ = bytes;
        Notify(Stage::StateUpdated);
    } catch (...) { usable_ = false; throw; }
}

void Storage::Load(StorageRecoveryMode recovery) {
    detail::ValidateFileHeader(ReadUpTo(fd_, detail::kFileHeaderBytes), config_);
    off_t valid_offset = detail::kFileHeaderBytes;
    for (;;) {
        const auto header = ReadUpTo(fd_, detail::kFrameHeaderBytes);
        if (header.empty()) { break; }
        std::string body;
        bool incomplete = header.size() != detail::kFrameHeaderBytes;
        if (incomplete) { detail::ValidatePartialFrameHeader(header); }
        else {
            const auto size = detail::FramePayloadBytes(header);
            body = ReadUpTo(fd_, size + detail::kFrameTrailerBytes);
            incomplete = body.size() != size + detail::kFrameTrailerBytes;
            if (!incomplete) {
                const auto payload = std::string_view(body).substr(0, size);
                if (kvstore::detail::ReadU32(body, size) != kvstore::detail::Crc32(payload)) {
                    throw std::runtime_error("Raft record payload checksum mismatch");
                }
                auto change = detail::DecodeChange(payload);
                if (change.type == detail::ChangeType::TermAndVote) {
                    detail::ValidateTermAndVote(state_, config_, change.term, change.voted_for);
                    state_.current_term = change.term;
                    state_.voted_for = change.voted_for;
                } else {
                    const auto bytes = detail::ValidateSuffix(state_, log_bytes_, change.first_index, change.entries);
                    ReserveSuffix(state_, change.first_index, change.entries.size());
                    InstallSuffix(state_, change.first_index, change.entries);
                    log_bytes_ = bytes;
                }
                valid_offset += static_cast<off_t>(header.size() + body.size());
            }
        }
        if (incomplete) {
            if (recovery != StorageRecoveryMode::TruncateIncompleteTail) {
                throw std::runtime_error("Incomplete Raft journal tail");
            }
            truncated_tail_bytes_ = header.size() + body.size();
            if (::ftruncate(fd_, valid_offset) == -1) { io::ThrowSystemError("Truncate incomplete Raft journal tail"); }
            io::SyncFile(fd_);
            break;
        }
    }
}

}  // namespace kvstore::raft
