#include "kvstore/sstable.h"
#include "kvstore/wal_record.h"
#include "coding.h"

#include <cerrno>
#include <filesystem>
#include <limits>
#include <stdexcept>
#include <string_view>
#include <system_error>
#include <utility>

#include <fcntl.h>
#include <unistd.h>

namespace kvstore {
namespace {

constexpr std::string_view kMagic = "KVS1";
constexpr std::size_t kTableHeaderSize = 8;
constexpr std::size_t kRecordHeaderSize = 13;

void ValidatePath(const std::string& path) {
    if (path.empty() || path.find('\0') != std::string::npos) {
        throw std::invalid_argument("SSTable path must be nonempty and contain no null bytes");
    }
}

void ThrowSystemError(const char* action) {
    const int error = errno;
    throw std::system_error(error, std::generic_category(), action);
}

using detail::AppendU32;
using detail::ReadU32;

// O_EXCL makes the no-overwrite rule part of file creation itself.
class NewFile {
public:
    explicit NewFile(const std::string& path) {
        fd_ = ::open(path.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
        if (fd_ == -1) {
            ThrowSystemError("Create SSTable");
        }
    }
    ~NewFile() {
        if (fd_ != -1) {
            ::close(fd_);
        }
    }
    NewFile(const NewFile&) = delete;
    NewFile& operator=(const NewFile&) = delete;

    void Write(std::string_view bytes) {
        std::size_t written = 0;
        while (written < bytes.size()) {
            const auto count = ::write(fd_, bytes.data() + written, bytes.size() - written);
            if (count == -1) {
                if (errno == EINTR) {
                    continue;
                }
                ThrowSystemError("Write SSTable");
            }
            if (count == 0) {
                throw std::runtime_error("SSTable write made no progress");
            }
            written += static_cast<std::size_t>(count);
        }
    }

    void Close() {
        const int fd = fd_;
        fd_ = -1;
        if (::close(fd) == -1) {
            ThrowSystemError("Close SSTable");
        }
    }

private:
    int fd_ = -1;
};

void ReadExact(std::istream& input, char* buffer, std::size_t size) {
    input.read(buffer, static_cast<std::streamsize>(size));
    if (!input) {
        throw std::runtime_error("Incomplete SSTable or file read failure");
    }
}

SSTableEntry ReadEntry(std::istream& input) {
    std::string bytes(kRecordHeaderSize, '\0');
    ReadExact(input, bytes.data(), bytes.size());
    // Validate type and lengths BEFORE using them to allocate a payload buffer.
    if (DecodeWalRecord(bytes).status != WalDecodeStatus::Incomplete) {
        throw std::runtime_error("Invalid SSTable entry header");
    }
    const std::size_t remaining = ReadU32(bytes, 5) + ReadU32(bytes, 9) + 4;
    bytes.resize(kRecordHeaderSize + remaining);
    ReadExact(input, bytes.data() + kRecordHeaderSize, remaining);
    auto decoded = DecodeWalRecord(bytes);
    if (decoded.status != WalDecodeStatus::Ok) {
        throw std::runtime_error("Corrupt SSTable entry");
    }
    auto& record = decoded.record.value();
    const auto type = record.type == WalRecordType::Put
                          ? SSTableEntryType::Value : SSTableEntryType::Tombstone;
    return {type, std::move(record.key), std::move(record.value)};
}

void ValidateEntry(const SSTableEntry& entry) {
    if (entry.type != SSTableEntryType::Value && entry.type != SSTableEntryType::Tombstone) {
        throw std::invalid_argument("Unknown SSTable entry type");
    }
    if (entry.type == SSTableEntryType::Tombstone && !entry.value.empty()) {
        throw std::invalid_argument("SSTable tombstones cannot contain a value");
    }
    if (entry.key.size() > kMaxWalPayloadBytes ||
        entry.value.size() > kMaxWalPayloadBytes - entry.key.size()) {
        throw std::length_error("SSTable key + value exceeds 1 MiB");
    }
}

}  // namespace

void WriteSSTable(const std::string& path, const std::vector<SSTableEntry>& entries) {
    ValidatePath(path);
    if (entries.size() > std::numeric_limits<std::uint32_t>::max()) {
        throw std::length_error("Too many SSTable entries");
    }
    // Reject all invalid input before creating a file. Do not silently sort or deduplicate.
    for (std::size_t i = 0; i < entries.size(); ++i) {
        ValidateEntry(entries[i]);
        if (i > 0 && !(entries[i - 1].key < entries[i].key)) {
            throw std::invalid_argument("SSTable keys must be strictly increasing");
        }
    }
    NewFile output(path);
    std::string header(kMagic);
    AppendU32(header, static_cast<std::uint32_t>(entries.size()));
    output.Write(header);
    for (const auto& entry : entries) {
        const auto type = entry.type == SSTableEntryType::Value
                              ? WalRecordType::Put : WalRecordType::Delete;
        output.Write(EncodeWalRecord({type, entry.key, entry.value}));
    }
    output.Close();
}

SSTableReader::SSTableReader(const std::string& path) {
    ValidatePath(path);
    if (!std::filesystem::is_regular_file(path)) {
        throw std::invalid_argument("SSTable must be an existing regular file");
    }
    input_.open(path, std::ios::binary);
    if (!input_.is_open()) {
        throw std::runtime_error("Cannot open SSTable");
    }
    std::string header(kTableHeaderSize, '\0');
    ReadExact(input_, header.data(), header.size());
    if (std::string_view(header).substr(0, kMagic.size()) != kMagic) {
        throw std::runtime_error("Invalid SSTable format/version");
    }
    entry_count_ = ReadU32(header, 4);
    std::string previous_key;
    for (std::uint32_t i = 0; i < entry_count_; ++i) {
        auto entry = ReadEntry(input_);
        if (i > 0 && !(previous_key < entry.key)) {
            throw std::runtime_error("SSTable keys are not strictly increasing");
        }
        previous_key = std::move(entry.key);
    }
    // The count catches even truncation exactly between two complete records.
    // EOF checking also rejects extra records or garbage after the declared entries.
    if (input_.peek() != std::char_traits<char>::eof() || input_.bad()) {
        throw std::runtime_error("Unexpected SSTable trailing bytes or read failure");
    }
}

std::optional<SSTableEntry> SSTableReader::Get(const std::string& key) {
    input_.clear();  // Clear EOF from validation or a previous lookup.
    input_.seekg(static_cast<std::streamoff>(kTableHeaderSize));
    if (!input_) {
        throw std::runtime_error("Cannot seek SSTable");
    }
    for (std::uint32_t i = 0; i < entry_count_; ++i) {
        auto entry = ReadEntry(input_);
        if (entry.key == key) {
            return entry;
        }
        if (key < entry.key) {
            break;  // Sorted keys mean the target cannot appear later.
        }
    }
    return std::nullopt;
}

}  // namespace kvstore
