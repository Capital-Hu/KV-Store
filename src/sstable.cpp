#include "kvstore/sstable.h"
#include "kvstore/wal_record.h"
#include "coding.h"
#include "file_io.h"

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

using detail::AppendU32;
using detail::ReadU32;

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
    SSTableWriter writer(path);
    for (const auto& entry : entries) { writer.Append(entry); }
    writer.Finish();
}

SSTableWriter::SSTableWriter(const std::string& path) {
    ValidatePath(path);
    fd_ = detail::OpenRegular(path, O_WRONLY | O_CREAT | O_EXCL);
    try {
        std::string header(kMagic);
        AppendU32(header, 0);  // The final count is unknown until the streaming merge ends.
        detail::WriteAll(fd_, header);
    } catch (...) {
        ::close(fd_);
        fd_ = -1;
        throw;
    }
}

SSTableWriter::~SSTableWriter() {
    if (fd_ != -1) { ::close(fd_); }
}

void SSTableWriter::EnsureWritable() const {
    if (fd_ == -1 || !usable_) { throw std::logic_error("SSTable writer is finished or unusable"); }
}

void SSTableWriter::Append(const SSTableEntry& entry) {
    EnsureWritable();
    ValidateEntry(entry);
    if (previous_key_ && !(*previous_key_ < entry.key)) {
        throw std::invalid_argument("SSTable keys must be strictly increasing");
    }
    if (entry_count_ == std::numeric_limits<std::uint32_t>::max()) {
        throw std::length_error("Too many SSTable entries");
    }
    const auto type = entry.type == SSTableEntryType::Value ? WalRecordType::Put : WalRecordType::Delete;
    const auto bytes = EncodeWalRecord({type, entry.key, entry.value});
    auto next_key = entry.key;  // Allocate before writing; committing this key uses a move.
    try {
        detail::WriteAll(fd_, bytes);
        previous_key_ = std::move(next_key);
        ++entry_count_;
    } catch (...) {
        usable_ = false;
        throw;
    }
}

void SSTableWriter::Finish() {
    EnsureWritable();
    try {
        std::string count;
        AppendU32(count, entry_count_);
        if (::lseek(fd_, 4, SEEK_SET) == -1) { detail::ThrowSystemError("Seek SSTable count"); }
        detail::WriteAll(fd_, count);  // Patch only the four count bytes, not any entries.
        const int fd = fd_;
        fd_ = -1;
        if (::close(fd) == -1) { detail::ThrowSystemError("Close SSTable"); }
    } catch (...) {
        usable_ = false;
        throw;
    }
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
    Rewind();
}

void SSTableReader::Rewind() {
    input_.clear();
    input_.seekg(static_cast<std::streamoff>(kTableHeaderSize));
    if (!input_) { throw std::runtime_error("Cannot seek SSTable"); }
    entries_read_ = 0;
}

std::optional<SSTableEntry> SSTableReader::Next() {
    if (entries_read_ >= entry_count_) { return std::nullopt; }
    auto entry = ReadEntry(input_);
    ++entries_read_;
    return entry;
}

std::optional<SSTableEntry> SSTableReader::Get(const std::string& key) {
    Rewind();
    while (auto entry = Next()) {
        if (entry->key == key) { return entry; }
        if (key < entry->key) { break; }
    }
    return std::nullopt;
}

}  // namespace kvstore
