#include "kvstore/sstable.h"
#include "kvstore/wal_record.h"
#include "coding.h"
#include "file_io.h"

#include <algorithm>
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

SSTableEntry ReadEntry(std::istream& input, std::size_t* encoded_size = nullptr) {
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
    if (encoded_size) { *encoded_size = bytes.size(); }
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

SSTableReader::SSTableReader(const std::string& path, bool use_bloom_filter)
    : SSTableReader(path, SSTableReadOptions{use_bloom_filter, true, {}, kMaxSSTableIndexBytes}) {}

SSTableReader::SSTableReader(const std::string& path, SSTableReadOptions options)
    : index_enabled_(options.use_block_index), cache_(std::move(options.block_cache)) {
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
    if (options.use_bloom_filter) { bloom_.emplace(entry_count_); }
    std::string previous_key;
    std::uint64_t offset = kTableHeaderSize, block_offset = offset;
    std::uint32_t block_bytes = 0, first_entry = 0;
    std::size_t index_bytes = 0;
    const auto add_index = [&] {
        if (!index_enabled_ || block_bytes == 0) { return; }
        const auto charge = sizeof(BlockIndexEntry) + previous_key.size();
        if (charge > options.index_budget_bytes || index_bytes > options.index_budget_bytes - charge) {
            // Drop partial metadata and retain the original exact scan as a safe fallback.
            index_enabled_ = false;
            std::vector<BlockIndexEntry>().swap(index_);
            return;
        }
        index_.push_back({previous_key, block_offset, block_bytes, first_entry});
        index_bytes += charge;
    };
    for (std::uint32_t i = 0; i < entry_count_; ++i) {
        std::size_t encoded_size = 0;
        auto entry = ReadEntry(input_, &encoded_size);
        if (i > 0 && !(previous_key < entry.key)) {
            throw std::runtime_error("SSTable keys are not strictly increasing");
        }
        // A tombstone is present in this table and must never be filtered out.
        if (bloom_) { bloom_->Add(entry.key); }
        if (index_enabled_) {
            if (block_bytes != 0 && block_bytes + encoded_size > kSSTableBlockBytes) {
                add_index();
                block_offset = offset;
                block_bytes = 0;
                first_entry = i;
            }
            block_bytes += static_cast<std::uint32_t>(encoded_size);
        }
        offset += encoded_size;
        previous_key = std::move(entry.key);
    }
    add_index();
    data_end_ = offset;
    // The count catches even truncation exactly between two complete records.
    // EOF checking also rejects extra records or garbage after the declared entries.
    if (input_.peek() != std::char_traits<char>::eof() || input_.bad()) {
        throw std::runtime_error("Unexpected SSTable trailing bytes or read failure");
    }
    Rewind();
    if (cache_ && index_enabled_ && !index_.empty()) { cache_table_id_ = cache_->NewTableId(); }
}

SSTableReader::~SSTableReader() {
    if (cache_ && cache_table_id_ != 0) { cache_->EraseTable(cache_table_id_); }
}

void SSTableReader::Seek(std::uint64_t offset) {
    if (offset > static_cast<std::uint64_t>(std::numeric_limits<std::streamoff>::max())) {
        throw std::runtime_error("SSTable offset exceeds stream limits");
    }
    input_.clear();
    input_.seekg(static_cast<std::streamoff>(offset));
    if (!input_) { throw std::runtime_error("Cannot seek SSTable"); }
}

void SSTableReader::Rewind() {
    Seek(kTableHeaderSize);
    entries_read_ = 0;
    next_offset_ = kTableHeaderSize;
    cursor_positioned_ = true;
}

std::optional<SSTableEntry> SSTableReader::Next() {
    if (entries_read_ >= entry_count_) { return std::nullopt; }
    if (!cursor_positioned_) { Seek(next_offset_); cursor_positioned_ = true; }
    std::size_t encoded_size = 0;
    auto entry = ReadEntry(input_, &encoded_size);
    next_offset_ += encoded_size;
    ++entries_read_;
    return entry;
}

std::shared_ptr<const std::string> SSTableReader::ReadBlock(std::size_t block) {
    const auto& meta = index_[block];
    const BlockCacheKey key{cache_table_id_, meta.offset};
    if (cache_) {
        auto cached = cache_->Lookup(key);
        if (cached) { ++read_stats_.cache_hits; return cached; }
        ++read_stats_.cache_misses;
    }
    cursor_positioned_ = false;
    Seek(meta.offset);
    std::string bytes(meta.bytes, '\0');
    ++read_stats_.block_reads;
    ReadExact(input_, bytes.data(), bytes.size());
    read_stats_.block_bytes_read += bytes.size();
    // Validate the entire block before admitting it, even if Get wants its first record.
    std::size_t consumed = 0;
    std::uint32_t count = 0;
    std::string previous;
    while (consumed < bytes.size()) {
        auto decoded = DecodeWalRecord(std::string_view(bytes).substr(consumed));
        if (decoded.status != WalDecodeStatus::Ok ||
            (count != 0 && !(previous < decoded.record->key))) {
            throw std::runtime_error("Corrupt or unsorted SSTable block");
        }
        consumed += decoded.bytes_consumed;
        previous = std::move(decoded.record->key);
        ++count;
        ++read_stats_.block_entries_validated;
    }
    const auto end_entry = block + 1 < index_.size() ? index_[block + 1].first_entry : entry_count_;
    if (count != end_entry - meta.first_entry || previous != meta.last_key) {
        throw std::runtime_error("SSTable block disagrees with validated index");
    }
    auto result = std::make_shared<const std::string>(std::move(bytes));
    if (cache_) { cache_->Insert(key, result); }
    return result;
}

std::optional<SSTableEntry> SSTableReader::GetIndexed(const std::string& key) {
    ++read_stats_.index_lookups;
    const auto found = std::lower_bound(index_.begin(), index_.end(), key,
        [](const BlockIndexEntry& block, const std::string& searched) { return block.last_key < searched; });
    if (found == index_.end()) {
        entries_read_ = entry_count_;
        next_offset_ = data_end_;
        cursor_positioned_ = false;
        return std::nullopt;
    }
    ++read_stats_.table_scans;
    const auto bytes = ReadBlock(static_cast<std::size_t>(found - index_.begin()));
    std::size_t consumed = 0;
    auto ordinal = found->first_entry;
    while (consumed < bytes->size()) {
        auto decoded = DecodeWalRecord(std::string_view(*bytes).substr(consumed));
        if (decoded.status != WalDecodeStatus::Ok) { throw std::runtime_error("Corrupt cached SSTable block"); }
        consumed += decoded.bytes_consumed;
        ++ordinal;
        ++read_stats_.entries_examined;
        // Preserve Get/Next cursor behavior even when a cache hit did not touch input_.
        entries_read_ = ordinal;
        next_offset_ = found->offset + consumed;
        cursor_positioned_ = false;
        auto& record = *decoded.record;
        if (record.key == key) {
            return SSTableEntry{record.type == WalRecordType::Put ? SSTableEntryType::Value : SSTableEntryType::Tombstone,
                                std::move(record.key), std::move(record.value)};
        }
        if (key < record.key) { return std::nullopt; }
    }
    return std::nullopt;
}

std::optional<SSTableEntry> SSTableReader::Get(const std::string& key) {
    ++read_stats_.get_calls;
    if (bloom_ && !bloom_->MayContain(key)) {
        ++read_stats_.bloom_negatives;
        return std::nullopt;
    }
    if (index_enabled_) { return GetIndexed(key); }
    ++read_stats_.table_scans;
    Rewind();
    while (auto entry = Next()) {
        ++read_stats_.entries_examined;
        if (entry->key == key) { return entry; }
        if (key < entry->key) { break; }
    }
    return std::nullopt;
}

}  // namespace kvstore
