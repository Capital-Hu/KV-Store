#include "kvstore/wal_record.h"
#include "coding.h"

#include <stdexcept>

namespace kvstore {
namespace {

constexpr std::string_view kMagic = "KVW1";
constexpr std::size_t kHeaderSize = 13;
constexpr std::size_t kChecksumSize = 4;

using detail::AppendU32;
using detail::ReadU32;
using detail::Crc32;

}  // namespace

std::string EncodeWalRecord(const WalRecord& record) {
    if (record.type != WalRecordType::Put && record.type != WalRecordType::Delete) {
        throw std::invalid_argument("Unknown WAL operation");
    }
    if (record.type == WalRecordType::Delete && !record.value.empty()) {
        throw std::invalid_argument("Delete records cannot contain a value");
    }
    if (record.key.size() > kMaxWalPayloadBytes ||
        record.value.size() > kMaxWalPayloadBytes - record.key.size()) {
        throw std::length_error("WAL key + value exceeds 1 MiB");
    }

    // Append each field to the same string in order to form one complete record.
    std::string bytes(kMagic);  // 1. Format identifier "KVW1", 4 bytes.
    // 2. Operation type, 1 byte; Put is the numeric value 1, not the character '1'.
    bytes.push_back(static_cast<char>(record.type));
    // 3. Two 4-byte lengths tell the decoder where the key and value end.
    AppendU32(bytes, static_cast<std::uint32_t>(record.key.size()));
    AppendU32(bytes, static_cast<std::uint32_t>(record.value.size()));
    // 4. Append raw content without separators or terminators; preserve embedded '\0'.
    bytes.append(record.key);
    bytes.append(record.value);
    // 5. Compute the checksum over the header and content, then append it.
    const auto checksum = Crc32(bytes);
    AppendU32(bytes, checksum);
    return bytes;
}

WalDecodeResult DecodeWalRecord(std::string_view bytes) {
    if (bytes.size() < kHeaderSize) {
        return {WalDecodeStatus::Incomplete, std::nullopt, 0};
    }
    if (bytes.substr(0, kMagic.size()) != kMagic) {
        return {WalDecodeStatus::Corrupt, std::nullopt, 0};
    }

    const auto type = static_cast<WalRecordType>(static_cast<unsigned char>(bytes[4]));
    if (type != WalRecordType::Put && type != WalRecordType::Delete) {
        return {WalDecodeStatus::Corrupt, std::nullopt, 0};
    }
    const auto key_size = ReadU32(bytes, 5);
    const auto value_size = ReadU32(bytes, 9);
    if (key_size > kMaxWalPayloadBytes ||
        value_size > kMaxWalPayloadBytes - key_size ||
        (type == WalRecordType::Delete && value_size != 0)) {
        return {WalDecodeStatus::Corrupt, std::nullopt, 0};
    }

    const std::size_t record_size = kHeaderSize + key_size + value_size + kChecksumSize;
    if (bytes.size() < record_size) {
        return {WalDecodeStatus::Incomplete, std::nullopt, 0};
    }
    const auto expected_crc = ReadU32(bytes, record_size - kChecksumSize);
    if (Crc32(bytes.substr(0, record_size - kChecksumSize)) != expected_crc) {
        return {WalDecodeStatus::Corrupt, std::nullopt, 0};
    }

    return {WalDecodeStatus::Ok,
            WalRecord{type,
                      std::string(bytes.substr(kHeaderSize, key_size)),
                      std::string(bytes.substr(kHeaderSize + key_size, value_size))},
            record_size};
}

}  // namespace kvstore
