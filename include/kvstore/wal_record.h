#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

namespace kvstore {

enum class WalRecordType : std::uint8_t {
    Put = 1,
    Delete = 2,
};

struct WalRecord {
    WalRecordType type;
    std::string key;
    std::string value;  // Delete records must have an empty value.
};

// Bound key + value bytes before allocating decoded strings.
constexpr std::size_t kMaxWalPayloadBytes = 1024 * 1024;

enum class WalDecodeStatus {
    Ok,
    Incomplete,
    Corrupt,
};

struct WalDecodeResult {
    WalDecodeStatus status;
    std::optional<WalRecord> record;
    std::size_t bytes_consumed;  // Nonzero only when status is Ok.
};

// Returns contiguous binary bytes stored in std::string, rather than readable text.
// Layout: KVW1 | type:u8 | key_size:u32 | value_size:u32 | key | value | crc32:u32
// The vertical bars are for illustration only; no separators are encoded.
//
// Byte offsets (zero-based):
//   0..3   : "KVW1", a 4-byte format/version identifier.
//   4      : Operation type, 1 byte; Put = 1, Delete = 2.
//   5..8   : Key length in bytes, a 4-byte unsigned integer.
//   9..12  : Value length in bytes, a 4-byte unsigned integer.
//   13 onward: Raw key bytes followed by raw value bytes.
//   Last 4 bytes: CRC32 of all preceding bytes, excluding the checksum itself.
// All 4-byte integers are little-endian; length 4 is encoded as 04 00 00 00 (hex).
//
// Example: Put("name", "Alice") uses 4 + 1 + 4 + 4 + 4 + 5 + 4 = 26 bytes.
//          [KVW1][01][04 00 00 00][05 00 00 00][name][Alice][4-byte CRC32]
// Fixed overhead is 17 bytes; total size = 17 + key.size() + value.size().
// Delete records must have a zero value length and contain no value bytes.
// Invalid records throw invalid_argument; oversized payloads throw length_error.
std::string EncodeWalRecord(const WalRecord& record);

// Decode only the first record. Failure returns no record and consumes no bytes.
WalDecodeResult DecodeWalRecord(std::string_view bytes);

}  // namespace kvstore
