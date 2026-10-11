#include "kvstore/wal_record.h"

#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>

void Check(bool condition, const std::string& message) {
    if (!condition) {
        throw std::runtime_error(message);
    }
}

void CheckRecord(const kvstore::WalDecodeResult& result,
                 const kvstore::WalRecord& expected, std::size_t expected_size) {
    Check(result.status == kvstore::WalDecodeStatus::Ok, "Record must decode successfully");
    Check(result.record.has_value(), "Successful decoding must return a record");
    Check(result.record->type == expected.type, "Operation must survive encoding");
    Check(result.record->key == expected.key, "Key must survive encoding");
    Check(result.record->value == expected.value, "Value must survive encoding");
    Check(result.bytes_consumed == expected_size, "Decoder must consume exactly one record");
}

void CheckFailure(const kvstore::WalDecodeResult& result, kvstore::WalDecodeStatus status) {
    Check(result.status == status, "Unexpected failure status");
    Check(!result.record.has_value(), "Failure must not return a record");
    Check(result.bytes_consumed == 0, "Failure must not consume bytes");
}

void TestPutRoundTrip() {
    const kvstore::WalRecord record{kvstore::WalRecordType::Put, "name", "Alice"};
    const auto bytes = kvstore::EncodeWalRecord(record);
    CheckRecord(kvstore::DecodeWalRecord(bytes), record, bytes.size());
}

void TestDeleteRoundTrip() {
    const kvstore::WalRecord record{kvstore::WalRecordType::Delete, "name", ""};
    const auto bytes = kvstore::EncodeWalRecord(record);
    CheckRecord(kvstore::DecodeWalRecord(bytes), record, bytes.size());
}

void TestEmptyKeysAndValues() {
    const kvstore::WalRecord put{kvstore::WalRecordType::Put, "", ""};
    const kvstore::WalRecord remove{kvstore::WalRecordType::Delete, "", ""};
    const auto put_bytes = kvstore::EncodeWalRecord(put);
    const auto delete_bytes = kvstore::EncodeWalRecord(remove);
    CheckRecord(kvstore::DecodeWalRecord(put_bytes), put, put_bytes.size());
    CheckRecord(kvstore::DecodeWalRecord(delete_bytes), remove, delete_bytes.size());
    Check(put_bytes != delete_bytes, "Put(empty) and Delete must remain distinct");
}

void TestBinaryStrings() {
    const kvstore::WalRecord record{kvstore::WalRecordType::Put,
                                   std::string("k\0\xff", 3), std::string("v\0\x80", 3)};
    const auto bytes = kvstore::EncodeWalRecord(record);
    CheckRecord(kvstore::DecodeWalRecord(bytes), record, bytes.size());
}

void TestGoldenEncodingAndDecoding() {
    // Independently computed with Python struct.pack('<II', 1, 1) and zlib.crc32.
    const std::string golden("\x4b\x56\x57\x31\x01\x01\x00\x00\x00"
                             "\x01\x00\x00\x00\x6b\x76\x33\x14\x1f\x3d", 19);
    const kvstore::WalRecord record{kvstore::WalRecordType::Put, "k", "v"};
    Check(kvstore::EncodeWalRecord(record) == golden, "Wire bytes must match the independent fixture");
    CheckRecord(kvstore::DecodeWalRecord(golden), record, golden.size());
}

void TestConcatenatedRecords() {
    const kvstore::WalRecord put{kvstore::WalRecordType::Put, "name", "Alice"};
    const kvstore::WalRecord remove{kvstore::WalRecordType::Delete, "name", ""};
    const auto first_bytes = kvstore::EncodeWalRecord(put);
    const auto second_bytes = kvstore::EncodeWalRecord(remove);
    const auto bytes = first_bytes + second_bytes;
    const auto first = kvstore::DecodeWalRecord(bytes);
    CheckRecord(first, put, first_bytes.size());
    const auto second = kvstore::DecodeWalRecord(std::string_view(bytes).substr(first.bytes_consumed));
    CheckRecord(second, remove, second_bytes.size());
}

void TestEveryTruncatedPrefix() {
    const auto bytes = kvstore::EncodeWalRecord({kvstore::WalRecordType::Put, "name", "Alice"});
    for (std::size_t size = 0; size < bytes.size(); ++size) {
        CheckFailure(kvstore::DecodeWalRecord(std::string_view(bytes).substr(0, size)),
                     kvstore::WalDecodeStatus::Incomplete);
    }
}

void TestPayloadAndChecksumDamage() {
    const auto original = kvstore::EncodeWalRecord({kvstore::WalRecordType::Put, "name", "Alice"});
    for (std::size_t offset = 13; offset < original.size(); ++offset) {
        auto damaged = original;
        damaged[offset] ^= 1;
        CheckFailure(kvstore::DecodeWalRecord(damaged), kvstore::WalDecodeStatus::Corrupt);
    }
}

void TestInvalidHeaders() {
    const auto bytes = kvstore::EncodeWalRecord({kvstore::WalRecordType::Put, "k", "v"});
    // Header-only inputs ensure these checks do not depend on a trailing CRC.
    auto header = bytes.substr(0, 13);
    header[0] = 'X';
    CheckFailure(kvstore::DecodeWalRecord(header), kvstore::WalDecodeStatus::Corrupt);
    header = bytes.substr(0, 13);
    header[3] = '2';
    CheckFailure(kvstore::DecodeWalRecord(header), kvstore::WalDecodeStatus::Corrupt);
    header = bytes.substr(0, 13);
    header[4] = 0;
    CheckFailure(kvstore::DecodeWalRecord(header), kvstore::WalDecodeStatus::Corrupt);
    header[4] = static_cast<char>(kvstore::WalRecordType::Delete);
    CheckFailure(kvstore::DecodeWalRecord(header), kvstore::WalDecodeStatus::Corrupt);
    header = bytes.substr(0, 13);
    header.replace(5, 4, 4, static_cast<char>(0xff));
    CheckFailure(kvstore::DecodeWalRecord(header), kvstore::WalDecodeStatus::Corrupt);
    header = bytes.substr(0, 13);
    header[9] = 0;
    header[11] = 0x10;  // value_size = 1 MiB; key_size = 1 exceeds the combined limit.
    CheckFailure(kvstore::DecodeWalRecord(header), kvstore::WalDecodeStatus::Corrupt);
}

void TestPayloadLimit() {
    const kvstore::WalRecord record{kvstore::WalRecordType::Put, "k",
                                   std::string(kvstore::kMaxWalPayloadBytes - 1, 'v')};
    const auto bytes = kvstore::EncodeWalRecord(record);
    CheckRecord(kvstore::DecodeWalRecord(bytes), record, bytes.size());
    bool rejected = false;
    try {
        kvstore::EncodeWalRecord({kvstore::WalRecordType::Put, "kk", record.value});
    } catch (const std::length_error&) {
        rejected = true;
    }
    Check(rejected, "An oversized combined payload must be rejected");
}

void TestInvalidEncoding() {
    bool rejected = false;
    try {
        kvstore::EncodeWalRecord({static_cast<kvstore::WalRecordType>(0), "k", "v"});
    } catch (const std::invalid_argument&) {
        rejected = true;
    }
    Check(rejected, "An unknown operation must be rejected");
    rejected = false;
    try {
        kvstore::EncodeWalRecord({kvstore::WalRecordType::Delete, "k", "v"});
    } catch (const std::invalid_argument&) {
        rejected = true;
    }
    Check(rejected, "Delete with a value must be rejected");
    rejected = false;
    try {
        kvstore::EncodeWalRecord({kvstore::WalRecordType::Put,
                                 std::string(kvstore::kMaxWalPayloadBytes + 1, 'k'), ""});
    } catch (const std::length_error&) {
        rejected = true;
    }
    Check(rejected, "An oversized key must be rejected");
}

void TestDecodedValuesOwnTheirBytes() {
    const kvstore::WalRecord record{kvstore::WalRecordType::Put, "name", "Alice"};
    auto bytes = kvstore::EncodeWalRecord(record);
    const auto result = kvstore::DecodeWalRecord(bytes);
    const auto size = bytes.size();
    bytes.assign(size, 'X');
    CheckRecord(result, record, size);
}

int main() {
    try {
        TestPutRoundTrip();
        TestDeleteRoundTrip();
        TestEmptyKeysAndValues();
        TestBinaryStrings();
        TestGoldenEncodingAndDecoding();
        TestConcatenatedRecords();
        TestEveryTruncatedPrefix();
        TestPayloadAndChecksumDamage();
        TestInvalidHeaders();
        TestPayloadLimit();
        TestInvalidEncoding();
        TestDecodedValuesOwnTheirBytes();
        std::cout << "All 12 WAL record tests passed.\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "WAL record test failed: " << error.what() << '\n';
        return 1;
    }
}
