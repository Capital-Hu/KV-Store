#include "kvstore/manifest.h"
#include "common/coding.h"

#include <functional>
#include <iostream>
#include <stdexcept>

void Check(bool condition, const std::string& message) {
    if (!condition) { throw std::runtime_error(message); }
}

void Reject(const std::function<void()>& action) {
    bool failed = false;
    try { action(); } catch (const std::exception&) { failed = true; }
    Check(failed, "Invalid manifest must be rejected");
}

void TestRoundTripAndOrder() {
    const auto initial = kvstore::DecodeManifest(kvstore::EncodeManifest({}));
    Check(initial.wal_id == 0 && initial.next_file_id == 1 && initial.sstable_ids.empty(), "Initial manifest");
    const kvstore::Manifest expected{8, 9, {3, 1, 7}};
    const auto actual = kvstore::DecodeManifest(kvstore::EncodeManifest(expected));
    Check(actual.wal_id == 8 && actual.next_file_id == 9 && actual.sstable_ids == expected.sstable_ids,
          "Metadata must preserve lookup order, not sort file IDs");
    const auto large_ids = kvstore::DecodeManifest(kvstore::EncodeManifest({1ULL << 40, (1ULL << 40) + 1, {1}}));
    Check(large_ids.wal_id == (1ULL << 40), "File IDs must preserve all 64 bits");
}

void TestGoldenBytes() {
    // Independent Python struct.pack('<QQIQ', 2, 3, 1, 1), then zlib.crc32.
    const std::string golden("\x4b\x56\x4d\x31\x02\x00\x00\x00\x00\x00\x00\x00"
                             "\x03\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00"
                             "\x01\x00\x00\x00\x00\x00\x00\x00", 32);
    auto fixture = golden;
    fixture.append("\xdd\x21\xe4\xa4", 4);
    Check(kvstore::EncodeManifest({2, 3, {1}}) == fixture, "Manifest must match fixed wire bytes");
    Check(kvstore::DecodeManifest(fixture).sstable_ids == std::vector<std::uint64_t>{1}, "Decode fixed fixture");
}

void TestTruncationAndCorruption() {
    const auto bytes = kvstore::EncodeManifest({4, 5, {1, 3}});
    for (std::size_t size = 0; size < bytes.size(); ++size) {
        Reject([&] { kvstore::DecodeManifest(std::string_view(bytes).substr(0, size)); });
    }
    for (std::size_t offset = 0; offset < bytes.size(); ++offset) {
        auto damaged = bytes;
        damaged[offset] ^= 1;
        Reject([&] { kvstore::DecodeManifest(damaged); });
    }
    Reject([&] { kvstore::DecodeManifest(bytes + "X"); });
}

void TestInvalidIdsEvenWithValidChecksum() {
    for (const auto& manifest : std::vector<kvstore::Manifest>{{0, 0, {}}, {3, 3, {}}, {2, 3, {0}},
                                                              {2, 3, {2}}, {2, 3, {3}}, {2, 3, {1, 1}}}) {
        Reject([&] { kvstore::EncodeManifest(manifest); });
    }
    auto bytes = kvstore::EncodeManifest({2, 3, {1}});
    bytes.replace(24, 8, 8, '\0');  // A table ID of zero is invalid even with a valid CRC.
    bytes.resize(bytes.size() - 4);
    kvstore::detail::AppendU32(bytes, kvstore::detail::Crc32(bytes));
    Reject([&] { kvstore::DecodeManifest(bytes); });
}

void TestCountLimit() {
    auto bytes = kvstore::EncodeManifest({});
    bytes.replace(20, 4, 4, static_cast<char>(0xff));
    Reject([&] { kvstore::DecodeManifest(bytes); });
    kvstore::Manifest excessive;
    excessive.sstable_ids.resize(kvstore::kMaxManifestTables + 1, 1);
    Reject([&] { kvstore::EncodeManifest(excessive); });
}

void TestFrozenWalFormatAndValidation() {
    for (const std::uint64_t frozen_id : {0, 2}) {
        const kvstore::Manifest expected{4, 5, {1}, frozen_id};
        const auto bytes = kvstore::EncodeManifest(expected);
        Check(bytes.substr(0, 4) == "KVM2" && bytes.size() == 44, "Two-WAL format has one extra 64-bit field");
        const auto actual = kvstore::DecodeManifest(bytes);
        Check(actual.frozen_wal_id == frozen_id && actual.wal_id == 4 && actual.sstable_ids == expected.sstable_ids,
              "Frozen WAL zero is present, not absent");
        for (std::size_t size = 0; size < bytes.size(); ++size) {
            Reject([&] { kvstore::DecodeManifest(std::string_view(bytes).substr(0, size)); });
        }
        for (std::size_t offset = 0; offset < bytes.size(); ++offset) {
            auto damaged = bytes; damaged[offset] ^= 1;
            Reject([&] { kvstore::DecodeManifest(damaged); });
        }
        Reject([&] { kvstore::DecodeManifest(bytes + "X"); });
    }
    for (const auto& invalid : std::vector<kvstore::Manifest>{{4, 5, {1}, 4}, {4, 5, {1}, 5}, {4, 5, {2}, 2}}) {
        Reject([&] { kvstore::EncodeManifest(invalid); });
    }
    auto bytes = kvstore::EncodeManifest({4, 5, {1}, 2});
    bytes.replace(24, 8, 8, '\0');
    bytes[24] = 4;  // An active/frozen WAL collision is invalid even with a valid CRC.
    bytes.resize(bytes.size() - 4);
    kvstore::detail::AppendU32(bytes, kvstore::detail::Crc32(bytes));
    Reject([&] { kvstore::DecodeManifest(bytes); });
}

int main() {
    try {
        TestRoundTripAndOrder(); TestGoldenBytes(); TestTruncationAndCorruption();
        TestInvalidIdsEvenWithValidChecksum(); TestCountLimit();
        TestFrozenWalFormatAndValidation();
        std::cout << "All 6 manifest tests passed.\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "Manifest test failed: " << error.what() << '\n';
        return 1;
    }
}
