#include "kvstore/sstable.h"
#include "kvstore/wal_record.h"

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <iterator>
#include <stdexcept>
#include <string>
#include <vector>

#include <unistd.h>

using kvstore::SSTableEntryType;

void Check(bool condition, const std::string& message) {
    if (!condition) {
        throw std::runtime_error(message);
    }
}

void CheckThrows(const std::function<void()>& action, const std::string& message) {
    bool rejected = false;
    try {
        action();
    } catch (const std::exception&) {
        rejected = true;
    }
    Check(rejected, message);
}

class TestDirectory {
public:
    TestDirectory() {
        char pattern[] = "/tmp/kvstore-sstable-XXXXXX";
        const char* path = ::mkdtemp(pattern);
        Check(path != nullptr, "Create test directory");
        path_ = path;
    }
    ~TestDirectory() {
        std::error_code ignored;
        std::filesystem::remove_all(path_, ignored);
    }
    std::string File(const std::string& name = "table.sst") const {
        return (path_ / name).string();
    }

private:
    std::filesystem::path path_;
};

void WriteBytes(const std::string& path, const std::string& bytes) {
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    output.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    output.close();
    Check(!output.fail(), "Write test bytes");
}

std::string ReadBytes(const std::string& path) {
    std::ifstream input(path, std::ios::binary);
    Check(input.is_open(), "Open test file");
    return std::string(std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>());
}

void CheckValue(kvstore::SSTableReader& reader, const std::string& key, const std::string& value) {
    const auto result = reader.Get(key);
    Check(result.has_value(), "Expected key must be present");
    Check(result->type == SSTableEntryType::Value, "Expected a value, not a tombstone");
    Check(result->key == key && result->value == value, "Key and value must survive file encoding");
}

void TestSortedLookupAndRepeatedReads() {
    TestDirectory directory;
    kvstore::WriteSSTable(directory.File(), {
        {SSTableEntryType::Value, "b", "first"},
        {SSTableEntryType::Value, "d", "middle"},
        {SSTableEntryType::Value, "z", "last"},
    });
    kvstore::SSTableReader reader(directory.File());
    Check(reader.size() == 3, "Reader must report the entry count");
    CheckValue(reader, "z", "last");
    CheckValue(reader, "b", "first");
    CheckValue(reader, "d", "middle");
    for (const auto& key : {"a", "c", "zz"}) {
        Check(!reader.Get(key).has_value(), "Missing keys before, between and after entries must stay absent");
    }
    CheckValue(reader, "b", "first");
}

void TestEmptyTable() {
    TestDirectory directory;
    kvstore::WriteSSTable(directory.File(), {});
    Check(std::filesystem::file_size(directory.File()) == 8, "Empty tables still need a complete header");
    kvstore::SSTableReader reader(directory.File());
    Check(reader.size() == 0, "Empty table count must be zero");
    Check(!reader.Get("").has_value() && !reader.Get("missing").has_value(), "Empty table has no keys");
}

void TestEmptyAndBinaryStrings() {
    TestDirectory directory;
    const std::string binary_key("\0k", 2);
    const std::string binary_value("v\0\xff", 3);
    const std::string high_key("\x80", 1);
    kvstore::WriteSSTable(directory.File(), {
        {SSTableEntryType::Value, "", ""},
        {SSTableEntryType::Value, binary_key, binary_value},
        {SSTableEntryType::Value, "name", "Alice"},
        {SSTableEntryType::Value, high_key, "high byte"},
    });
    kvstore::SSTableReader reader(directory.File());
    CheckValue(reader, "", "");
    CheckValue(reader, binary_key, binary_value);
    CheckValue(reader, high_key, "high byte");
}

void TestTombstoneIsNotMissingOrEmpty() {
    TestDirectory directory;
    kvstore::WriteSSTable(directory.File(), {
        {SSTableEntryType::Tombstone, "deleted", ""},
        {SSTableEntryType::Value, "empty", ""},
    });
    kvstore::SSTableReader reader(directory.File());
    Check(reader.size() == 2, "Tombstones must count as entries");
    const auto deleted = reader.Get("deleted");
    Check(deleted.has_value() && deleted->type == SSTableEntryType::Tombstone,
          "Tombstone must remain distinguishable from a missing key");
    Check(deleted->value.empty(), "Tombstone must have no value bytes");
    CheckValue(reader, "empty", "");
    Check(!reader.Get("missing").has_value(), "Missing key must return nullopt");
}

void TestUnsortedAndDuplicateInput() {
    TestDirectory directory;
    CheckThrows([&] {
        kvstore::WriteSSTable(directory.File(), {
            {SSTableEntryType::Value, "z", "last"},
            {SSTableEntryType::Value, "a", "first"},
        });
    }, "Unsorted input must be rejected");
    Check(!std::filesystem::exists(directory.File()), "Unsorted input must not create a file");
    CheckThrows([&] {
        kvstore::WriteSSTable(directory.File(), {
            {SSTableEntryType::Value, "a", "old"},
            {SSTableEntryType::Tombstone, "a", ""},
        });
    }, "Duplicate keys must be rejected rather than silently resolved");
    Check(!std::filesystem::exists(directory.File()), "Duplicate keys must not create a file");
}

void TestInvalidEntriesLeaveNoFile() {
    TestDirectory directory;
    const std::vector<kvstore::SSTableEntry> invalid = {
        {static_cast<SSTableEntryType>(99), "bad", ""},
        {SSTableEntryType::Tombstone, "bad", "unexpected value"},
        {SSTableEntryType::Value, "k", std::string(kvstore::kMaxWalPayloadBytes, 'v')},
        {SSTableEntryType::Tombstone, std::string(kvstore::kMaxWalPayloadBytes + 1, 'k'), ""},
    };
    for (const auto& entry : invalid) {
        CheckThrows([&] { kvstore::WriteSSTable(directory.File(), {entry}); }, "Invalid entry must be rejected");
        Check(!std::filesystem::exists(directory.File()), "Entry validation must happen before file creation");
    }
    CheckThrows([&] {
        kvstore::WriteSSTable(directory.File(), {
            {SSTableEntryType::Value, "a", "valid"}, invalid[1],
        });
    }, "An invalid later entry must also be rejected before file creation");
    Check(!std::filesystem::exists(directory.File()), "Validation must cover all entries before writing");
}

void TestMaximumPayload() {
    TestDirectory directory;
    const std::string value(kvstore::kMaxWalPayloadBytes - 1, 'v');
    kvstore::WriteSSTable(directory.File(), {
        {SSTableEntryType::Value, "a", value},
        {SSTableEntryType::Value, "b", value},
        {SSTableEntryType::Value, "end", "last"},
    });
    kvstore::SSTableReader reader(directory.File());
    CheckValue(reader, "b", value);
    CheckValue(reader, "a", value);
    CheckValue(reader, "end", "last");
}

void TestGoldenFile() {
    TestDirectory directory;
    // KVS1, count = 1, then the independently computed Put("k", "v") fixture.
    const std::string golden("\x4b\x56\x53\x31\x01\x00\x00\x00"
                             "\x4b\x56\x57\x31\x01\x01\x00\x00\x00"
                             "\x01\x00\x00\x00\x6b\x76\x33\x14\x1f\x3d", 27);
    kvstore::WriteSSTable(directory.File(), {{SSTableEntryType::Value, "k", "v"}});
    Check(ReadBytes(directory.File()) == golden, "SSTable bytes must match the independent fixture");
    WriteBytes(directory.File("fixture.sst"), golden);
    kvstore::SSTableReader reader(directory.File("fixture.sst"));
    CheckValue(reader, "k", "v");
}

void TestEveryTruncatedPrefix() {
    TestDirectory directory;
    kvstore::WriteSSTable(directory.File(), {
        {SSTableEntryType::Value, "a", "first"},
        {SSTableEntryType::Value, "b", "second"},
    });
    const auto bytes = ReadBytes(directory.File());
    for (std::size_t size = 0; size < bytes.size(); ++size) {
        const auto prefix = bytes.substr(0, size);
        WriteBytes(directory.File("broken.sst"), prefix);
        CheckThrows([&] { kvstore::SSTableReader reader(directory.File("broken.sst")); },
                    "Every truncated prefix, including record boundaries, must be rejected");
        Check(ReadBytes(directory.File("broken.sst")) == prefix, "Reader must never repair or change a file");
    }
}

void TestCorruptionAndTrailingBytes() {
    TestDirectory directory;
    kvstore::WriteSSTable(directory.File(), {{SSTableEntryType::Value, "k", "v"}});
    const auto bytes = ReadBytes(directory.File());
    // Cover magic/version, entry count, record header, content and CRC bytes.
    for (std::size_t offset = 0; offset < bytes.size(); ++offset) {
        auto damaged = bytes;
        damaged[offset] ^= 1;
        WriteBytes(directory.File("broken.sst"), damaged);
        CheckThrows([&] { kvstore::SSTableReader reader(directory.File("broken.sst")); },
                    "Damaged SSTable must be rejected during opening");
        Check(ReadBytes(directory.File("broken.sst")) == damaged, "Corruption must not trigger truncation");
    }
    for (const auto& extra : {std::string("X"), kvstore::EncodeWalRecord({kvstore::WalRecordType::Put, "z", "last"})}) {
        WriteBytes(directory.File("broken.sst"), bytes + extra);
        CheckThrows([&] { kvstore::SSTableReader reader(directory.File("broken.sst")); },
                    "Trailing garbage and undeclared complete entries must be rejected");
    }
    auto excessive_length = bytes;
    excessive_length.replace(8 + 5, 4, 4, static_cast<char>(0xff));
    WriteBytes(directory.File("broken.sst"), excessive_length);
    CheckThrows([&] { kvstore::SSTableReader reader(directory.File("broken.sst")); },
                "Invalid payload lengths must be rejected before allocating payload bytes");
}

void TestInvalidOrderingOnDisk() {
    TestDirectory directory;
    const std::string header("KVS1\x02\0\0\0", 8);
    for (const auto& second_key : {"a", "z"}) {
        // Both records have valid CRCs; only their key order is invalid.
        const auto bytes = header + kvstore::EncodeWalRecord({kvstore::WalRecordType::Put, "z", "old"}) +
                           kvstore::EncodeWalRecord({kvstore::WalRecordType::Delete, second_key, ""});
        WriteBytes(directory.File(), bytes);
        CheckThrows([&] { kvstore::SSTableReader reader(directory.File()); },
                    "Reader must independently reject unsorted or duplicate keys");
    }
}

void TestExistingFileAndInvalidPaths() {
    TestDirectory directory;
    kvstore::WriteSSTable(directory.File(), {{SSTableEntryType::Value, "name", "Alice"}});
    const auto before = ReadBytes(directory.File());
    CheckThrows([&] { kvstore::WriteSSTable(directory.File(), {}); }, "Existing files must never be overwritten");
    Check(ReadBytes(directory.File()) == before, "Rejected overwrite must preserve every byte");
    CheckThrows([&] { kvstore::WriteSSTable("", {}); }, "Empty output path must be rejected");
    CheckThrows([&] { kvstore::WriteSSTable(std::string("bad\0path", 8), {}); }, "Null in output path must be rejected");
    CheckThrows([&] { kvstore::WriteSSTable(directory.File("missing-parent/table.sst"), {}); },
                "Missing parent directory must be reported");
    CheckThrows([&] { kvstore::SSTableReader reader(""); }, "Empty input path must be rejected");
    CheckThrows([&] { kvstore::SSTableReader reader(std::string("bad\0path", 8)); }, "Null in input path must be rejected");
    CheckThrows([&] { kvstore::SSTableReader reader(directory.File("missing.sst")); }, "Missing input must be rejected");
    CheckThrows([&] { kvstore::SSTableReader reader(directory.File(".")); }, "Directory input must be rejected");
    kvstore::SSTableReader reader(directory.File());
    CheckValue(reader, "name", "Alice");
}

int main() {
    try {
        TestSortedLookupAndRepeatedReads();
        TestEmptyTable();
        TestEmptyAndBinaryStrings();
        TestTombstoneIsNotMissingOrEmpty();
        TestUnsortedAndDuplicateInput();
        TestInvalidEntriesLeaveNoFile();
        TestMaximumPayload();
        TestGoldenFile();
        TestEveryTruncatedPrefix();
        TestCorruptionAndTrailingBytes();
        TestInvalidOrderingOnDisk();
        TestExistingFileAndInvalidPaths();
        std::cout << "All 12 SSTable tests passed.\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "SSTable test failed: " << error.what() << '\n';
        return 1;
    }
}
