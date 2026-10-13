#include "kvstore/memtable.h"

#include <iostream>
#include <stdexcept>
#include <string>

void Check(bool condition, const std::string& message) {
    if (!condition) {
        throw std::runtime_error(message);
    }
}

void TestMissingKey() {
    kvstore::MemTable table;
    Check(!table.Get("missing").has_value(), "Missing key must have no value");
    Check(!table.Delete("missing"), "Deleting a missing key must return false");
}

void TestPutAndGet() {
    kvstore::MemTable table;
    table.Put("name", "Alice");
    table.Put("city", "Vancouver");
    Check(table.Get("name") == "Alice", "Put must store the value");
    Check(table.Get("city") == "Vancouver", "Different keys must retain their values");
}

void TestOverwrite() {
    kvstore::MemTable table;
    table.Put("name", "Alice");
    table.Put("name", "Bob");
    Check(table.Get("name") == "Bob", "Put must replace an existing value");
}

void TestEmptyValue() {
    kvstore::MemTable table;
    table.Put("empty", "");
    const auto value = table.Get("empty");
    Check(value.has_value(), "An empty value must still be present");
    Check(value.value().empty(), "An empty value must remain empty");
    Check(!table.Get("missing").has_value(), "Empty and missing must be distinct");
}

void TestDeleteAndReinsert() {
    kvstore::MemTable table;
    table.Put("name", "Alice");
    table.Put("city", "Vancouver");
    Check(table.Delete("name"), "Deleting an existing key must return true");
    Check(!table.Get("name").has_value(), "Deleted key must not be found");
    Check(!table.Delete("name"), "Repeated deletion must return false");
    Check(table.Get("city") == "Vancouver", "Delete must preserve other keys");
    table.Put("name", "Bob");
    Check(table.Get("name") == "Bob", "A deleted key may be inserted again");
}

void TestEmptyKey() {
    kvstore::MemTable table;
    table.Put("", "value");
    Check(table.Get("") == "value", "An empty string is a valid key");
    Check(table.Delete(""), "An empty key may be deleted");
    Check(!table.Get("").has_value(), "Deleted empty key must not be found");
}

void TestReturnedValueIsACopy() {
    kvstore::MemTable table;
    table.Put("name", "Alice");
    const auto original = table.Get("name");
    table.Put("name", "Bob");
    table.Delete("name");
    Check(original == "Alice", "A returned value must outlive later updates and deletion");
}

void TestIndependentInstances() {
    kvstore::MemTable first;
    kvstore::MemTable second;
    first.Put("name", "Alice");
    Check(!second.Get("name").has_value(), "Instances must not share entries");
}

void TestLookupStatesAndOverwrite() {
    kvstore::MemTable table;
    Check(!table.Lookup("missing").has_value(), "Absent entry must have no lookup result");
    table.Put("empty", "");
    const auto empty = table.Lookup("empty");
    Check(empty.has_value() && empty->value.has_value() && empty->value->empty(),
          "Present empty value must remain distinct from a tombstone");
    Check(!table.Delete("missing"), "Deleting an absent entry must return false");
    const auto deleted = table.Lookup("missing");
    Check(deleted.has_value() && deleted->key == "missing" && !deleted->value.has_value(),
          "Even an absent key must acquire a tombstone");
    table.Put("missing", "new");
    Check(table.Get("missing") == "new", "Put must replace a tombstone");
    Check(table.Lookup("missing")->value == "new", "Internal lookup must also see reinsertion");
    Check(table.Delete("missing"), "Deleting a reinserted value must return true");
    Check(!table.Delete("missing"), "Deleting an existing tombstone must return false");
    Check(table.Lookup("missing").has_value(), "Repeated deletion must retain the tombstone");
}

void TestSortedSnapshotIncludesTombstones() {
    kvstore::MemTable table;
    table.Put("z", "old");
    table.Put("a", "");
    table.Put("m", "removed");
    table.Put("z", "latest");
    table.Delete("m");
    table.Delete("never-present");
    const auto snapshot = table.Snapshot();
    Check(snapshot.size() == 4, "Export must contain unique keys, including tombstones");
    Check(snapshot[0].key == "a" && snapshot[0].value == "", "Empty value must export first in sorted order");
    Check(snapshot[1].key == "m" && !snapshot[1].value.has_value(), "Deleted value must export as a tombstone");
    Check(snapshot[2].key == "never-present" && !snapshot[2].value.has_value(), "Missing-key deletion must also export");
    Check(snapshot[3].key == "z" && snapshot[3].value == "latest", "Export must retain only the latest value");
    Check(table.Get("z") == "latest" && table.Lookup("m").has_value(), "Export must not clear or alter the table");
}

void TestSnapshotOwnsItsData() {
    kvstore::MemTable table;
    const std::string key("k\0\xff", 3);
    const std::string value("v\0\x80", 3);
    table.Put(key, value);
    auto snapshot = table.Snapshot();
    const auto lookup = table.Lookup(key);
    table.Put(key, "changed");
    table.Delete(key);
    Check(snapshot.size() == 1 && snapshot[0].key == key && snapshot[0].value == value,
          "Exported binary strings must own their bytes across later updates");
    Check(lookup.has_value() && lookup->value == value, "Lookup result must also own its bytes");
    snapshot[0].value = "caller changed the copy";
    Check(!table.Lookup(key)->value.has_value(), "Changing the exported copy must not affect the table");
}

void TestClearRemovesAllEntries() {
    kvstore::MemTable table;
    table.Put("live", "value");
    table.Delete("deleted");
    const auto snapshot = table.Snapshot();
    table.Clear();
    Check(!table.Lookup("live").has_value() && !table.Lookup("deleted").has_value(),
          "Clear must remove both values and tombstones");
    Check(table.Snapshot().empty(), "A cleared table must export no entries");
    Check(snapshot.size() == 2 && snapshot[0].key == "deleted" && snapshot[1].value == "value",
          "Previously exported data must outlive Clear");
    table.Put("new", "value");
    Check(table.Get("new") == "value", "A cleared table must remain writable");
}

int main() {
    try {
        TestMissingKey();
        TestPutAndGet();
        TestOverwrite();
        TestEmptyValue();
        TestDeleteAndReinsert();
        TestEmptyKey();
        TestReturnedValueIsACopy();
        TestIndependentInstances();
        TestLookupStatesAndOverwrite();
        TestSortedSnapshotIncludesTombstones();
        TestSnapshotOwnsItsData();
        TestClearRemovesAllEntries();
        std::cout << "All 12 MemTable tests passed.\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "MemTable test failed: " << error.what() << '\n';
        return 1;
    }
}
