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
        std::cout << "All 8 MemTable tests passed.\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "MemTable test failed: " << error.what() << '\n';
        return 1;
    }
}
