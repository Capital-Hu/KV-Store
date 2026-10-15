#include "kvstore/bloom_filter.h"

#include <algorithm>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

void Check(bool condition, const std::string& message) {
    if (!condition) { throw std::runtime_error(message); }
}

void TestEmptyFilter() {
    for (const std::size_t expected : {0, 1, 64}) {
        kvstore::BloomFilter filter(expected);
        Check(filter.enabled() && filter.byte_size() >= 8, "Small filters have at least 64 bits");
        Check(!filter.MayContain("") && !filter.MayContain("missing"), "Empty filter rejects every key");
    }
}

void TestNoFalseNegatives() {
    for (const std::size_t count : {1, 7, 8, 9, 64, 127, 1024, 5000}) {
        kvstore::BloomFilter filter(count);
        for (std::size_t i = 0; i < count; ++i) { filter.Add("key:" + std::to_string(i)); }
        for (std::size_t i = 0; i < count; ++i) {
            Check(filter.MayContain("key:" + std::to_string(i)), "Every inserted key must remain a possible match");
        }
    }
}

void TestEmptyBinaryAndOwnedBytes() {
    std::string all_bytes;
    for (int byte = 0; byte < 256; ++byte) { all_bytes.push_back(static_cast<char>(byte)); }
    const std::vector<std::string> keys = {"", "a", std::string("a\0b", 3), std::string("\0b", 2),
                                         std::string("\xff\x80", 2), all_bytes, std::string(1024 * 1024, 'x')};
    kvstore::BloomFilter filter(keys.size());
    for (const auto& key : keys) {
        auto temporary = key;
        filter.Add(temporary);
        temporary.assign("changed");  // Add must not retain a string_view into the caller's buffer.
    }
    for (const auto& key : keys) { Check(filter.MayContain(key), "Hash all bytes, including nulls and high bits"); }
}

void TestDuplicatesAndUnderestimatedSize() {
    kvstore::BloomFilter filter(0);
    filter.Add("same");
    std::vector<bool> before;
    for (int i = 0; i < 200; ++i) { before.push_back(filter.MayContain(std::to_string(i))); }
    for (int i = 0; i < 100; ++i) { filter.Add("same"); }
    for (int i = 0; i < 200; ++i) {
        Check(filter.MayContain(std::to_string(i)) == before[i], "Duplicate additions do not change membership hints");
    }
    for (int i = 0; i < 2000; ++i) { filter.Add(std::to_string(i)); }
    for (int i = 0; i < 2000; ++i) {
        Check(filter.MayContain(std::to_string(i)), "Saturation may add false positives but cannot remove inserted keys");
    }
}

void TestBoundedAllocationAndFallback() {
    const auto limit = kvstore::BloomFilter::kMaxBytes * 8 / kvstore::BloomFilter::kBitsPerKey;
    kvstore::BloomFilter maximum(limit);
    Check(maximum.enabled() && maximum.byte_size() == kvstore::BloomFilter::kMaxBytes, "Largest enabled filter respects cap");
    maximum.Add("present");
    Check(maximum.MayContain("present"), "Largest enabled filter remains usable");
    for (const auto size : {limit + 1, std::numeric_limits<std::size_t>::max()}) {
        kvstore::BloomFilter disabled(size);
        disabled.Add("present");
        Check(!disabled.enabled() && disabled.byte_size() == 0, "Oversized counts do not allocate or overflow");
        Check(disabled.MayContain("present") && disabled.MayContain("missing"), "Disabled filter always falls back to exact lookup");
    }
}

void TestDeterministicInsertionOrder() {
    kvstore::BloomFilter forward(100), backward(100);
    for (int i = 0; i < 100; ++i) { forward.Add("key" + std::to_string(i)); }
    for (int i = 99; i >= 0; --i) { backward.Add("key" + std::to_string(i)); }
    for (int i = 0; i < 1000; ++i) {
        const auto key = "key" + std::to_string(i);
        Check(forward.MayContain(key) == backward.MayContain(key), "Insertion order cannot change the bit set");
    }
    auto moved = std::move(forward);
    forward.Add("later");
    Check(!forward.enabled() && forward.MayContain("key0"), "Moved-from filter safely falls back without modulo by zero");
    for (int i = 0; i < 100; ++i) { Check(moved.MayContain("key" + std::to_string(i)), "Move retains inserted keys"); }
}

void TestMeasuredFalsePositiveRate() {
    constexpr int inserted = 10000, probes = 20000;
    kvstore::BloomFilter filter(inserted);
    for (int i = 0; i < inserted; ++i) { filter.Add("present:" + std::to_string(i)); }
    for (int i = 0; i < inserted; ++i) { Check(filter.MayContain("present:" + std::to_string(i)), "No false negatives in sample"); }
    int positives = 0;
    for (int i = 0; i < probes; ++i) { if (filter.MayContain("absent:" + std::to_string(i))) { ++positives; } }
    Check(positives < probes * 3 / 100, "Fixed absent-key sample must have under 3% false positives");
    std::cout << "Bloom sample: " << positives << '/' << probes << " false positives ("
              << 100.0 * positives / probes << "%), " << filter.byte_size() << " bytes.\n";
}

int main() {
    try {
        TestEmptyFilter(); TestNoFalseNegatives(); TestEmptyBinaryAndOwnedBytes();
        TestDuplicatesAndUnderestimatedSize(); TestBoundedAllocationAndFallback();
        TestDeterministicInsertionOrder(); TestMeasuredFalsePositiveRate();
        std::cout << "All 7 Bloom filter tests passed.\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "Bloom filter test failed: " << error.what() << '\n';
        return 1;
    }
}
