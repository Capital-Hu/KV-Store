#include "kvstore/bloom_filter.h"

#include <algorithm>
#include <utility>

namespace kvstore {
namespace {

// Fixed unsigned-byte hashing keeps binary keys and test measurements reproducible.
std::uint64_t Mix(std::uint64_t value) {
    value ^= value >> 30;
    value *= 0xbf58476d1ce4e5b9ULL;
    value ^= value >> 27;
    value *= 0x94d049bb133111ebULL;
    return value ^ (value >> 31);
}

std::pair<std::uint64_t, std::uint64_t> Hashes(std::string_view key) {
    std::uint64_t hash = 14695981039346656037ULL;
    for (unsigned char byte : key) {
        hash ^= byte;
        hash *= 1099511628211ULL;
    }
    // An odd step cannot be zero modulo the byte-aligned (multiple-of-eight) bit count.
    return {Mix(hash), Mix(hash ^ 0x9e3779b97f4a7c15ULL) | 1ULL};
}

}  // namespace

BloomFilter::BloomFilter(std::size_t expected_keys) {
    // Check before multiplication, including adversarial counts from a file header.
    if (expected_keys > kMaxBytes * 8 / kBitsPerKey) {
        enabled_ = false;
        return;
    }
    const auto bytes = std::max<std::size_t>(8, (expected_keys * kBitsPerKey + 7) / 8);
    bits_.resize(bytes, 0);
}

void BloomFilter::Add(std::string_view key) {
    if (!enabled()) { return; }
    auto [position, step] = Hashes(key);
    const auto bit_count = bits_.size() * 8;
    for (std::size_t i = 0; i < kProbeCount; ++i) {
        const auto bit = position % bit_count;
        bits_[bit / 8] |= static_cast<std::uint8_t>(1U << (bit % 8));
        position += step;  // Unsigned wraparound is intentional and identical in both paths.
    }
}

bool BloomFilter::MayContain(std::string_view key) const {
    if (!enabled()) { return true; }
    auto [position, step] = Hashes(key);
    const auto bit_count = bits_.size() * 8;
    for (std::size_t i = 0; i < kProbeCount; ++i) {
        const auto bit = position % bit_count;
        const auto mask = static_cast<std::uint8_t>(1U << (bit % 8));
        if ((bits_[bit / 8] & mask) == 0) { return false; }
        position += step;
    }
    return true;
}

}  // namespace kvstore
