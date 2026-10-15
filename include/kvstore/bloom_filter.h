#pragma once

#include <cstddef>
#include <cstdint>
#include <string_view>
#include <vector>

namespace kvstore {

// Add-only membership hint: false means absent, true still needs an exact lookup.
// Sizing is an estimate; adding more keys can increase false positives, not lose keys.
// Mutable during construction, then safe for concurrent read-only queries.
class BloomFilter {
public:
    static constexpr std::size_t kBitsPerKey = 10;
    static constexpr std::size_t kProbeCount = 7;
    static constexpr std::size_t kMaxBytes = 8 * 1024 * 1024;

    explicit BloomFilter(std::size_t expected_keys);
    void Add(std::string_view key);
    bool MayContain(std::string_view key) const;
    // Oversized estimates disable filtering: MayContain conservatively returns true.
    bool enabled() const { return enabled_ && !bits_.empty(); }
    std::size_t byte_size() const { return bits_.size(); }

private:
    std::vector<std::uint8_t> bits_;
    bool enabled_ = true;
};

}  // namespace kvstore
