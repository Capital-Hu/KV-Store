#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace kvstore {

// Merge sorted files, ordered oldest to newest. For equal keys, the later input wins.
// Default preserves tombstones, so merging a subset cannot resurrect an older value.
// Drop tombstones only when ALL older disk versions are covered and no snapshots need them.
// Output must be a new path. Files are not synchronized, published or deleted here.
// Returns the output entry count. Memory holds one current entry per input, not all data.
std::uint32_t MergeSSTables(const std::vector<std::string>& inputs,
                           const std::string& output, bool drop_tombstones = false);

}  // namespace kvstore
