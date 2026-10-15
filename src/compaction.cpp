#include "kvstore/compaction.h"
#include "kvstore/sstable.h"

#include <memory>
#include <optional>
#include <queue>
#include <utility>

namespace kvstore {
namespace {

// priority_queue normally returns the greatest item. Reverse key comparison
// to get the smallest key first; for ties, the newest source (largest index) wins.
struct EntryOrder {
    const std::vector<std::optional<SSTableEntry>>* heads;
    bool operator()(std::size_t left, std::size_t right) const {
        const auto& left_key = (*heads)[left]->key;
        const auto& right_key = (*heads)[right]->key;
        if (left_key != right_key) { return left_key > right_key; }
        return left < right;
    }
};

}  // namespace

std::uint32_t MergeSSTables(const std::vector<std::string>& inputs,
                           const std::string& output, bool drop_tombstones) {
    std::vector<std::unique_ptr<SSTableReader>> readers;
    std::vector<std::optional<SSTableEntry>> heads;
    readers.reserve(inputs.size());
    heads.reserve(inputs.size());
    // Validate all input files before creating output. Each reader has its own cursor.
    for (const auto& path : inputs) {
        // Full sequential scans cannot skip keys; avoid unused per-input filters.
        auto reader = std::make_unique<SSTableReader>(path, false);
        reader->Rewind();
        heads.push_back(reader->Next());
        readers.push_back(std::move(reader));
    }
    // Do not resize heads after creating a comparator that refers to it.
    std::priority_queue<std::size_t, std::vector<std::size_t>, EntryOrder> heap(EntryOrder{&heads});
    for (std::size_t i = 0; i < heads.size(); ++i) {
        if (heads[i]) { heap.push(i); }
    }
    SSTableWriter writer(output);
    std::optional<std::string> previous_key;
    while (!heap.empty()) {
        const auto source = heap.top();
        heap.pop();
        auto entry = std::move(*heads[source]);
        // Only change a head AFTER removing its index from the heap.
        heads[source] = readers[source]->Next();
        if (heads[source]) { heap.push(source); }
        if (previous_key && *previous_key == entry.key) { continue; }
        // Remember even dropped tombstones, so older values of the same key stay hidden.
        previous_key = entry.key;
        if (entry.type == SSTableEntryType::Value || !drop_tombstones) { writer.Append(entry); }
    }
    writer.Finish();
    return writer.size();
}

}  // namespace kvstore
