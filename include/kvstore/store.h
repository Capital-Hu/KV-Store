#pragma once

#include "kvstore/memtable.h"

#include <cstddef>
#include <optional>
#include <string>
#include <string_view>

namespace kvstore {

enum class WalRecoveryMode {
    Strict,
    TruncateIncompleteTail,
};

// Single-threaded store. The WAL's parent directory must already exist.
// Successful writes synchronize the WAL before updating the MemTable.
class Store {
public:
    explicit Store(const std::string& wal_path,
                   WalRecoveryMode recovery = WalRecoveryMode::Strict);
    ~Store();

    // A file descriptor and its lock must have exactly one owner.
    Store(const Store&) = delete;
    Store& operator=(const Store&) = delete;
    Store(Store&&) = delete;
    Store& operator=(Store&&) = delete;

    // Key + value must fit within the WAL's 1 MiB payload limit.
    void Put(const std::string& key, const std::string& value);
    std::optional<std::string> Get(const std::string& key) const;
    bool Delete(const std::string& key);

    std::size_t truncated_tail_bytes() const { return truncated_tail_bytes_; }

private:
    void Replay(WalRecoveryMode recovery);
    void AppendAndSync(std::string_view bytes);
    void EnsureUsable() const;

    int fd_ = -1;
    MemTable table_;
    bool usable_ = true;
    std::size_t truncated_tail_bytes_ = 0;
};

}  // namespace kvstore
