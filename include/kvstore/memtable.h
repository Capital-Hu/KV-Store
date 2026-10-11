#pragma once

#include <map>
#include <optional>
#include <string>

namespace kvstore {

// Single-threaded, in-memory storage. No persistence yet.
class MemTable {
public:
    void Put(const std::string& key, const std::string& value);
    std::optional<std::string> Get(const std::string& key) const;

    // Returns true if a key was removed; a missing key is allowed.
    bool Delete(const std::string& key);

private:
    std::map<std::string, std::string> entries_;
};

}  // namespace kvstore
