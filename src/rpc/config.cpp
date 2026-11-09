#include "kvstore/rpc/node_server.h"

#include <algorithm>
#include <charconv>
#include <set>
#include <stdexcept>

namespace kvstore::rpc {

ServerOptions::ServerOptions() {
    replica.election.heartbeat_interval = std::chrono::milliseconds{100};
    replica.election.election_timeout_min = std::chrono::milliseconds{600};
    replica.election.election_timeout_max = std::chrono::milliseconds{1000};
}

std::uint64_t ParseUnsigned(const std::string& text) {
    std::uint64_t result = 0;
    const auto parsed = std::from_chars(text.data(), text.data() + text.size(), result);
    if (text.empty() || parsed.ec != std::errc{} || parsed.ptr != text.data() + text.size()) {
        throw std::invalid_argument("Expected an unsigned decimal integer");
    }
    return result;
}

namespace {
constexpr auto kWildcard = "0.0.0.0";

void ValidateHostAndPort(const std::string& address, bool allow_wildcard) {
    const auto colon = address.rfind(':');
    if (colon == std::string::npos || colon == 0) { throw std::invalid_argument("Address must be HOST:PORT"); }
    const auto host = address.substr(0, colon);
    const auto valid_character = [](char c) {
        return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '.' || c == '_';
    };
    if (!std::all_of(host.begin(), host.end(), valid_character) || host.front() == '-' || host.front() == '.') {
        throw std::invalid_argument("Address host must be a DNS name or IPv4 address");
    }
    if (!allow_wildcard && host == kWildcard) { throw std::invalid_argument("0.0.0.0 can only be a listen address"); }
    const auto port_text = address.substr(colon + 1);
    const auto port = ParseUnsigned(port_text);
    if (port == 0 || port > 65535 || std::to_string(port) != port_text) { throw std::invalid_argument("Invalid TCP port"); }
}
}  // namespace

void ValidateAddress(const std::string& address) { ValidateHostAndPort(address, false); }
void ValidateListenAddress(const std::string& address) { ValidateHostAndPort(address, true); }

bool IsLoopbackAddress(const std::string& address) {
    return address.compare(0, 10, "127.0.0.1:") == 0 || address.compare(0, 10, "localhost:") == 0;
}

std::vector<Member> ParseMembers(const std::string& text) {
    std::vector<Member> result;
    std::set<raft::NodeId> ids;
    std::set<std::string> addresses;
    std::size_t start = 0;
    do {
        const auto end = text.find(',', start);
        const auto item = text.substr(start, end == std::string::npos ? end : end - start);
        const auto equal = item.find('=');
        if (equal == std::string::npos) { throw std::invalid_argument("Members use ID=HOST:PORT separated by commas"); }
        const auto id = ParseUnsigned(item.substr(0, equal));
        const auto address = item.substr(equal + 1); ValidateAddress(address);
        if (id == 0 || !ids.insert(id).second || !addresses.insert(address).second) {
            throw std::invalid_argument("Members need distinct nonzero IDs and distinct addresses");
        }
        result.push_back({id, address});
        if (end == std::string::npos) { break; }
        start = end + 1;
    } while (start <= text.size());
    if (!raft::ValidMemberCount(result.size())) { throw std::invalid_argument("A positive odd member count is required"); }
    return result;
}

}  // namespace kvstore::rpc
