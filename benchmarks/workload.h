#pragma once

#include <algorithm>
#include <array>
#include <cstdint>
#include <optional>
#include <random>
#include <stdexcept>
#include <string>
#include <vector>

namespace kvstore::bench {

enum class Kind { Get, Put, Delete };
enum class Outcome { Ok, Unknown, Unavailable, Rejected };
inline const char* Name(Kind kind) {
    switch (kind) {
        case Kind::Get: return "get";
        case Kind::Put: return "put";
        case Kind::Delete: return "delete";
    }
    throw std::logic_error("Invalid benchmark operation");
}
inline const char* Name(Outcome outcome) {
    switch (outcome) {
        case Outcome::Ok: return "ok";
        case Outcome::Unknown: return "unknown";
        case Outcome::Unavailable: return "unavailable";
        case Outcome::Rejected: return "rejected";
    }
    throw std::logic_error("Invalid benchmark outcome");
}

struct Options {
    std::size_t operations = 1000;
    std::size_t warmup = 128;
    std::size_t keys = 256;
    std::size_t value_bytes = 256;
    std::size_t threads = 4;
    std::size_t read_percent = 50;
    std::size_t delete_percent = 10;
    std::size_t timeout_ms = 5000;
    std::uint64_t seed = 4242;
};

inline void Validate(const Options& options) {
    if (options.operations == 0 || options.operations > 200000 || options.warmup > 200000 ||
        options.keys == 0 || options.keys > 100000 || options.value_bytes > 65536 ||
        options.threads == 0 || options.threads > 32 || options.threads > options.operations ||
        options.read_percent > 100 || options.delete_percent > 100 - options.read_percent ||
        options.timeout_ms == 0 || options.timeout_ms > 30000) {
        throw std::invalid_argument("Invalid benchmark size, concurrency, percentages or timeout");
    }
    // Leave headroom below the live Raft log limit; even reads append NoOps.
    const auto entries = std::uint64_t{options.keys} + options.warmup + options.operations;
    if (entries > 500000 || entries * (25 + 16 + options.value_bytes) > 48 * 1024 * 1024) {
        throw std::invalid_argument("Workload exceeds the conservative fresh-cluster log budget");
    }
}

struct Request { Kind kind; std::size_t key; };
inline std::vector<Request> MakePlan(const Options& options, std::size_t count, std::uint64_t seed) {
    std::mt19937_64 random(seed);
    std::vector<Request> plan;
    plan.reserve(count);
    const auto reads = count * options.read_percent / 100;
    const auto deletes = count * options.delete_percent / 100;
    for (std::size_t i = 0; i < count; ++i) {
        const auto kind = i < reads ? Kind::Get : i < reads + deletes ? Kind::Delete : Kind::Put;
        plan.push_back({kind, static_cast<std::size_t>(random() % options.keys)});
    }
    // Integer modulo and an explicit shuffle keep the generated plan portable.
    for (std::size_t i = plan.size(); i > 1; --i) { std::swap(plan[i - 1], plan[random() % i]); }
    return plan;
}
inline std::uint64_t PlanHash(const std::vector<Request>& plan) {
    std::uint64_t hash = 14695981039346656037ULL;
    for (const auto& request : plan) {
        hash = (hash ^ static_cast<unsigned>(request.kind)) * 1099511628211ULL;
        for (unsigned shift = 0; shift < 64; shift += 8) {
            hash = (hash ^ ((std::uint64_t{request.key} >> shift) & 255)) * 1099511628211ULL;
        }
    }
    return hash;
}
inline std::string Key(std::size_t index) {
    const auto number = std::to_string(index);
    return "key-" + std::string(12 - number.size(), '0') + number;
}

struct Sample {
    Kind kind = Kind::Get;
    Outcome outcome = Outcome::Unavailable;
    std::uint64_t latency_ns = 0;
    bool found = false;
};
struct Stats {
    std::array<std::size_t, 4> outcomes{};
    std::size_t found = 0;
    std::size_t missing = 0;
    std::vector<std::uint64_t> all_latencies;
    std::vector<std::uint64_t> ok_latencies;
};
inline Stats Summarize(const std::vector<Sample>& samples, std::optional<Kind> kind = std::nullopt) {
    Stats stats;
    for (const auto& sample : samples) {
        if (kind && sample.kind != *kind) { continue; }
        ++stats.outcomes[static_cast<std::size_t>(sample.outcome)];
        stats.all_latencies.push_back(sample.latency_ns);
        if (sample.outcome == Outcome::Ok) {
            stats.ok_latencies.push_back(sample.latency_ns);
            if (sample.kind == Kind::Get) { ++(sample.found ? stats.found : stats.missing); }
        }
    }
    std::sort(stats.all_latencies.begin(), stats.all_latencies.end());
    std::sort(stats.ok_latencies.begin(), stats.ok_latencies.end());
    return stats;
}
// Nearest rank: ceil(N * percent / 100), using one-based ranks.
inline std::optional<std::uint64_t> Percentile(const std::vector<std::uint64_t>& sorted, unsigned percent) {
    if (percent == 0 || percent > 100) { throw std::invalid_argument("Percentile must be 1..100"); }
    if (sorted.empty()) { return std::nullopt; }
    return sorted[(sorted.size() * percent + 99) / 100 - 1];
}

inline std::string JsonString(const std::string& value) {
    constexpr char digits[] = "0123456789abcdef";
    std::string result = "\"";
    for (const unsigned char byte : value) {
        if (byte == '"' || byte == '\\') { result += '\\'; result += static_cast<char>(byte); }
        else if (byte < 32) { result += "\\u00"; result += digits[byte >> 4]; result += digits[byte & 15]; }
        else { result += static_cast<char>(byte); }
    }
    return result + '"';
}

}  // namespace kvstore::bench
