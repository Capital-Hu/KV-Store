#include "workload.h"

#include <iostream>

namespace {
using namespace kvstore::bench;
void Check(bool condition, const char* message) { if (!condition) { throw std::runtime_error(message); } }
void TestRanks() {
    std::vector<std::uint64_t> values;
    for (unsigned i = 1; i <= 100; ++i) { values.push_back(i); }
    Check(Percentile(values, 50) == 50 && Percentile(values, 95) == 95 && Percentile(values, 99) == 99, "Known percentile ranks");
    Check(Percentile({7}, 99) == 7 && Percentile({1, 9}, 50) == 1 && Percentile({1, 9}, 99) == 9, "Small sample ranks");
    Check(!Percentile({}, 99), "Empty sample has no percentile");
}
void TestFailuresRemainVisible() {
    const std::vector<Sample> samples{{Kind::Get, Outcome::Ok, 100, true}, {Kind::Get, Outcome::Ok, 200, false},
        {Kind::Put, Outcome::Unknown, 9000, false}, {Kind::Delete, Outcome::Unavailable, 8000, false},
        {Kind::Put, Outcome::Rejected, 1, false}};
    const auto stats = Summarize(samples);
    Check(stats.all_latencies.size() == 5 && stats.ok_latencies.size() == 2, "Failures not removed from all-call latency");
    Check(Percentile(stats.all_latencies, 99) == 9000 && Percentile(stats.ok_latencies, 99) == 200, "Separate all/ok tail latency");
    Check(stats.outcomes == std::array<std::size_t, 4>{2, 1, 1, 1}, "Every outcome counted");
    Check(stats.found == 1 && stats.missing == 1, "Missing read is a successful read");
    const auto writes = Summarize(samples, Kind::Put);
    Check(writes.all_latencies.size() == 2 && writes.ok_latencies.empty(), "Per-operation selection");
}
void TestPlansAndLimits() {
    Options options; Validate(options);
    const auto plan = MakePlan(options, 1000, 4242);
    std::array<unsigned, 3> counts{};
    for (const auto& request : plan) {
        ++counts[static_cast<std::size_t>(request.kind)];
        Check(request.key < options.keys && Key(request.key).size() == 16, "Key domain and size");
    }
    Check(counts == std::array<unsigned, 3>{500, 400, 100}, "Exact mixed workload counts");
    Check(PlanHash(plan) == PlanHash(MakePlan(options, 1000, 4242)), "Repeatable plan");
    Check(PlanHash(plan) != PlanHash(MakePlan(options, 1000, 4243)), "Seed changes plan");
    Check(MakePlan(options, 0, 0).empty(), "Zero warmup allowed");
    for (unsigned bad = 0; bad < 4; ++bad) {
        auto invalid = options;
        if (bad == 0) { invalid.threads = 0; }
        if (bad == 1) { invalid.read_percent = 95; }
        if (bad == 2) { invalid.operations = 200000; invalid.value_bytes = 65536; }
        if (bad == 3) { invalid.timeout_ms = 30001; }
        bool rejected = false;
        try { Validate(invalid); } catch (const std::invalid_argument&) { rejected = true; }
        Check(rejected, "Invalid or over-budget workload rejected");
    }
    Check(JsonString("a\"\\\n") == "\"a\\\"\\\\\\u000a\"", "JSON special characters escaped");
}
}
int main() {
    try { TestRanks(); TestFailuresRemainVisible(); TestPlansAndLimits(); std::cout << "All 3 benchmark accounting tests passed\n"; return 0; }
    catch (const std::exception& error) { std::cerr << error.what() << '\n'; return 1; }
}
