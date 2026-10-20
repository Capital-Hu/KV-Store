#include "workload.h"
#include "kvstore/rpc/client.h"

#include <condition_variable>
#include <exception>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <mutex>
#include <set>
#include <thread>

namespace {
namespace bench = kvstore::bench;
namespace rpc = kvstore::rpc;
using Clock = std::chrono::steady_clock;
constexpr auto usage = "kvbench --members MEMBERS [--operations N] [--warmup N] [--keys N] "
    "[--value-bytes N] [--threads N] [--read-percent N] [--delete-percent N] "
    "[--seed N] [--timeout-ms N] [--samples PATH]";

bench::Outcome Outcome(rpc::ClientCode code) {
    switch (code) {
        case rpc::ClientCode::Ok: return bench::Outcome::Ok;
        case rpc::ClientCode::Unknown: return bench::Outcome::Unknown;
        case rpc::ClientCode::Unavailable: return bench::Outcome::Unavailable;
        case rpc::ClientCode::Rejected: return bench::Outcome::Rejected;
    }
    throw std::logic_error("Invalid client code");
}
struct Phase { std::vector<bench::Sample> samples; double seconds = 0; };
Phase Run(const std::vector<std::unique_ptr<rpc::Client>>& clients,
          const std::vector<bench::Request>& plan, const std::vector<std::string>& keys, const std::string& value) {
    Phase phase; phase.samples.resize(plan.size());
    if (plan.empty()) { return phase; }
    std::mutex mutex;
    std::condition_variable ready;
    std::size_t waiting = 0;
    bool start = false, cancelled = false;
    std::vector<std::thread> workers;
    std::vector<std::exception_ptr> errors(clients.size());
    std::vector<Clock::time_point> finishes(clients.size());
    try {
        for (std::size_t worker = 0; worker < clients.size(); ++worker) {
            workers.emplace_back([&, worker] {
                {
                    std::unique_lock<std::mutex> lock(mutex);
                    ++waiting; ready.notify_all();
                    ready.wait(lock, [&] { return start || cancelled; });
                    if (cancelled) { return; }
                }
                try {
                    // Each worker owns fixed plan slots and at most one outstanding call.
                    for (std::size_t i = worker; i < plan.size(); i += clients.size()) {
                        const auto& request = plan[i];
                        const auto begin = Clock::now();
                        const auto result = request.kind == bench::Kind::Get ? clients[worker]->Get(keys[request.key]) :
                            request.kind == bench::Kind::Put ? clients[worker]->Put(keys[request.key], value) :
                            clients[worker]->Delete(keys[request.key]);
                        const auto end = Clock::now();
                        phase.samples[i] = {request.kind, Outcome(result.code),
                            static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count()),
                            result.value.has_value()};
                    }
                } catch (...) { errors[worker] = std::current_exception(); }
                finishes[worker] = Clock::now();
            });
        }
    } catch (...) {
        { std::lock_guard<std::mutex> lock(mutex); cancelled = true; }
        ready.notify_all();
        for (auto& worker : workers) { worker.join(); }
        throw;
    }
    std::unique_lock<std::mutex> lock(mutex);
    ready.wait(lock, [&] { return waiting == clients.size(); });
    const auto begin = Clock::now(); start = true;
    lock.unlock(); ready.notify_all();
    for (auto& worker : workers) { worker.join(); }
    for (const auto& error : errors) { if (error) { std::rethrow_exception(error); } }
    phase.seconds = std::chrono::duration<double>(*std::max_element(finishes.begin(), finishes.end()) - begin).count();
    return phase;
}

void PrintLatency(const std::vector<std::uint64_t>& sorted) {
    std::cout << '{';
    for (const unsigned percent : {50, 95, 99}) {
        if (percent != 50) { std::cout << ','; }
        std::cout << "\"p" << percent << "\":";
        const auto value = bench::Percentile(sorted, percent);
        if (value) { std::cout << static_cast<double>(*value) / 1000000; }
        else { std::cout << "null"; }
    }
    std::cout << '}';
}
void PrintStats(const bench::Stats& stats, double seconds) {
    const auto count = stats.all_latencies.size();
    std::cout << "{\"attempted\":" << count;
    for (const auto outcome : {bench::Outcome::Ok, bench::Outcome::Unknown, bench::Outcome::Unavailable, bench::Outcome::Rejected}) {
        std::cout << ",\"" << bench::Name(outcome) << "\":" << stats.outcomes[static_cast<std::size_t>(outcome)];
    }
    std::cout << ",\"read_found\":" << stats.found << ",\"read_missing\":" << stats.missing
              << ",\"attempted_ops_per_second\":" << (seconds > 0 ? count / seconds : 0)
              << ",\"ok_ops_per_second\":" << (seconds > 0 ? stats.ok_latencies.size() / seconds : 0)
              << ",\"latency_ms_all\":";
    PrintLatency(stats.all_latencies); std::cout << ",\"latency_ms_ok\":";
    PrintLatency(stats.ok_latencies); std::cout << '}';
}
void PrintPhase(const Phase& phase, std::uint64_t hash) {
    std::cout << "{\"seconds\":" << phase.seconds << ",\"plan_hash\":" << bench::JsonString(std::to_string(hash))
              << ",\"total\":";
    PrintStats(bench::Summarize(phase.samples), phase.seconds);
    std::cout << ",\"by_operation\":{";
    for (const auto kind : {bench::Kind::Get, bench::Kind::Put, bench::Kind::Delete}) {
        if (kind != bench::Kind::Get) { std::cout << ','; }
        std::cout << '"' << bench::Name(kind) << "\":";
        PrintStats(bench::Summarize(phase.samples, kind), phase.seconds);
    }
    std::cout << "}}";
}
}

int main(int argc, char** argv) {
    try {
        if (argc == 2 && std::string(argv[1]) == "--help") { std::cout << usage << '\n'; return 0; }
        bench::Options options;
        std::string members, sample_path;
        std::set<std::string> seen;
        for (int i = 1; i < argc; i += 2) {
            const std::string flag = argv[i];
            if (i + 1 == argc || !seen.insert(flag).second) { throw std::invalid_argument(usage); }
            const std::string value = argv[i + 1];
            if (flag == "--members") { members = value; continue; }
            if (flag == "--samples") { sample_path = value; continue; }
            const auto number = rpc::ParseUnsigned(value);
            if (number > std::numeric_limits<std::size_t>::max()) { throw std::invalid_argument("Number too large"); }
            if (flag == "--operations") { options.operations = number; }
            else if (flag == "--warmup") { options.warmup = number; }
            else if (flag == "--keys") { options.keys = number; }
            else if (flag == "--value-bytes") { options.value_bytes = number; }
            else if (flag == "--threads") { options.threads = number; }
            else if (flag == "--read-percent") { options.read_percent = number; }
            else if (flag == "--delete-percent") { options.delete_percent = number; }
            else if (flag == "--timeout-ms") { options.timeout_ms = number; }
            else if (flag == "--seed") { options.seed = number; }
            else { throw std::invalid_argument(usage); }
        }
        bench::Validate(options);
        rpc::ClientOptions client_options; client_options.members = rpc::ParseMembers(members);
        client_options.timeout = std::chrono::milliseconds{options.timeout_ms};
        client_options.rpc_timeout = client_options.timeout;
        std::ofstream sample_output;
        if (!sample_path.empty()) {
            if (std::filesystem::exists(sample_path)) { throw std::invalid_argument("Samples path already exists"); }
            sample_output.open(sample_path);
            if (!sample_output) { throw std::runtime_error("Cannot open samples file"); }
        }
        std::vector<std::unique_ptr<rpc::Client>> clients;
        for (std::size_t i = 0; i < options.threads; ++i) { clients.push_back(std::make_unique<rpc::Client>(client_options)); }
        std::vector<std::string> keys;
        for (std::size_t i = 0; i < options.keys; ++i) { keys.push_back(bench::Key(i)); }
        const std::string value(options.value_bytes, 'v');
        const auto warmup_plan = bench::MakePlan(options, options.warmup, options.seed ^ 0x9e3779b97f4a7c15ULL);
        const auto measured_plan = bench::MakePlan(options, options.operations, options.seed);
        std::cerr << "Preloading " << keys.size() << " keys\n";
        for (const auto& key : keys) {
            const auto result = clients.front()->Put(key, value);
            if (result.code != rpc::ClientCode::Ok) { throw std::runtime_error("Preload did not confirm: " + result.message); }
        }
        std::cerr << "Warmup: " << options.warmup << " operations\n";
        const auto warmup = Run(clients, warmup_plan, keys, value);
        std::cerr << "Measuring " << options.operations << " operations with " << options.threads << " clients\n";
        const auto measured = Run(clients, measured_plan, keys, value);
        if (sample_output.is_open()) {
            sample_output << "index,operation,outcome,latency_ns,found\n";
            for (std::size_t i = 0; i < measured.samples.size(); ++i) {
                const auto& sample = measured.samples[i];
                sample_output << i << ',' << bench::Name(sample.kind) << ',' << bench::Name(sample.outcome) << ',' << sample.latency_ns << ',';
                if (sample.kind == bench::Kind::Get && sample.outcome == bench::Outcome::Ok) { sample_output << sample.found; }
                sample_output << '\n';
            }
            sample_output.flush();
            if (!sample_output) { throw std::runtime_error("Cannot write samples file"); }
        }
        std::cout << std::setprecision(12) << "{\"schema_version\":1,\"clock\":\"steady_clock\",\"load_model\":\"closed_loop\","
                  << "\"build_type\":" << bench::JsonString(KVSTORE_BENCH_BUILD_TYPE)
                  << ",\"compiler\":" << bench::JsonString(__VERSION__) << ",\"members\":" << bench::JsonString(members)
                  << ",\"parameters\":{\"operations\":" << options.operations << ",\"warmup\":" << options.warmup
                  << ",\"keys\":" << options.keys << ",\"key_bytes\":16,\"value_bytes\":" << options.value_bytes
                  << ",\"threads\":" << options.threads << ",\"read_percent\":" << options.read_percent
                  << ",\"delete_percent\":" << options.delete_percent << ",\"seed\":" << options.seed
                  << ",\"timeout_ms\":" << options.timeout_ms << "},\"preload_ok\":" << keys.size() << ",\"warmup\":";
        PrintPhase(warmup, bench::PlanHash(warmup_plan)); std::cout << ",\"measurement\":";
        PrintPhase(measured, bench::PlanHash(measured_plan)); std::cout << "}\n";
        if (!std::cout) { throw std::runtime_error("Cannot write benchmark JSON"); }
        const auto ok = [](const Phase& phase) {
            return std::all_of(phase.samples.begin(), phase.samples.end(), [](const auto& sample) { return sample.outcome == bench::Outcome::Ok; });
        };
        return ok(warmup) && ok(measured) ? 0 : 3;
    } catch (const std::exception& error) { std::cerr << "Benchmark failed: " << error.what() << '\n'; return 2; }
}
