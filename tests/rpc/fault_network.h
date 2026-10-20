#pragma once

#include "process_fixture.h"

#include <condition_variable>
#include <algorithm>
#include <exception>
#include <queue>

namespace {

// Test-only message scheduler. Every admitted envelope still crosses real gRPC calls.
class FaultNetwork {
public:
    struct Rule {
        bool blocked = false;
        std::size_t drop_every = 0;
        std::chrono::milliseconds delay{0};
        std::chrono::milliseconds alternate_delay{0};
        std::size_t duplicate_every = 0;
    };
    struct Stats {
        std::size_t dropped = 0, overflow = 0, forwarded = 0, duplicated = 0, reordered = 0, delayed = 0;
    };

    explicit FaultNetwork(const std::vector<std::string>& targets)
        : links_(targets.size(), std::vector<Link>(targets.size())), ends_(targets.size()) {
        do {
            addresses_ = Addresses(targets.size());
        } while (std::any_of(addresses_.begin(), addresses_.end(), [&](const auto& address) {
            return std::find(targets.begin(), targets.end(), address) != targets.end();
        }));
        try {
            for (std::size_t i = 0; i < ends_.size(); ++i) {
                ends_[i] = std::make_unique<Endpoint>(*this, i);
                grpc::ChannelArguments arguments;
                arguments.SetInt(GRPC_ARG_ENABLE_HTTP_PROXY, 0); arguments.SetInt(GRPC_ARG_ENABLE_RETRIES, 0);
                arguments.SetMaxReceiveMessageSize(rpc::kMaxRpcBytes); arguments.SetMaxSendMessageSize(rpc::kMaxRpcBytes);
                ends_[i]->target = wire::RaftTransport::NewStub(grpc::CreateCustomChannel(targets[i], grpc::InsecureChannelCredentials(), arguments));
                grpc::ServerBuilder builder; builder.AddChannelArgument(GRPC_ARG_ALLOW_REUSEPORT, 0);
                builder.SetMaxReceiveMessageSize(rpc::kMaxRpcBytes); builder.SetMaxSendMessageSize(rpc::kMaxRpcBytes);
                builder.RegisterService(&ends_[i]->service);
                int port = 0; builder.AddListeningPort(addresses_[i], grpc::InsecureServerCredentials(), &port);
                ends_[i]->server = builder.BuildAndStart();
                Check(ends_[i]->server != nullptr && port != 0, "Start test forwarding endpoint");
            }
            for (std::size_t i = 0; i < ends_.size(); ++i) {
                ends_[i]->worker = std::thread([this, i] {
                    try { Forward(i); }
                    catch (...) { std::lock_guard<std::mutex> lock(mutex_); failure_ = std::current_exception(); stopping_ = true; ready_.notify_all(); }
                });
            }
        } catch (...) { Stop(); throw; }
    }
    ~FaultNetwork() { Stop(); }
    FaultNetwork(const FaultNetwork&) = delete;
    FaultNetwork& operator=(const FaultNetwork&) = delete;

    std::string Routes() const {
        std::string routes;
        for (std::size_t i = 0; i < addresses_.size(); ++i) {
            if (i) { routes += ','; }
            routes += std::to_string(i + 1) + '=' + addresses_[i];
        }
        return routes;
    }

    void Configure(const std::function<Rule(int, int)>& rules) {
        std::unique_lock<std::mutex> lock(mutex_);
        for (int from = 0; from < static_cast<int>(ends_.size()); ++from) {
            for (int to = 0; to < static_cast<int>(ends_.size()); ++to) {
                if (from == to) { continue; }
                auto& link = links_[from][to]; link.rule = rules(from, to); ++link.generation; link.admitted = 0;
            }
        }
        ready_.notify_all();
        // A completed cut includes calls already forwarding on the links now blocked.
        ready_.wait(lock, [&] {
            if (failure_) { return true; }
            for (const auto& row : links_) {
                for (const auto& link : row) { if (link.rule.blocked && link.in_flight != 0) { return false; } }
            }
            return true;
        });
        if (failure_) { std::rethrow_exception(failure_); }
    }
    void Heal() { Configure([](int, int) { return Rule{}; }); }
    void Isolate(int node) {
        Configure([&](int from, int to) { Rule rule; rule.blocked = from == node || to == node; return rule; });
    }
    Stats Snapshot() {
        std::lock_guard<std::mutex> lock(mutex_);
        if (failure_) { std::rethrow_exception(failure_); }
        return stats_;
    }

private:
    using Clock = std::chrono::steady_clock;
    struct Link {
        Rule rule;
        std::size_t generation = 0, admitted = 0, sequence = 0, last_delivered = 0, in_flight = 0;
    };
    struct Job {
        Clock::time_point due;
        std::size_t from, generation, order, bytes;
        bool duplicate;
        wire::Envelope message;
    };
    struct Later { bool operator()(const Job& a, const Job& b) const { return a.due > b.due; } };
    struct Service final : wire::RaftTransport::Service {
        Service(FaultNetwork& owner, std::size_t destination) : owner(owner), destination(destination) {}
        grpc::Status Deliver(grpc::ServerContext*, const wire::Envelope* input, wire::Empty*) override {
            if (input->from() < 1 || input->from() > owner.ends_.size() || input->to() != destination + 1 || input->from() == input->to()) {
                return {grpc::StatusCode::INVALID_ARGUMENT, "Wrong forwarding route"};
            }
            try { return owner.Admit(destination, *input); }
            catch (const std::exception& error) { return {grpc::StatusCode::RESOURCE_EXHAUSTED, error.what()}; }
        }
        FaultNetwork& owner; std::size_t destination;
    };
    struct Endpoint {
        Endpoint(FaultNetwork& owner, std::size_t destination) : service(owner, destination) {}
        Service service;
        std::unique_ptr<wire::RaftTransport::Stub> target;
        std::unique_ptr<grpc::Server> server;
        std::thread worker;
        std::priority_queue<Job, std::vector<Job>, Later> jobs;
        std::size_t queued_bytes = 0;
    };

    grpc::Status Admit(std::size_t destination, const wire::Envelope& message) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (stopping_) { return {grpc::StatusCode::UNAVAILABLE, "Forwarder stopped"}; }
        const auto from = static_cast<std::size_t>(message.from() - 1);
        auto& link = links_[from][destination]; const auto number = ++link.admitted;
        const auto order = ++link.sequence;
        if (link.rule.blocked || (link.rule.drop_every && number % link.rule.drop_every == 0)) {
            ++stats_.dropped; return grpc::Status::OK;
        }
        const auto delay = number % 2 == 0 ? link.rule.delay : link.rule.delay + link.rule.alternate_delay;
        const auto bytes = message.ByteSizeLong();
        auto enqueue = [&](bool duplicate) {
            auto& endpoint = *ends_[destination];
            if (endpoint.jobs.size() >= 256 || bytes > 8 * 1024 * 1024 - endpoint.queued_bytes) { ++stats_.overflow; return; }
            endpoint.jobs.push({Clock::now() + delay + (duplicate ? 5ms : 0ms), from, link.generation, order, bytes, duplicate, message});
            endpoint.queued_bytes += bytes;
            if (delay.count()) { ++stats_.delayed; }
        };
        enqueue(false);
        if (link.rule.duplicate_every && number % link.rule.duplicate_every == 0) { enqueue(true); }
        ready_.notify_all(); return grpc::Status::OK;
    }

    void Forward(std::size_t destination) {
        auto& endpoint = *ends_[destination];
        while (true) {
            Job job;
            {
                std::unique_lock<std::mutex> lock(mutex_);
                while (!stopping_) {
                    if (endpoint.jobs.empty()) { ready_.wait(lock); }
                    else if (Clock::now() < endpoint.jobs.top().due) {
                        const auto due = endpoint.jobs.top().due;
                        ready_.wait_until(lock, due);
                    }
                    else { break; }
                }
                if (stopping_) { return; }
                job = endpoint.jobs.top(); endpoint.jobs.pop(); endpoint.queued_bytes -= job.bytes;
                auto& link = links_[job.from][destination];
                if (job.generation != link.generation || link.rule.blocked) { ++stats_.dropped; continue; }
                ++link.in_flight;
            }
            grpc::ClientContext context; context.set_deadline(std::chrono::system_clock::now() + 250ms);
            wire::Empty acknowledgement;
            const auto status = endpoint.target->Deliver(&context, job.message, &acknowledgement);
            {
                std::lock_guard<std::mutex> lock(mutex_);
                auto& link = links_[job.from][destination]; --link.in_flight;
                if (status.ok()) {
                    ++stats_.forwarded;
                    if (job.duplicate) { ++stats_.duplicated; }
                    if (job.order < link.last_delivered) { ++stats_.reordered; }
                    link.last_delivered = std::max(link.last_delivered, job.order);
                }
                ready_.notify_all();
            }
        }
    }

    void Stop() {
        { std::lock_guard<std::mutex> lock(mutex_); stopping_ = true; ready_.notify_all(); }
        for (auto& endpoint : ends_) {
            if (endpoint && endpoint->server) { endpoint->server->Shutdown(std::chrono::system_clock::now() + 250ms); }
        }
        for (auto& endpoint : ends_) {
            if (endpoint && endpoint->worker.joinable()) { endpoint->worker.join(); }
            if (endpoint && endpoint->server) { endpoint->server->Wait(); endpoint->server.reset(); }
        }
    }

    std::vector<std::string> addresses_;
    std::mutex mutex_;
    std::condition_variable ready_;
    std::vector<std::vector<Link>> links_;
    std::vector<std::unique_ptr<Endpoint>> ends_;
    Stats stats_;
    bool stopping_ = false;
    std::exception_ptr failure_;
};

class FaultCluster {
public:
    explicit FaultCluster(std::size_t count = 3) : cluster(true, [this](const auto& addresses) {
        network = std::make_unique<FaultNetwork>(addresses); return network->Routes();
    }, count) {}
    std::unique_ptr<FaultNetwork> network;
    Cluster cluster;
};
}  // namespace
