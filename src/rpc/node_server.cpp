#include "kvstore/rpc/node_server.h"
#include "protocol_codec.h"
#include "raft_node.grpc.pb.h"

#include <grpcpp/grpcpp.h>

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <deque>
#include <iostream>
#include <mutex>
#include <set>
#include <stdexcept>
#include <thread>

namespace kvstore::rpc {
namespace {
constexpr std::size_t kPeerQueueLimit = 16;

raft::NodeConfig Validate(const ServerOptions& options) {
    if (options.id == 0 || options.members.size() != 3 || options.tick_interval.count() <= 0 ||
        options.rpc_timeout.count() <= 0 || options.client_wait_timeout.count() <= 0 ||
        options.client_wait_timeout > std::chrono::seconds{30} ||
        options.tick_interval >= options.replica.election.heartbeat_interval) {
        throw std::invalid_argument("Invalid node, membership or transport timing");
    }
    std::set<raft::NodeId> ids; std::set<std::string> addresses;
    raft::NodeConfig config{options.id, {}};
    for (const auto& member : options.members) {
        ValidateAddress(member.address);
        if (member.id == 0 || !ids.insert(member.id).second || !addresses.insert(member.address).second) {
            throw std::invalid_argument("Duplicate member identity or address");
        }
        if (member.id != options.id) { config.peers.push_back(member.id); }
    }
    if (ids.count(options.id) == 0) { throw std::invalid_argument("This node is absent from the member list"); }
    if (!options.raft_routes.empty()) {
        std::set<raft::NodeId> routed_ids; std::set<std::string> routed_addresses;
        for (const auto& route : options.raft_routes) {
            ValidateAddress(route.address);
            if (!routed_ids.insert(route.id).second || !routed_addresses.insert(route.address).second) {
                throw std::invalid_argument("Duplicate Raft route identity or address");
            }
        }
        if (routed_ids != ids) { throw std::invalid_argument("Raft routes must name exactly the configured members"); }
    }
    return config;
}
}  // namespace

struct NodeServer::Impl final : wire::RaftTransport::Service, wire::NodeAdmin::Service, wire::KeyValue::Service {
    struct Peer {
        raft::NodeId id;
        std::unique_ptr<wire::RaftTransport::Stub> stub;
        std::mutex mutex;
        std::condition_variable ready;
        std::deque<wire::Envelope> messages;
        std::thread worker;
    };

    explicit Impl(ServerOptions requested) : options(std::move(requested)) {
        const auto config = Validate(options);
        replica = std::make_unique<ReplicatedStore>(options.directory, config, options.mode, options.replica);
        std::string address;
        for (const auto& member : options.members) {
            if (member.id == options.id) { address = member.address; continue; }
            auto peer = std::make_unique<Peer>(); peer->id = member.id;
            grpc::ChannelArguments arguments;
            arguments.SetInt(GRPC_ARG_ENABLE_HTTP_PROXY, 0);
            arguments.SetMaxSendMessageSize(kMaxRpcBytes); arguments.SetMaxReceiveMessageSize(kMaxRpcBytes);
            auto endpoint = member.address;
            for (const auto& route : options.raft_routes) { if (route.id == member.id) { endpoint = route.address; break; } }
            peer->stub = wire::RaftTransport::NewStub(grpc::CreateCustomChannel(endpoint, grpc::InsecureChannelCredentials(), arguments));
            peers.push_back(std::move(peer));
        }
        grpc::ServerBuilder builder;
        // Prevent a second server from silently sharing this node's TCP address.
        builder.AddChannelArgument(GRPC_ARG_ALLOW_REUSEPORT, 0);
        builder.SetMaxReceiveMessageSize(kMaxRpcBytes); builder.SetMaxSendMessageSize(kMaxRpcBytes);
        builder.RegisterService(static_cast<wire::RaftTransport::Service*>(this));
        builder.RegisterService(static_cast<wire::NodeAdmin::Service*>(this));
        builder.RegisterService(static_cast<wire::KeyValue::Service*>(this));
        int selected_port = 0;
        builder.AddListeningPort(address, grpc::InsecureServerCredentials(), &selected_port);
        server = builder.BuildAndStart();
        if (!server || selected_port == 0) { throw std::runtime_error("Cannot listen at " + address); }
        try {
            for (auto& peer : peers) { peer->worker = std::thread([this, target = peer.get()] { SendLoop(*target); }); }
            ticker = std::thread([this] { TickLoop(); });
        } catch (...) { Stop(); throw; }
    }

    ~Impl() override { Stop(); }

    void RequestStop() {
        stopping.store(true);
        progress.notify_all();
        {
            std::lock_guard<std::mutex> lock(tick_mutex);
            tick_ready.notify_all();
        }
        for (auto& peer : peers) {
            // Synchronize with the wait predicate to avoid a lost stop notification.
            std::lock_guard<std::mutex> lock(peer->mutex);
            peer->ready.notify_all();
        }
    }

    void Stop() {
        RequestStop();
        if (server) { server->Shutdown(std::chrono::system_clock::now() + options.rpc_timeout); }
        if (ticker.joinable()) { ticker.join(); }
        for (auto& peer : peers) { if (peer->worker.joinable()) { peer->worker.join(); } }
        if (server) { server->Wait(); server.reset(); }
    }

    // All callers hold state_mutex. Sender workers never hold this mutex during RPC.
    void Collect() {
        progress.notify_all();
        for (const auto& message : replica->DrainMessages()) {
            for (auto& peer : peers) {
                if (peer->id != message.to) { continue; }
                auto encoded = EncodeMessage(message);
                std::lock_guard<std::mutex> lock(peer->mutex);
                // Coalesce retries already waiting in the queue; in-flight calls are independent.
                const auto pending = std::find_if(peer->messages.begin(), peer->messages.end(), [&](const auto& old) {
                    return old.rpc_id() == encoded.rpc_id() && old.body_case() == encoded.body_case();
                });
                if (pending != peer->messages.end()) { *pending = std::move(encoded); }
                else {
                    if (peer->messages.size() == kPeerQueueLimit) { peer->messages.pop_front(); }
                    peer->messages.push_back(std::move(encoded));
                }
                peer->ready.notify_one(); break;
            }
        }
    }

    void SendLoop(Peer& peer) {
        try {
            while (!stopping.load()) {
                wire::Envelope message;
                {
                    std::unique_lock<std::mutex> lock(peer.mutex);
                    peer.ready.wait(lock, [&] { return stopping.load() || !peer.messages.empty(); });
                    if (stopping.load()) { return; }
                    message = std::move(peer.messages.front()); peer.messages.pop_front();
                }
                grpc::ClientContext context;
                context.set_deadline(std::chrono::system_clock::now() + options.rpc_timeout);
                wire::Empty acknowledgement;
                // Failed delivery is packet loss; protocol timers generate safe retries.
                (void)peer.stub->Deliver(&context, message, &acknowledgement);
            }
        } catch (const std::exception& error) { Fail(error.what()); }
    }

    void TickLoop() {
        try {
            auto previous = std::chrono::steady_clock::now();
            while (!stopping.load()) {
                {
                    std::unique_lock<std::mutex> lock(tick_mutex);
                    if (tick_ready.wait_for(lock, options.tick_interval, [&] { return stopping.load(); })) { return; }
                }
                std::lock_guard<std::mutex> lock(state_mutex);
                if (stopping.load()) { return; }
                const auto now = std::chrono::steady_clock::now();
                const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - previous);
                previous += elapsed;
                replica->AdvanceTime(elapsed); Collect();
            }
        } catch (const std::exception& error) { Fail(error.what()); }
    }

    void Fail(const char* message) {
        if (!failed.exchange(true)) { std::cerr << "Node failed: " << message << '\n'; }
        RequestStop();
    }

    grpc::Status Ready(grpc::ServerContext* context) const {
        if (stopping.load()) { return {grpc::StatusCode::UNAVAILABLE, "Node is stopping or failed"}; }
        if (context->IsCancelled()) { return {grpc::StatusCode::CANCELLED, "Call cancelled"}; }
        return grpc::Status::OK;
    }

    grpc::Status Deliver(grpc::ServerContext* context, const wire::Envelope* input, wire::Empty*) override {
        raft::Message message;
        try { message = DecodeMessage(*input); }
        catch (const std::invalid_argument& error) { return {grpc::StatusCode::INVALID_ARGUMENT, error.what()}; }
        if (message.to != options.id || std::none_of(peers.begin(), peers.end(), [&](const auto& peer) { return peer->id == message.from; })) {
            return {grpc::StatusCode::INVALID_ARGUMENT, "Wrong recipient or unknown sender"};
        }
        std::lock_guard<std::mutex> lock(state_mutex);
        const auto ready = Ready(context); if (!ready.ok()) { return ready; }
        try { replica->Step(message); Collect(); return grpc::Status::OK; }
        catch (const std::exception& error) { Fail(error.what()); return {grpc::StatusCode::UNAVAILABLE, "Node failed; reopen required"}; }
    }

    grpc::Status Inspect(grpc::ServerContext* context, const wire::Empty*, wire::NodeStatus* output) override {
        std::lock_guard<std::mutex> lock(state_mutex);
        const auto ready = Ready(context); if (!ready.ok()) { return ready; }
        try { EncodeStatus(replica->status(), output); return grpc::Status::OK; }
        catch (const std::exception& error) { Fail(error.what()); return {grpc::StatusCode::UNAVAILABLE, "Node failed"}; }
    }

    grpc::Status Propose(grpc::ServerContext* context, const wire::Command* input, wire::Proposal* output) override {
        raft::Command command;
        try { command = DecodeCommand(*input, false); }
        catch (const std::invalid_argument& error) { return {grpc::StatusCode::INVALID_ARGUMENT, error.what()}; }
        std::lock_guard<std::mutex> lock(state_mutex);
        const auto ready = Ready(context); if (!ready.ok()) { return ready; }
        try {
            if (replica->status().role != raft::Role::Leader) { return {grpc::StatusCode::FAILED_PRECONDITION, "This node is not leader"}; }
            const auto proposal = replica->Propose(std::move(command));
            output->set_index(proposal.index); output->set_term(proposal.term); Collect(); return grpc::Status::OK;
        } catch (const std::exception& error) {
            if (replica->usable()) { return {grpc::StatusCode::RESOURCE_EXHAUSTED, "Proposal could not be accepted"}; }
            Fail(error.what()); return {grpc::StatusCode::UNAVAILABLE, "Node failed"};
        }
    }

    grpc::Status LocalGet(grpc::ServerContext* context, const wire::GetRequest* input, wire::GetResponse* output) override {
        if (input->key().size() > raft::kMaxRaftCommandBytes) { return {grpc::StatusCode::INVALID_ARGUMENT, "Key too large"}; }
        std::lock_guard<std::mutex> lock(state_mutex);
        const auto ready = Ready(context); if (!ready.ok()) { return ready; }
        try {
            const auto value = replica->LocalGet(input->key()); output->set_found(value.has_value());
            if (value) { output->set_value(*value); } return grpc::Status::OK;
        } catch (const std::exception& error) { Fail(error.what()); return {grpc::StatusCode::UNAVAILABLE, "Node failed"}; }
    }

    grpc::Status Applied(grpc::ServerContext* context, const wire::Proposal* input, wire::AppliedResponse* output) override {
        std::lock_guard<std::mutex> lock(state_mutex);
        const auto ready = Ready(context); if (!ready.ok()) { return ready; }
        try { output->set_applied(replica->IsApplied({input->index(), input->term()})); return grpc::Status::OK; }
        catch (const std::exception& error) { Fail(error.what()); return {grpc::StatusCode::UNAVAILABLE, "Node failed"}; }
    }

    // The caller keeps the lock object; wait releases it so replication can advance.
    bool WaitApplied(std::unique_lock<std::mutex>& lock, grpc::ServerContext* context, raft::Proposal proposal) {
        const auto deadline = std::chrono::steady_clock::now() + options.client_wait_timeout;
        while (!stopping.load() && !context->IsCancelled()) {
            if (replica->IsApplied(proposal)) { return true; }
            const auto state = replica->status();
            if (state.role != raft::Role::Leader || state.term != proposal.term ||
                std::chrono::steady_clock::now() >= deadline) { return false; }
            // Bounded waits also observe cancellation/stop without relying on a notification.
            progress.wait_for(lock, options.tick_interval);
        }
        return false;
    }

    grpc::Status Write(grpc::ServerContext* context, const wire::Command* input, wire::WriteReply* output) override {
        raft::Command command;
        try { command = DecodeCommand(*input, false); }
        catch (const std::invalid_argument& error) { return {grpc::StatusCode::INVALID_ARGUMENT, error.what()}; }
        std::unique_lock<std::mutex> lock(state_mutex);
        const auto ready = Ready(context); if (!ready.ok()) { return ready; }
        bool attempted = false;
        try {
            const auto state = replica->status(); output->set_leader_id(state.leader_id.value_or(0));
            if (state.role != raft::Role::Leader) { output->set_outcome(wire::CLIENT_NOT_LEADER); return grpc::Status::OK; }
            if (client_waiters >= 64) { return {grpc::StatusCode::RESOURCE_EXHAUSTED, "Too many waiting client calls"}; }
            ++client_waiters;
            struct Slot { std::size_t& count; ~Slot() { --count; } } slot{client_waiters};
            attempted = true;
            const auto proposal = replica->Propose(std::move(command));
            output->mutable_proposal()->set_index(proposal.index); output->mutable_proposal()->set_term(proposal.term);
            Collect();
            output->set_outcome(WaitApplied(lock, context, proposal) ? wire::CLIENT_OK : wire::CLIENT_UNKNOWN);
            return grpc::Status::OK;
        } catch (const std::exception& error) {
            if (replica->usable()) {
                if (attempted) { output->set_outcome(wire::CLIENT_UNKNOWN); return grpc::Status::OK; }
                return {grpc::StatusCode::RESOURCE_EXHAUSTED, "Proposal could not be accepted"};
            }
            Fail(error.what()); return {grpc::StatusCode::UNAVAILABLE, "Node failed; write outcome unknown"};
        }
    }

    grpc::Status Read(grpc::ServerContext* context, const wire::GetRequest* input, wire::ReadReply* output) override {
        if (input->key().size() > raft::kMaxRaftCommandBytes) { return {grpc::StatusCode::INVALID_ARGUMENT, "Key too large"}; }
        std::unique_lock<std::mutex> lock(state_mutex);
        const auto ready = Ready(context); if (!ready.ok()) { return ready; }
        try {
            const auto state = replica->status(); output->set_leader_id(state.leader_id.value_or(0));
            if (state.role != raft::Role::Leader) { output->set_outcome(wire::CLIENT_NOT_LEADER); return grpc::Status::OK; }
            if (client_waiters >= 64) { return {grpc::StatusCode::RESOURCE_EXHAUSTED, "Too many waiting client calls"}; }
            ++client_waiters;
            struct Slot { std::size_t& count; ~Slot() { --count; } } slot{client_waiters};
            const auto barrier = replica->ProposeReadBarrier(); Collect();
            if (!WaitApplied(lock, context, barrier)) { output->set_outcome(wire::CLIENT_UNKNOWN); return grpc::Status::OK; }
            const auto current = replica->status();
            if (current.role != raft::Role::Leader || current.term != barrier.term) {
                output->set_outcome(wire::CLIENT_NOT_LEADER); return grpc::Status::OK;
            }
            const auto value = replica->LocalGet(input->key());
            output->mutable_result()->set_found(value.has_value());
            if (value) { output->mutable_result()->set_value(*value); }
            output->set_outcome(wire::CLIENT_OK); return grpc::Status::OK;
        } catch (const std::exception& error) {
            if (replica->usable()) { return {grpc::StatusCode::RESOURCE_EXHAUSTED, "Read barrier could not be accepted"}; }
            Fail(error.what()); return {grpc::StatusCode::UNAVAILABLE, "Node failed"};
        }
    }

    ServerOptions options;
    std::mutex state_mutex;
    std::condition_variable progress;
    std::size_t client_waiters = 0;
    std::unique_ptr<ReplicatedStore> replica;
    std::vector<std::unique_ptr<Peer>> peers;
    std::unique_ptr<grpc::Server> server;
    std::atomic<bool> stopping{false};
    std::atomic<bool> failed{false};
    std::mutex tick_mutex;
    std::condition_variable tick_ready;
    std::thread ticker;
};

NodeServer::NodeServer(ServerOptions options) : impl_(std::make_unique<Impl>(std::move(options))) {}
NodeServer::~NodeServer() = default;
void NodeServer::Stop() { impl_->Stop(); }
bool NodeServer::failed() const { return impl_->failed.load(); }

}  // namespace kvstore::rpc
