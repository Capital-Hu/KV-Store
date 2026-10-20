#include "kvstore/rpc/client.h"
#include "protocol_codec.h"
#include "raft_node.grpc.pb.h"

#include <grpcpp/grpcpp.h>
#include <algorithm>
#include <set>
#include <stdexcept>
#include <thread>

namespace kvstore::rpc {
namespace {
using Clock = std::chrono::steady_clock;
ClientResult Result(ClientCode code, std::string message) {
    ClientResult result; result.code = code; result.message = std::move(message); return result;
}
}

struct Client::Impl {
    struct Peer {
        std::unique_ptr<wire::NodeAdmin::Stub> admin;
        std::unique_ptr<wire::KeyValue::Stub> kv;
    };
    explicit Impl(ClientOptions requested) : options(std::move(requested)) {
        if (options.members.size() != 3 || options.timeout.count() <= 0 ||
            options.timeout > std::chrono::seconds{30} || options.rpc_timeout.count() <= 0 ||
            options.rpc_timeout > std::chrono::seconds{30}) { throw std::invalid_argument("Invalid client members or timeout"); }
        std::set<raft::NodeId> ids; std::set<std::string> addresses;
        for (const auto& member : options.members) {
            ValidateAddress(member.address);
            if (member.id == 0 || !ids.insert(member.id).second || !addresses.insert(member.address).second) {
                throw std::invalid_argument("Duplicate client member identity or address");
            }
            grpc::ChannelArguments arguments;
            arguments.SetInt(GRPC_ARG_ENABLE_HTTP_PROXY, 0);
            // No transparent/configured gRPC retry may duplicate a submitted write.
            arguments.SetInt(GRPC_ARG_ENABLE_RETRIES, 0);
            arguments.SetMaxSendMessageSize(kMaxRpcBytes); arguments.SetMaxReceiveMessageSize(kMaxRpcBytes);
            auto channel = grpc::CreateCustomChannel(member.address, grpc::InsecureChannelCredentials(), arguments);
            peers.push_back({wire::NodeAdmin::NewStub(channel), wire::KeyValue::NewStub(channel)});
        }
    }

    void Deadline(grpc::ClientContext& context, Clock::time_point end, std::chrono::milliseconds cap) const {
        const auto remaining = std::max(Clock::duration::zero(), end - Clock::now());
        const auto budget = std::min(remaining, std::chrono::duration_cast<Clock::duration>(cap));
        context.set_deadline(std::chrono::system_clock::now() +
            std::chrono::duration_cast<std::chrono::system_clock::duration>(budget));
    }

    std::optional<std::size_t> Leader(Clock::time_point end, std::size_t first) const {
        std::optional<std::size_t> candidate;
        raft::Term newest = 0;
        for (std::size_t offset = 0; offset < peers.size() && Clock::now() < end; ++offset) {
            const auto index = (first + offset) % peers.size();
            grpc::ClientContext context; Deadline(context, end, std::chrono::milliseconds{150});
            wire::Empty input; wire::NodeStatus status;
            if (!peers[index].admin->Inspect(&context, input, &status).ok() || status.id() != options.members[index].id) { continue; }
            if (status.term() > newest) { newest = status.term(); candidate.reset(); }
            if (!candidate && status.term() == newest && status.role() == wire::LEADER) { candidate = index; }
        }
        return candidate;
    }

    std::size_t Next(std::size_t current, raft::NodeId hint) const {
        for (std::size_t i = 0; i < options.members.size(); ++i) {
            if (i != current && options.members[i].id == hint) { return i; }
        }
        return (current + 1) % peers.size();
    }

    ClientResult Call(const std::string& key, const std::optional<wire::Command>& write) const {
        if (key.size() > raft::kMaxRaftCommandBytes) { return Result(ClientCode::Rejected, "Key too large"); }
        if (write) {
            try { (void)DecodeCommand(*write, false); }
            catch (const std::invalid_argument& error) { return Result(ClientCode::Rejected, error.what()); }
        }
        const auto end = Clock::now() + options.timeout;
        std::size_t first = 0;
        while (Clock::now() < end) {
            const auto leader = Leader(end, first);
            if (leader && Clock::now() < end) {
                grpc::ClientContext context; Deadline(context, end, options.rpc_timeout);
                if (write) {
                    wire::WriteReply reply;
                    const auto status = peers[*leader].kv->Write(&context, *write, &reply);
                    if (!status.ok()) {
                        // A transport status alone cannot certify that no application occurred.
                        return Result(ClientCode::Unknown, "Write RPC failed; it may have committed. Do not blindly retry.");
                    }
                    if (reply.outcome() == wire::CLIENT_NOT_LEADER) { first = Next(*leader, reply.leader_id()); }
                    else {
                        auto result = Result(reply.outcome() == wire::CLIENT_OK && reply.has_proposal() &&
                            reply.proposal().index() != 0 && reply.proposal().term() != 0 ? ClientCode::Ok : ClientCode::Unknown,
                            "Write outcome is unknown unless confirmed applied");
                        if (reply.has_proposal()) { result.proposal = raft::Proposal{reply.proposal().index(), reply.proposal().term()}; }
                        if (result.code == ClientCode::Ok) { result.message.clear(); }
                        return result;
                    }
                } else {
                    wire::GetRequest input; input.set_key(key); wire::ReadReply reply;
                    const auto status = peers[*leader].kv->Read(&context, input, &reply);
                    if (status.ok() && reply.outcome() == wire::CLIENT_OK && reply.has_result()) {
                        auto result = Result(ClientCode::Ok, "");
                        if (reply.result().found()) { result.value = reply.result().value(); }
                        return result;
                    }
                    if (!status.ok() && status.error_code() == grpc::StatusCode::INVALID_ARGUMENT) {
                        return Result(ClientCode::Rejected, status.error_message());
                    }
                    first = Next(*leader, reply.leader_id());
                }
            }
            const auto remaining = end - Clock::now();
            if (remaining > Clock::duration::zero()) {
                std::this_thread::sleep_for(std::min(remaining, std::chrono::duration_cast<Clock::duration>(std::chrono::milliseconds{20})));
            }
        }
        return Result(ClientCode::Unavailable, "No successful leader operation within the deadline");
    }

    ClientOptions options;
    std::vector<Peer> peers;
};

Client::Client(ClientOptions options) : impl_(std::make_unique<Impl>(std::move(options))) {}
Client::~Client() = default;
ClientResult Client::Put(const std::string& key, const std::string& value) const {
    wire::Command command; command.set_operation(wire::PUT); command.set_key(key); command.set_value(value);
    return impl_->Call(key, command);
}
ClientResult Client::Delete(const std::string& key) const {
    wire::Command command; command.set_operation(wire::DELETE); command.set_key(key);
    return impl_->Call(key, command);
}
ClientResult Client::Get(const std::string& key) const { return impl_->Call(key, std::nullopt); }

}  // namespace kvstore::rpc
