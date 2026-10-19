#include "protocol_codec.h"
#include "kvstore/raft/node.h"
#include "kvstore/raft/storage.h"

#include <stdexcept>
#include <type_traits>

namespace kvstore::rpc {
namespace {
void Require(bool valid) { if (!valid) { throw std::invalid_argument("Invalid Raft network message"); } }
}

void EncodeCommand(const raft::Command& command, wire::Command* result) {
    switch (command.type) {
        case raft::CommandType::Put: result->set_operation(wire::PUT); break;
        case raft::CommandType::Delete: result->set_operation(wire::DELETE); break;
        case raft::CommandType::NoOp: result->set_operation(wire::NO_OP); break;
    }
    result->set_key(command.key); result->set_value(command.value);
}

raft::Command DecodeCommand(const wire::Command& command, bool allow_noop) {
    Require(command.key().size() <= raft::kMaxRaftCommandBytes &&
            command.value().size() <= raft::kMaxRaftCommandBytes - command.key().size());
    raft::Command result;
    switch (command.operation()) {
        case wire::PUT: result.type = raft::CommandType::Put; break;
        case wire::DELETE: Require(command.value().empty()); result.type = raft::CommandType::Delete; break;
        case wire::NO_OP:
            Require(allow_noop && command.key().empty() && command.value().empty()); result.type = raft::CommandType::NoOp; break;
        default: throw std::invalid_argument("Unknown operation");
    }
    result.key = command.key(); result.value = command.value(); return result;
}

wire::Envelope EncodeMessage(const raft::Message& message) {
    wire::Envelope result;
    result.set_from(message.from); result.set_to(message.to); result.set_rpc_id(message.rpc_id);
    std::visit([&](const auto& body) {
        using Body = std::decay_t<decltype(body)>;
        if constexpr (std::is_same_v<Body, raft::RequestVoteRequest>) {
            auto* out = result.mutable_vote_request();
            out->set_term(body.term); out->set_candidate_id(body.candidate_id);
            out->set_last_log_index(body.last_log_index); out->set_last_log_term(body.last_log_term);
        } else if constexpr (std::is_same_v<Body, raft::RequestVoteResponse>) {
            auto* out = result.mutable_vote_response(); out->set_term(body.term); out->set_vote_granted(body.vote_granted);
        } else if constexpr (std::is_same_v<Body, raft::AppendEntriesRequest>) {
            auto* out = result.mutable_append_request(); out->set_term(body.term); out->set_leader_id(body.leader_id);
            out->set_prev_log_index(body.prev_log_index); out->set_prev_log_term(body.prev_log_term); out->set_leader_commit(body.leader_commit);
            for (const auto& entry : body.entries) {
                auto* target = out->add_entries(); target->set_index(entry.index); target->set_term(entry.term);
                EncodeCommand(entry.command, target->mutable_command());
            }
        } else {
            auto* out = result.mutable_append_response(); out->set_term(body.term); out->set_success(body.success);
        }
    }, message.body);
    return result;
}

raft::Message DecodeMessage(const wire::Envelope& message) {
    Require(message.from() != 0 && message.to() != 0 && message.from() != message.to() && message.rpc_id() != 0);
    raft::Message result{message.from(), message.to(), message.rpc_id(), {}};
    switch (message.body_case()) {
        case wire::Envelope::kVoteRequest: {
            const auto& body = message.vote_request();
            Require(body.term() != 0 && body.candidate_id() == message.from() && body.last_log_term() <= body.term() &&
                    ((body.last_log_index() == 0) == (body.last_log_term() == 0)));
            result.body = raft::RequestVoteRequest{body.term(), body.candidate_id(), body.last_log_index(), body.last_log_term()}; break;
        }
        case wire::Envelope::kVoteResponse:
            result.body = raft::RequestVoteResponse{message.vote_response().term(), message.vote_response().vote_granted()}; break;
        case wire::Envelope::kAppendRequest: {
            const auto& body = message.append_request();
            Require(body.term() != 0 && body.leader_id() == message.from() && body.prev_log_term() <= body.term() &&
                    ((body.prev_log_index() == 0) == (body.prev_log_term() == 0)) && body.entries_size() <= 64 &&
                    body.prev_log_index() <= raft::kMaxRaftEntries &&
                    static_cast<std::size_t>(body.entries_size()) <= raft::kMaxRaftEntries - body.prev_log_index());
            raft::AppendEntriesRequest request{body.term(), body.leader_id(), body.prev_log_index(), body.prev_log_term(), {}, body.leader_commit()};
            auto term = body.prev_log_term(); std::size_t bytes = 13;
            for (const auto& source : body.entries()) {
                Require(source.index() == body.prev_log_index() + request.entries.size() + 1 &&
                        source.term() != 0 && source.term() >= term && source.term() <= body.term());
                auto command = DecodeCommand(source.command());
                bytes += 25 + command.key.size() + command.value.size(); Require(bytes <= 2 * 1024 * 1024);
                request.entries.push_back({source.index(), source.term(), std::move(command)}); term = source.term();
            }
            result.body = std::move(request); break;
        }
        case wire::Envelope::kAppendResponse:
            result.body = raft::AppendEntriesResponse{message.append_response().term(), message.append_response().success()}; break;
        default: throw std::invalid_argument("Missing Raft message body");
    }
    return result;
}

void EncodeStatus(const raft::NodeStatus& status, wire::NodeStatus* result) {
    result->set_id(status.id); result->set_term(status.term); result->set_leader_id(status.leader_id.value_or(0));
    result->set_role(status.role == raft::Role::Leader ? wire::LEADER : status.role == raft::Role::Candidate ? wire::CANDIDATE : wire::FOLLOWER);
    result->set_last_log_index(status.last_log_index); result->set_commit_index(status.commit_index); result->set_last_applied(status.last_applied);
}
}  // namespace kvstore::rpc
