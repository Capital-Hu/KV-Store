#include "kvstore/rpc/node_server.h"
#include "rpc/protocol_codec.h"
#include "raft_node.grpc.pb.h"

#include <grpcpp/grpcpp.h>
#include <iostream>
#include <limits>
#include <stdexcept>

namespace {
namespace raft = kvstore::raft;
namespace rpc = kvstore::rpc;
namespace wire = kvstore::wire;

void Check(bool condition, const std::string& message) { if (!condition) { throw std::runtime_error(message); } }
template <typename Action>
void Reject(Action action, const std::string& message) {
    try { action(); } catch (const std::exception&) { return; } throw std::runtime_error(message);
}
wire::Envelope Append() {
    return rpc::EncodeMessage({10, 20, 7, raft::AppendEntriesRequest{2, 10, 0, 0,
        {{1, 1, {raft::CommandType::Put, std::string("k\0x", 3), std::string("v\0z", 3)}},
         {2, 2, {raft::CommandType::Delete, "a", ""}}, {3, 2, {raft::CommandType::NoOp, "", ""}}}, 3}});
}

void TestAllMessageKindsSurviveWireEncoding() {
    const auto maximum = std::numeric_limits<std::uint64_t>::max();
    const std::vector<raft::Message> messages{
        {10, 20, maximum, raft::RequestVoteRequest{maximum, 10, 3, 2}},
        {20, 10, 8, raft::RequestVoteResponse{2, true}},
        rpc::DecodeMessage(Append()), {20, 10, 9, raft::AppendEntriesResponse{2, false}}};
    for (const auto& message : messages) {
        const auto original = rpc::EncodeMessage(message);
        wire::Envelope parsed; Check(parsed.ParseFromString(original.SerializeAsString()), "Parse serialized protobuf");
        const auto restored = rpc::DecodeMessage(parsed);
        Check(rpc::EncodeMessage(restored).SerializeAsString() == original.SerializeAsString(), "Every field survives protobuf transport");
    }
    const auto data = std::get<raft::AppendEntriesRequest>(rpc::DecodeMessage(Append()).body);
    Check(data.entries[0].command.key == std::string("k\0x", 3) && data.entries[0].command.value == std::string("v\0z", 3), "Byte fields preserve embedded zeroes");
}

void TestInvalidEnvelopeIdentityAndBody() {
    for (int fault = 0; fault < 5; ++fault) {
        auto message = Append();
        if (fault == 0) { message.set_from(0); } else if (fault == 1) { message.set_to(0); }
        else if (fault == 2) { message.set_to(message.from()); } else if (fault == 3) { message.set_rpc_id(0); }
        else { message.clear_body(); }
        Reject([&] { rpc::DecodeMessage(message); }, "Invalid envelope rejected before protocol state");
    }
}

void TestOperationValidationAndAdminNoOpRejection() {
    wire::Command command; command.set_operation(wire::PUT);
    Check(rpc::DecodeCommand(command).value.empty(), "Empty Put is valid");
    for (const auto operation : {0, 99}) {
        command.set_operation(static_cast<wire::Operation>(operation)); Reject([&] { rpc::DecodeCommand(command); }, "Unknown enum rejected");
    }
    command.set_operation(wire::DELETE); command.set_value("bad"); Reject([&] { rpc::DecodeCommand(command); }, "Delete has no value");
    command.set_operation(wire::NO_OP); command.clear_value(); command.set_key("bad");
    Reject([&] { rpc::DecodeCommand(command); }, "NoOp has no key"); command.clear_key();
    Reject([&] { rpc::DecodeCommand(command, false); }, "Admin cannot propose NoOp");
    command.set_operation(wire::PUT); command.set_value(std::string(raft::kMaxRaftCommandBytes + 1, 'x'));
    Reject([&] { rpc::DecodeCommand(command); }, "Command size bounded");
}

void TestAppendAndVoteMetadataValidation() {
    for (int fault = 0; fault < 8; ++fault) {
        auto message = Append(); auto* body = message.mutable_append_request();
        switch (fault) {
            case 0: body->set_leader_id(30); break;
            case 1: body->set_prev_log_term(1); break;
            case 2: body->mutable_entries(0)->set_index(2); break;
            case 3: body->mutable_entries(0)->set_term(0); break;
            case 4: body->mutable_entries(0)->set_term(3); break;
            case 5: body->mutable_entries(2)->set_term(1); break;
            case 6: body->set_prev_log_index(raft::kMaxRaftEntries + 1); break;
            case 7: body->mutable_entries(0)->clear_command(); break;
        }
        Reject([&] { rpc::DecodeMessage(message); }, "Invalid append shape rejected");
    }
    auto vote = rpc::EncodeMessage({10, 20, 1, raft::RequestVoteRequest{2, 10, 0, 0}});
    vote.mutable_vote_request()->set_last_log_term(1);
    Reject([&] { rpc::DecodeMessage(vote); }, "Vote prefix metadata must agree");
    vote.mutable_vote_request()->set_last_log_index(1); vote.mutable_vote_request()->set_last_log_term(3);
    Reject([&] { rpc::DecodeMessage(vote); }, "Vote log term cannot exceed request term");
}

void TestAppendCountAndByteLimits() {
    auto message = Append(); auto* body = message.mutable_append_request(); body->clear_entries();
    for (std::uint64_t i = 1; i <= 65; ++i) {
        auto* entry = body->add_entries(); entry->set_index(i); entry->set_term(2); entry->mutable_command()->set_operation(wire::NO_OP);
    }
    Reject([&] { rpc::DecodeMessage(message); }, "Transport accepts at most one protocol batch");
    body->clear_entries();
    for (std::uint64_t i = 1; i <= 2; ++i) {
        auto* entry = body->add_entries(); entry->set_index(i); entry->set_term(2); entry->mutable_command()->set_operation(wire::PUT);
        entry->mutable_command()->set_value(std::string(raft::kMaxRaftCommandBytes, 'x'));
    }
    Reject([&] { rpc::DecodeMessage(message); }, "Encoded entry headers count against batch byte budget");
}

void TestStatusFieldsAndUnknownLeader() {
    raft::NodeStatus status; status.id = 20; status.term = 4; status.last_log_index = 8; status.commit_index = 7; status.last_applied = 6;
    for (const auto role : {raft::Role::Follower, raft::Role::Candidate, raft::Role::Leader}) {
        status.role = role; wire::NodeStatus output; rpc::EncodeStatus(status, &output);
        Check(output.id() == 20 && output.term() == 4 && output.leader_id() == 0 && output.last_log_index() == 8 &&
              output.commit_index() == 7 && output.last_applied() == 6, "Diagnostics retain separate positions");
        Check(output.role() == (role == raft::Role::Follower ? wire::FOLLOWER : role == raft::Role::Candidate ? wire::CANDIDATE : wire::LEADER), "Role encoding is explicit");
    }
    status.leader_id = 30; wire::NodeStatus output; rpc::EncodeStatus(status, &output); Check(output.leader_id() == 30, "Known leader retained");
}

void TestMemberParsingAndLoopbackRestriction() {
    const auto members = rpc::ParseMembers("10=127.0.0.1:5101,20=127.0.0.1:5102,30=127.0.0.1:5103");
    Check(members.size() == 3 && members[1].id == 20, "Member IDs are configurable");
    for (const auto& invalid : {"", "1=127.0.0.1:1", "1=127.0.0.1:1,1=127.0.0.1:2,3=127.0.0.1:3",
         "1=127.0.0.1:1,2=127.0.0.1:1,3=127.0.0.1:3", "0=127.0.0.1:1,2=127.0.0.1:2,3=127.0.0.1:3",
         "1=127.0.0.1:1,2=127.0.0.1:2,3=127.0.0.1:3,"}) {
        Reject([&] { rpc::ParseMembers(invalid); }, "Invalid member set rejected");
    }
    for (const auto& address : {"0.0.0.0:5001", "localhost:5001", "127.0.0.1:0", "127.0.0.1:65536", "127.0.0.1:05001", "127.0.0.1:-1"}) {
        Reject([&] { rpc::ValidateAddress(address); }, "Invalid or non-loopback address rejected");
    }
    Reject([&] { rpc::ParseUnsigned("18446744073709551616"); }, "Integer overflow rejected");
}

void TestRuntimeOptionsRejectBeforeOpeningFiles() {
    rpc::ServerOptions options; options.directory = "";
    options.members = rpc::ParseMembers("10=127.0.0.1:5101,20=127.0.0.1:5102,30=127.0.0.1:5103");
    for (const int fault : {0, 1, 2}) {
        auto changed = options; changed.id = fault == 0 ? 99 : 10;
        if (fault == 1) { changed.tick_interval = std::chrono::milliseconds{0}; }
        if (fault == 2) { changed.rpc_timeout = std::chrono::milliseconds{0}; }
        Reject([&] { rpc::NodeServer server(changed); }, "Bad runtime config rejected before Store creation");
    }
}
}  // namespace

int main() {
    try {
        TestAllMessageKindsSurviveWireEncoding(); TestInvalidEnvelopeIdentityAndBody(); TestOperationValidationAndAdminNoOpRejection();
        TestAppendAndVoteMetadataValidation(); TestAppendCountAndByteLimits(); TestStatusFieldsAndUnknownLeader();
        TestMemberParsingAndLoopbackRestriction(); TestRuntimeOptionsRejectBeforeOpeningFiles();
        std::cout << "All 8 gRPC codec/config tests passed.\n";
    } catch (const std::exception& error) { std::cerr << "gRPC codec test failed: " << error.what() << '\n'; return 1; }
}
