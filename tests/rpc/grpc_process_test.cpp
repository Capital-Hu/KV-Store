#include "process_fixture.h"

namespace {
void TestThreeProcessesReplicateRealValues() {
    Cluster cluster; const auto leader = cluster.Leader();
    const auto proposal = cluster.Put(leader, "name", "Alice"); cluster.WaitApplied(proposal);
    for (int i = 0; i < 3; ++i) { Check(cluster.Get(i, "name") == "Alice", "All three independent processes read Alice"); }
    wire::Proposal rejected;
    Check(cluster.ProposeAt((leader + 1) % 3, wire::PUT, "bad", "value", rejected).error_code() == grpc::StatusCode::FAILED_PRECONDITION,
          "Follower does not accept a client proposal");
    const std::string key("b\0k", 3), value("b\0v", 3);
    const auto binary = cluster.Put(leader, key, value); cluster.WaitApplied(binary);
    const auto empty = cluster.Put(leader, "", ""); cluster.WaitApplied(empty);
    for (int i = 0; i < 3; ++i) { Check(cluster.Get(i, key) == value && cluster.Get(i, "") == "", "RPC bytes preserve empty and binary values"); }
    wire::Proposal deletion; Check(cluster.ProposeAt(leader, wire::DELETE, "name", "", deletion).ok(), "Accept Delete");
    cluster.WaitApplied(deletion);
    for (int i = 0; i < 3; ++i) { Check(!cluster.Get(i, "name"), "Committed Delete reaches all Stores"); }
    int invocation = 0;
    const auto admin_call = [&](std::vector<std::string> arguments, int expected_exit = 0) {
        Child admin; const auto output = cluster.directory.Path("admin-" + std::to_string(invocation++) + ".log");
        arguments.insert(arguments.begin(), {admin_binary, cluster.addresses[leader]});
        admin.Start(std::move(arguments), output); const auto status = admin.Wait();
        Check(WIFEXITED(status) && WEXITSTATUS(status) == expected_exit, "Admin executable outcome: " + Read(output));
        return Read(output);
    };
    Check(admin_call({"status"}).find("role=LEADER") != std::string::npos, "Actual admin executable observes running node");
    const auto parse_receipt = [](const std::string& text) {
        std::istringstream input(text); std::string accepted, index, term; input >> accepted >> index >> term;
        Check(accepted == "ACCEPTED" && index.find("index=") == 0 && term.find("term=") == 0, "CLI labels local acceptance explicitly");
        wire::Proposal proposal; proposal.set_index(rpc::ParseUnsigned(index.substr(6))); proposal.set_term(rpc::ParseUnsigned(term.substr(5)));
        return proposal;
    };
    const auto cli_put = parse_receipt(admin_call({"propose-put", "cli-key", "cli-value"})); cluster.WaitApplied(cli_put);
    Check(admin_call({"local-get", "cli-key"}) == "cli-value\n", "CLI retrieves actual replicated value");
    Check(admin_call({"applied", std::to_string(cli_put.index()), std::to_string(cli_put.term())}) == "true\n", "CLI checks proposal application");
    const auto cli_delete = parse_receipt(admin_call({"propose-delete", "cli-key"})); cluster.WaitApplied(cli_delete);
    Check(admin_call({"local-get", "cli-key"}, 3) == "<not found>\n", "CLI distinguishes absence from RPC failure");
}

void TestMalformedAndOversizedRequestsDoNotMutateNode() {
    Cluster cluster; const auto leader = cluster.Leader(); wire::NodeStatus before;
    Check(cluster.Inspect(leader, before).ok(), "Observe current term");
    auto transport = wire::RaftTransport::NewStub(LocalChannel(cluster.addresses[leader]));
    for (int fault = 0; fault < 5; ++fault) {
        auto message = rpc::EncodeMessage({static_cast<raft::NodeId>((leader + 1) % 3 + 1), static_cast<raft::NodeId>(leader + 1), 99,
            raft::RequestVoteRequest{999, static_cast<raft::NodeId>((leader + 1) % 3 + 1), 0, 0}});
        if (fault == 0) { message.clear_body(); } else if (fault == 1) { message.set_from(99); message.mutable_vote_request()->set_candidate_id(99); }
        else if (fault == 2) { message.set_to(99); } else if (fault == 3) { message.mutable_vote_request()->set_candidate_id(99); }
        else { message.set_rpc_id(0); }
        grpc::ClientContext context; context.set_deadline(std::chrono::system_clock::now() + 1s); wire::Empty response;
        Check(transport->Deliver(&context, message, &response).error_code() == grpc::StatusCode::INVALID_ARGUMENT, "Malformed message rejected at transport boundary");
    }
    auto oversized = rpc::EncodeMessage({2, 1, 1, raft::AppendEntriesRequest{1, 2, 0, 0, {}, 0}});
    auto* entry = oversized.mutable_append_request()->add_entries(); entry->set_index(1); entry->set_term(1);
    entry->mutable_command()->set_operation(wire::PUT); entry->mutable_command()->set_value(std::string(rpc::kMaxRpcBytes + 1, 'x'));
    grpc::ClientContext context; context.set_deadline(std::chrono::system_clock::now() + 1s); wire::Empty response;
    Check(transport->Deliver(&context, oversized, &response).error_code() == grpc::StatusCode::RESOURCE_EXHAUSTED, "gRPC bounds incoming bytes");
    wire::Proposal invalid;
    Check(cluster.ProposeAt(leader, wire::NO_OP, "", "", invalid).error_code() == grpc::StatusCode::INVALID_ARGUMENT, "Admin cannot propose NoOp");
    wire::NodeStatus after; Check(cluster.Inspect(leader, after).ok() && after.term() == before.term(), "Rejected high-term garbage never enters Raft");
}

void TestUnavailablePeerAndGracefulRestart() {
    Cluster cluster; const auto leader = cluster.Leader(); const auto offline = (leader + 1) % 3;
    cluster.children[offline].Stop();
    const auto proposal = cluster.Put(leader, "available", "yes"); cluster.WaitApplied(proposal, offline);
    Check(cluster.Get(leader, "available") == "yes", "Unavailable peer does not block healthy majority");
    cluster.Start(offline, "existing"); cluster.WaitUp(offline); cluster.WaitApplied(proposal);
    Check(cluster.Get(offline, "available") == "yes", "Restarted process catches up over gRPC");
    for (auto& child : cluster.children) { child.Stop(); }
    for (int i = 0; i < 3; ++i) { cluster.Start(i, "existing"); }
    for (int i = 0; i < 3; ++i) { cluster.WaitUp(i); Check(cluster.Get(i, "available") == "yes", "Full restart restores applied KV from independent files"); }
}

void TestLeaderCrashFailoverAndRejoin() {
    Cluster cluster; const auto old = cluster.Leader(); const auto first = cluster.Put(old, "name", "Alice"); cluster.WaitApplied(first);
    cluster.children[old].Stop(SIGKILL);
    const auto replacement = cluster.Leader(old);
    const auto second = cluster.Put(replacement, "name", "Bob"); cluster.WaitApplied(second, old);
    Check(second.term() > first.term(), "Replacement leader uses newer term");
    cluster.Start(old, "existing"); cluster.WaitUp(old); cluster.WaitApplied(second);
    for (int i = 0; i < 3; ++i) { Check(cluster.Get(i, "name") == "Bob", "Old leader rejoins without losing new committed value"); }
}

void TestConcurrentAdminCallsSerializeWithRaft() {
    Cluster cluster; const auto leader = cluster.Leader();
    std::array<wire::Proposal, 12> receipts;
    std::vector<std::thread> workers; std::mutex errors_mutex; std::string error;
    for (int i = 0; i < 12; ++i) {
        workers.emplace_back([&, i] {
            try {
                receipts[i] = cluster.Put(leader, "k" + std::to_string(i), "v" + std::to_string(i));
                wire::NodeStatus status; Check(cluster.Inspect(leader, status).ok(), "Concurrent status succeeds");
                cluster.Get(leader, "k" + std::to_string(i));
            } catch (const std::exception& failure) { std::lock_guard<std::mutex> lock(errors_mutex); error = failure.what(); }
        });
    }
    for (auto& worker : workers) { worker.join(); } Check(error.empty(), "Concurrent admin calls: " + error);
    for (const auto& receipt : receipts) { cluster.WaitApplied(receipt); }
    for (int node = 0; node < 3; ++node) {
        for (int i = 0; i < 12; ++i) { Check(cluster.Get(node, "k" + std::to_string(i)) == "v" + std::to_string(i), "Parallel RPCs retain ordered state"); }
    }
}

void TestBindFailureInvalidStartupAndStopWithOfflinePeers() {
    Cluster cluster(false);
    Child collision;
    collision.Start({node_binary, "--id", "1", "--directory", cluster.directory.Path("collision"), "--mode", "new", "--members", cluster.members},
                    cluster.directory.Path("collision.log"));
    auto status = collision.Wait(); Check(WIFEXITED(status) && WEXITSTATUS(status) == 1, "Address collision fails rather than sharing listener");
    cluster.WaitUp(0);
    Child invalid;
    invalid.Start({node_binary, "--id", "99", "--directory", cluster.directory.Path("invalid"), "--mode", "new", "--members", cluster.members},
                  cluster.directory.Path("invalid.log"));
    status = invalid.Wait(); Check(WIFEXITED(status) && WEXITSTATUS(status) == 2 && !std::filesystem::exists(cluster.directory.Path("invalid")),
                                  "Bad membership fails before creating data");
    Until([&] { wire::NodeStatus observed; return cluster.Inspect(0, observed).ok() && observed.term() > 0; },
          "Lone node should attempt an election against its offline peers");
    const auto began = std::chrono::steady_clock::now(); cluster.children[0].Stop();
    Check(std::chrono::steady_clock::now() - began < 3s, "SIGTERM joins senders even when both peers are offline");
}
}  // namespace

int main(int argc, char** argv) {
    try {
        Check(argc == 3, "Pass kvnode and kvadmin executable paths"); node_binary = argv[1]; admin_binary = argv[2];
        TestThreeProcessesReplicateRealValues(); TestMalformedAndOversizedRequestsDoNotMutateNode();
        TestUnavailablePeerAndGracefulRestart(); TestLeaderCrashFailoverAndRejoin();
        TestConcurrentAdminCallsSerializeWithRaft(); TestBindFailureInvalidStartupAndStopWithOfflinePeers();
        std::cout << "All 6 gRPC process tests passed (3 independent processes, leader crash and restarts).\n";
    } catch (const std::exception& error) { std::cerr << "gRPC process test failed: " << error.what() << '\n'; return 1; }
}
