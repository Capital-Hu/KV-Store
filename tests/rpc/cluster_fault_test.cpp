#include "fault_network.h"
#include "kvstore/rpc/client.h"

#include <map>

namespace {
using Code = rpc::ClientCode;
using Model = std::map<std::string, std::optional<std::string>>;

rpc::ClientOptions Options(const Cluster& cluster, std::chrono::milliseconds timeout = 5s) {
    rpc::ClientOptions options; options.members = rpc::ParseMembers(cluster.members); options.timeout = timeout; return options;
}
wire::Proposal Confirm(const rpc::ClientResult& result) {
    Check(result.code == Code::Ok && result.proposal.has_value(), "Write must be confirmed applied: " + result.message);
    wire::Proposal proposal; proposal.set_index(result.proposal->index); proposal.set_term(result.proposal->term); return proposal;
}
wire::NodeStatus State(Cluster& cluster, int node) {
    wire::NodeStatus status; Check(cluster.Inspect(node, status).ok(), "Inspect reachable node"); return status;
}
wire::WriteReply WriteAt(Cluster& cluster, int node, const std::string& key, const std::string& value) {
    auto stub = wire::KeyValue::NewStub(LocalChannel(cluster.addresses[node]));
    grpc::ClientContext context; context.set_deadline(std::chrono::system_clock::now() + 3s);
    wire::Command input; input.set_operation(wire::PUT); input.set_key(key); input.set_value(value); wire::WriteReply output;
    const auto status = stub->Write(&context, input, &output);
    Check(status.ok(), "Direct write RPC returned: " + status.error_message()); return output;
}
wire::ReadReply ReadAt(Cluster& cluster, int node, const std::string& key) {
    auto stub = wire::KeyValue::NewStub(LocalChannel(cluster.addresses[node]));
    grpc::ClientContext context; context.set_deadline(std::chrono::system_clock::now() + 3s);
    wire::GetRequest input; input.set_key(key); wire::ReadReply output;
    const auto status = stub->Read(&context, input, &output);
    Check(status.ok(), "Direct read RPC returned: " + status.error_message()); return output;
}
void MatchAll(Cluster& cluster, const Model& model) {
    Until([&] {
        std::optional<std::uint64_t> commit;
        for (int i = 0; i < 3; ++i) {
            const auto state = State(cluster, i);
            if (state.commit_index() != state.last_applied()) { return false; }
            if (commit && *commit != state.commit_index()) { return false; }
            commit = state.commit_index();
            for (const auto& item : model) { if (cluster.Get(i, item.first) != item.second) { return false; } }
        }
        return true;
    }, "All nodes must converge on the acknowledged state and applied commit position", 15s);
}

void TestIsolatedLeaderAndConflictRepair() {
    FaultCluster fixture; auto& cluster = fixture.cluster; rpc::Client client(Options(cluster));
    const auto seed = Confirm(client.Put("name", "Alice")); cluster.WaitApplied(seed);
    const auto old = cluster.Leader(); const auto old_term = State(cluster, old).term();
    fixture.network->Isolate(old);
    const auto current = cluster.Leader(old);
    Check(State(cluster, current).term() > old_term && State(cluster, old).role() == wire::LEADER,
          "Minority leader remains alive while the majority elects a higher-term leader");
    const auto updated = Confirm(client.Put("name", "Bob")); cluster.WaitApplied(updated, old);
    Check(cluster.Get(old, "name") == "Alice", "Isolated old leader still has a stale local value");
    const auto unknown = WriteAt(cluster, old, "orphan", "uncommitted");
    Check(unknown.outcome() == wire::CLIENT_UNKNOWN && unknown.has_proposal(), "Minority cannot acknowledge a fresh write");
    const auto read = ReadAt(cluster, old, "name");
    Check(read.outcome() != wire::CLIENT_OK && !read.has_result(), "Stale leader cannot return Alice through consistent read");
    Check(State(cluster, old).commit_index() < unknown.proposal().index() && !cluster.Get(old, "orphan"),
          "Local append never implies commitment or Store application");
    fixture.network->Heal();
    Confirm(client.Put("fence", "after-heal"));
    MatchAll(cluster, {{"name", "Bob"}, {"orphan", std::nullopt}, {"fence", "after-heal"}});
    Check(State(cluster, old).role() == wire::FOLLOWER, "Old leader steps down and repairs its conflicting suffix");
    Check(fixture.network->Snapshot().dropped > 0, "Partition actually intercepted Raft traffic");
}

void TestFollowerIsolationDoesNotStopMajority() {
    FaultCluster fixture; auto& cluster = fixture.cluster; rpc::Client client(Options(cluster));
    cluster.WaitApplied(Confirm(client.Put("retired", "old")));
    const auto leader = cluster.Leader(), lagging = (leader + 1) % 3;
    fixture.network->Isolate(lagging);
    for (int i = 0; i < 6; ++i) { Confirm(client.Put("name", "version" + std::to_string(i))); }
    Confirm(client.Delete("retired"));
    const auto read = client.Get("name"); Check(read.code == Code::Ok && read.value == "version5", "Connected majority serves consistent requests");
    Check(cluster.Get(lagging, "retired") == "old", "Lagging follower has not applied the deletion");
    fixture.network->Heal();
    MatchAll(cluster, {{"name", "version5"}, {"retired", std::nullopt}});
    Check(fixture.network->Snapshot().dropped > 0, "Follower isolation dropped messages");
}

void TestOneWayLossAndUnknownLaterCommit() {
    FaultCluster fixture; auto& cluster = fixture.cluster; rpc::Client client(Options(cluster));
    cluster.WaitApplied(Confirm(client.Put("seed", "v"))); const auto leader = cluster.Leader();
    const auto term = State(cluster, leader).term();
    fixture.network->Configure([&](int from, int to) { FaultNetwork::Rule rule; rule.blocked = to == leader && from != leader; return rule; });
    const auto unknown = WriteAt(cluster, leader, "pending", "later");
    Check(unknown.outcome() == wire::CLIENT_UNKNOWN && unknown.has_proposal(), "Responses lost on one direction prevent a success acknowledgement");
    Check(State(cluster, leader).term() == term && State(cluster, leader).commit_index() < unknown.proposal().index(),
          "Leader heartbeats still flow, but no new quorum acknowledgement reaches it");
    for (int i = 0; i < 3; ++i) { Check(!cluster.Get(i, "pending"), "No node applies an unconfirmed new entry"); }
    fixture.network->Heal(); cluster.WaitApplied(unknown.proposal());
    MatchAll(cluster, {{"seed", "v"}, {"pending", "later"}});
}

void TestAllLinksPartitionedAndRecovery() {
    FaultCluster fixture; auto& cluster = fixture.cluster; rpc::Client client(Options(cluster));
    cluster.WaitApplied(Confirm(client.Put("seed", "saved"))); const auto leader = cluster.Leader();
    std::array<std::uint64_t, 3> committed{};
    for (int i = 0; i < 3; ++i) { committed[i] = State(cluster, i).commit_index(); }
    fixture.network->Configure([](int, int) { FaultNetwork::Rule rule; rule.blocked = true; return rule; });
    const auto unknown = WriteAt(cluster, leader, "uncertain", "maybe");
    Check(unknown.outcome() == wire::CLIENT_UNKNOWN, "An island cannot commit a fresh write");
    rpc::Client bounded(Options(cluster, 1600ms));
    const auto read = bounded.Get("seed"); Check(read.code == Code::Unavailable && !read.value, "Total partition never falls back to a stale successful read");
    for (int i = 0; i < 3; ++i) {
        Check(State(cluster, i).commit_index() == committed[i] && cluster.Get(i, "seed") == "saved", "No isolated node advances the saved commit prefix");
    }
    fixture.network->Heal();
    Until([&] { return client.Get("seed").code == Code::Ok; }, "Healed cluster restores quorum reads", 15s);
    Confirm(client.Put("uncertain", "resolved"));
    MatchAll(cluster, {{"seed", "saved"}, {"uncertain", "resolved"}});
}

void TestDelayedLostDuplicatedReorderedTraffic() {
    for (const int schedule : {1, 2}) {
        FaultCluster fixture; auto& cluster = fixture.cluster; rpc::Client client(Options(cluster));
        cluster.WaitApplied(Confirm(client.Put("seed", "initial")));
        fixture.network->Configure([&](int from, int to) {
            FaultNetwork::Rule rule; rule.delay = 5ms; rule.alternate_delay = 220ms;
            rule.drop_every = static_cast<std::size_t>(7 + (from + to + schedule) % 3);
            rule.duplicate_every = static_cast<std::size_t>(3 + schedule); return rule;
        });
        Model expected{{"seed", "initial"}}; int acknowledged = 0;
        for (int i = 0; i < 10; ++i) {
            const auto key = "key" + std::to_string(i % 4), value = "value" + std::to_string(i);
            const bool deleting = i % 3 == 2;
            const auto result = deleting ? client.Delete(key) : client.Put(key, value);
            if (result.code == Code::Ok) { expected[key] = deleting ? std::optional<std::string>{} : value; ++acknowledged; }
            else if (result.code == Code::Unknown) { expected.erase(key); }
            else { Check(result.code == Code::Unavailable, "Faults cannot turn valid input into rejection"); }
            if (i % 2 == 0) {
                const auto read = client.Get("seed");
                Check(read.code != Code::Ok || read.value == "initial", "Any successful read under faults returns the committed value");
            }
        }
        Check(acknowledged >= 4, "Bounded message faults still permit repeated majority progress");
        const auto stats = fixture.network->Snapshot();
        Check(stats.dropped > 0 && stats.delayed > 0 && stats.duplicated > 0 && stats.reordered > 0,
              "Loss, delay, duplication and reordering all occurred on real forwarded envelopes");
        Check(stats.overflow == 0, "Configured fault schedule did not rely on queue overflow");
        fixture.network->Heal(); Confirm(client.Put("recovered", "yes")); expected["recovered"] = "yes";
        MatchAll(cluster, expected);
        std::cout << "schedule=" << schedule << " confirmed=" << acknowledged << " dropped=" << stats.dropped
                  << " duplicate=" << stats.duplicated << " reorder=" << stats.reordered << '\n';
    }
}

void TestCrashDuringPartitionAndFullRestart() {
    FaultCluster fixture; auto& cluster = fixture.cluster; rpc::Client client(Options(cluster));
    cluster.WaitApplied(Confirm(client.Put("name", "Alice"))); const auto old = cluster.Leader();
    fixture.network->Isolate((old + 1) % 3); cluster.children[old].Stop(SIGKILL);
    rpc::Client bounded(Options(cluster, 1400ms));
    Check(bounded.Get("name").code == Code::Unavailable, "One crash plus a cut leaves no connected majority");
    fixture.network->Heal(); cluster.Leader(old);
    Confirm(client.Put("name", "Bob"));
    cluster.Start(old, "existing"); cluster.WaitUp(old);
    MatchAll(cluster, {{"name", "Bob"}});
    for (int i = 0; i < 3; ++i) { cluster.children[i].Stop(); }
    for (int i = 0; i < 3; ++i) { cluster.Start(i, "existing"); }
    for (int i = 0; i < 3; ++i) {
        cluster.WaitUp(i); Check(cluster.Get(i, "name") == "Bob", "Each reopened Store retains the acknowledged value before new writes");
    }
    const auto read = client.Get("name"); Check(read.code == Code::Ok && read.value == "Bob", "Cluster resumes consistent reads after full restart");
    MatchAll(cluster, {{"name", "Bob"}});
}

void TestInvalidRoutesFailBeforeDataCreation() {
    Directory directory; const auto addresses = Addresses();
    const auto members = "1=" + addresses[0] + ",2=" + addresses[1] + ",3=" + addresses[2];
    const std::vector<std::string> invalid{
        "1=" + addresses[0] + ",2=" + addresses[1] + ",4=" + addresses[2],
        "1=" + addresses[0] + ",2=" + addresses[0] + ",3=" + addresses[2],
        "1=" + addresses[0] + ",2=" + addresses[1]};
    for (std::size_t i = 0; i < invalid.size(); ++i) {
        Child node; const auto data = directory.Path("invalid" + std::to_string(i));
        node.Start({node_binary, "--id", "1", "--directory", data, "--mode", "new", "--members", members,
                    "--raft-routes", invalid[i]}, directory.Path("invalid.log"));
        const auto status = node.Wait();
        Check(WIFEXITED(status) && WEXITSTATUS(status) == 2 && !std::filesystem::exists(data),
              "Bad route IDs, duplicates and missing members fail before opening data");
    }
}
}  // namespace

int main(int argc, char** argv) {
    try {
        Check(argc == 3, "Pass kvnode and kvadmin paths"); node_binary = argv[1]; admin_binary = argv[2];
        const auto run = [](const char* name, auto test) {
            try { test(); std::cout << name << ": passed\n" << std::flush; }
            catch (const std::exception& error) { throw std::runtime_error(std::string(name) + ": " + error.what()); }
        };
        run("Leader partition", TestIsolatedLeaderAndConflictRepair);
        run("Follower partition", TestFollowerIsolationDoesNotStopMajority);
        run("One-way loss", TestOneWayLossAndUnknownLaterCommit);
        run("No majority", TestAllLinksPartitionedAndRecovery);
        run("Message fault schedules", TestDelayedLostDuplicatedReorderedTraffic);
        run("Crash and restart", TestCrashDuringPartitionAndFullRestart);
        run("Route validation", TestInvalidRoutesFailBeforeDataCreation);
        std::cout << "All 7 cluster fault tests passed (three real node processes and scheduled gRPC forwarding).\n";
    } catch (const std::exception& error) { std::cerr << "Cluster fault test failed: " << error.what() << '\n'; return 1; }
}
