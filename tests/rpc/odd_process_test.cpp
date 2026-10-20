#include "fault_network.h"
#include "kvstore/rpc/client.h"

namespace {
using Code = rpc::ClientCode;
rpc::ClientOptions Options(const Cluster& cluster, std::chrono::milliseconds timeout = 3000ms) {
    rpc::ClientOptions result; result.members = rpc::ParseMembers(cluster.members);
    result.timeout = timeout; result.rpc_timeout = timeout; return result;
}
wire::Proposal Confirm(const rpc::ClientResult& result) {
    Check(result.code == Code::Ok && result.proposal.has_value(), "Client confirms committed operation: " + result.message);
    wire::Proposal proposal; proposal.set_index(result.proposal->index); proposal.set_term(result.proposal->term); return proposal;
}
wire::WriteReply DirectWrite(Cluster& cluster, int node, const std::string& key, const std::string& value) {
    auto stub = wire::KeyValue::NewStub(LocalChannel(cluster.addresses[node]));
    grpc::ClientContext context; context.set_deadline(std::chrono::system_clock::now() + 1600ms);
    wire::Command command; command.set_operation(wire::PUT); command.set_key(key); command.set_value(value);
    wire::WriteReply result;
    Check(stub->Write(&context, command, &result).ok(), "Direct business write RPC returns outcome"); return result;
}
int LeaderIn(Cluster& cluster, const std::vector<bool>& included) {
    int result = -1;
    Until([&] {
        result = -1;
        for (std::size_t i = 0; i < included.size(); ++i) {
            if (!included[i]) { continue; }
            wire::NodeStatus status;
            if (cluster.Inspect(static_cast<int>(i), status).ok() && status.role() == wire::LEADER) {
                if (result != -1) { return false; } result = static_cast<int>(i);
            }
        }
        return result != -1;
    }, "Surviving quorum elects one leader");
    return result;
}

void TestRoundTripAndRestart(std::size_t count) {
    Cluster cluster(true, {}, count); rpc::Client client(Options(cluster)); cluster.Leader();
    cluster.WaitApplied(Confirm(client.Put("name", "Alice")));
    const auto read = client.Get("name"); Check(read.code == Code::Ok && read.value == "Alice", "Odd-sized client consistent read");
    cluster.WaitApplied(Confirm(client.Delete("name")));
    const auto missing = client.Get("name"); Check(missing.code == Code::Ok && !missing.value, "Odd-sized delete remains absent");
    cluster.WaitApplied(Confirm(client.Put("name", "Bob")));
    for (auto& child : cluster.children) { child.Stop(); }
    for (std::size_t i = 0; i < count; ++i) { cluster.Start(static_cast<int>(i), "existing"); }
    for (std::size_t i = 0; i < count; ++i) {
        cluster.WaitUp(static_cast<int>(i)); Check(cluster.Get(static_cast<int>(i), "name") == "Bob", "Reopened nodes restore committed state");
    }
    Until([&] { const auto result = client.Get("name"); return result.code == Code::Ok && result.value == "Bob"; }, "Client reads after full restart");
    std::cout << count << "-node business operations and full restart: passed\n" << std::flush;
}

void TestFiveNodePartition() {
    FaultCluster fixture(5); auto& cluster = fixture.cluster;
    rpc::Client client(Options(cluster)); const auto old_leader = cluster.Leader();
    cluster.WaitApplied(Confirm(client.Put("name", "Alice")));
    std::vector<bool> majority(5, true); majority[old_leader] = false; majority[(old_leader + 1) % 5] = false;
    fixture.network->Configure([&](int from, int to) {
        FaultNetwork::Rule rule; rule.blocked = majority[from] != majority[to]; return rule;
    });
    const auto leader = LeaderIn(cluster, majority);
    const auto written = DirectWrite(cluster, leader, "name", "Carol");
    Check(written.outcome() == wire::CLIENT_OK && written.has_proposal(), "Three of five can confirm a write");
    const auto refused = DirectWrite(cluster, old_leader, "orphan", "minority");
    Check(refused.outcome() != wire::CLIENT_OK, "Two of five cannot confirm a write");
    {
        auto stub = wire::KeyValue::NewStub(LocalChannel(cluster.addresses[old_leader]));
        grpc::ClientContext context; context.set_deadline(std::chrono::system_clock::now() + 1600ms);
        wire::GetRequest input; input.set_key("name"); wire::ReadReply output;
        const auto status = stub->Read(&context, input, &output);
        Check(!status.ok() || output.outcome() != wire::CLIENT_OK, "Minority cannot return a successful stale read");
    }
    fixture.network->Heal();
    Until([&] { const auto result = client.Get("name"); return result.code == Code::Ok && result.value == "Carol"; }, "Healed five-node read preserves majority write");
    cluster.WaitApplied(Confirm(client.Put("fence", "ready")));
    for (int i = 0; i < 5; ++i) {
        Check(cluster.Get(i, "name") == "Carol" && !cluster.Get(i, "orphan"), "Healed members repair minority suffix");
    }
    std::cout << "Five-node 3/2 partition, minority rejection and healing: passed\n" << std::flush;
}

void TestFiveNodeCrashQuorum() {
    Cluster cluster(true, {}, 5); rpc::Client client(Options(cluster)); const auto old_leader = cluster.Leader();
    cluster.WaitApplied(Confirm(client.Put("seed", "durable")));
    const auto other = (old_leader + 1) % 5;
    cluster.children[old_leader].Stop(SIGKILL); cluster.children[other].Stop(SIGKILL);
    std::vector<bool> alive(5, true); alive[old_leader] = alive[other] = false;
    const auto leader = LeaderIn(cluster, alive);
    Confirm(client.Put("surviving", "majority"));
    const auto read = client.Get("surviving"); Check(read.code == Code::Ok && read.value == "majority", "Three surviving nodes remain usable");
    int third = -1;
    for (int i = 0; i < 5; ++i) { if (alive[i] && i != leader) { third = i; break; } }
    cluster.children[third].Stop(SIGKILL);
    const auto uncertain = DirectWrite(cluster, leader, "pending", "unknown");
    Check(uncertain.outcome() != wire::CLIENT_OK, "Two survivors cannot confirm");
    rpc::Client bounded(Options(cluster, 700ms));
    Check(bounded.Get("seed").code == Code::Unavailable, "Two survivors do not serve consistent old reads");
    for (const auto node : {old_leader, other, third}) { cluster.Start(node, "existing"); cluster.WaitUp(node); }
    Until([&] { const auto result = client.Get("surviving"); return result.code == Code::Ok && result.value == "majority"; }, "Restored quorum preserves confirmed write");
    cluster.WaitApplied(Confirm(client.Put("fence", "restored")));
    for (int i = 0; i < 5; ++i) {
        Check(cluster.Get(i, "seed") == "durable" && cluster.Get(i, "surviving") == "majority", "Crashed nodes catch up");
    }
    std::cout << "Five-node two/three crashes and recovery: passed\n" << std::flush;
}

void TestInvalidMembershipBeforeFiles() {
    Directory directory; const auto addresses = Addresses(4);
    for (const auto count : {2u, 4u}) {
        std::string members;
        for (std::size_t i = 0; i < count; ++i) { if (i) { members += ','; } members += std::to_string(i + 1) + '=' + addresses[i]; }
        const auto data = directory.Path("even" + std::to_string(count));
        Child child; child.Start({node_binary, "--id", "1", "--directory", data, "--mode", "new", "--members", members}, directory.Path("even.log"));
        const auto status = child.Wait();
        Check(WIFEXITED(status) && WEXITSTATUS(status) == 2 && !std::filesystem::exists(data), "Even count rejected before directory creation");
        bool rejected = false;
        try { rpc::ClientOptions options; options.members = {{1,addresses[0]},{2,addresses[1]}}; rpc::Client client(options); }
        catch (const std::invalid_argument&) { rejected = true; }
        Check(rejected, "Client independently rejects even membership");
    }
}
}

int main(int argc, char** argv) {
    try {
        Check(argc == 3, "Provide node and admin binaries"); node_binary = argv[1]; admin_binary = argv[2];
        for (const auto count : {1u, 5u, 7u}) { TestRoundTripAndRestart(count); }
        TestFiveNodePartition(); TestFiveNodeCrashQuorum(); TestInvalidMembershipBeforeFiles();
        std::cout << "All odd-sized process tests passed\n"; return 0;
    } catch (const std::exception& error) { std::cerr << "Odd process test failed: " << error.what() << '\n'; return 1; }
}
