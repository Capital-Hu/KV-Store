#include "process_fixture.h"
#include "kvstore/rpc/client.h"

namespace {
using Code = rpc::ClientCode;

rpc::ClientOptions Options(const Cluster& cluster) {
    rpc::ClientOptions result; result.members = rpc::ParseMembers(cluster.members);
    result.timeout = 3000ms; result.rpc_timeout = 3000ms; return result;
}
wire::NodeStatus Status(Cluster& cluster, int node) {
    wire::NodeStatus status; Check(cluster.Inspect(node, status).ok(), "Inspect snapshot node"); return status;
}

void TestRestartedFollowerCatchesUpFromSnapshot() {
    extra_node_arguments = {"--snapshot-entries", "20", "--snapshot-retain", "5"};
    Cluster cluster; rpc::Client client(Options(cluster));
    const auto leader = cluster.Leader();
    const auto lagging = (leader + 1) % 3;
    Check(client.Put("before", "crash").code == Code::Ok, "Write before the follower stops");
    cluster.children[lagging].Stop(SIGKILL);

    for (int i = 0; i < 80; ++i) {
        const auto written = client.Put("key" + std::to_string(i % 16), "value" + std::to_string(i));
        Check(written.code == Code::Ok, "Majority write " + std::to_string(i) + ": " + written.message);
    }
    Check(client.Delete("key3").code == Code::Ok, "Majority delete");
    Until([&] { return Status(cluster, leader).log_base_index() > 10; }, "Leader truncates its log");

    cluster.Start(lagging, "existing"); cluster.WaitUp(lagging);
    Until([&] { return Status(cluster, lagging).last_applied() >= Status(cluster, leader).log_base_index() && Status(cluster, lagging).log_base_index() > 0; },
          "Restarted follower installs a snapshot", 20s);
    Check(client.Put("fence", "after").code == Code::Ok, "Write after catch-up");
    Until([&] { return cluster.Get(lagging, "fence") == "after"; }, "Follower resumes log replication", 20s);
    Check(cluster.Get(lagging, "before") == "crash" && cluster.Get(lagging, "key15") == "value79" && !cluster.Get(lagging, "key3"), "Follower state comes from the snapshot");

    cluster.children[leader].Stop(SIGKILL);
    cluster.Leader(leader);
    Until([&] { const auto read = client.Get("key15"); return read.code == Code::Ok && read.value == "value79"; }, "Survivors serve after the old leader dies");
    Check(client.Put("new", "term").code == Code::Ok, "Survivors accept writes");
    cluster.Start(leader, "existing"); cluster.WaitUp(leader);
    Until([&] { return cluster.Get(leader, "new") == "term"; }, "Old leader rejoins with a truncated log", 20s);

    for (auto& child : cluster.children) { child.Stop(); }
    for (int i = 0; i < 3; ++i) { cluster.Start(i, "existing"); }
    for (int i = 0; i < 3; ++i) { cluster.WaitUp(i); Check(cluster.Get(i, "key15") == "value79" && !cluster.Get(i, "key3"), "Full restart keeps truncated state"); }
    Until([&] { const auto read = client.Get("new"); return read.code == Code::Ok && read.value == "term"; }, "Cluster serves after full restart");
    std::cout << "Snapshot catch-up, leader change and full restart: passed\n" << std::flush;
}
}

int main(int argc, char** argv) {
    try {
        Check(argc == 3, "Provide node and admin binaries"); node_binary = argv[1]; admin_binary = argv[2];
        TestRestartedFollowerCatchesUpFromSnapshot();
        return 0;
    } catch (const std::exception& error) { std::cerr << "Snapshot process test failed: " << error.what() << '\n'; return 1; }
}
