#include "process_fixture.h"
#include "kvstore/rpc/client.h"

#include <atomic>
#include <future>

namespace {
using Code = rpc::ClientCode;
std::string client_binary;

rpc::ClientOptions Options(const Cluster& cluster, std::chrono::milliseconds timeout = 5s) {
    rpc::ClientOptions options; options.members = rpc::ParseMembers(cluster.members); options.timeout = timeout; return options;
}
wire::Proposal Receipt(const rpc::ClientResult& result) {
    Check(result.code == Code::Ok && result.proposal.has_value(), "Client write confirms application and returns receipt");
    wire::Proposal proposal; proposal.set_index(result.proposal->index); proposal.set_term(result.proposal->term); return proposal;
}

class PausedFollowers {
public:
    PausedFollowers(Cluster& cluster, int leader) : cluster_(cluster), leader_(leader) {
        for (int i = 0; i < 3; ++i) { if (i != leader_) { cluster_.children[i].Signal(SIGSTOP); } }
        // Drain any response already sent before the pause.
        std::this_thread::sleep_for(200ms);
    }
    ~PausedFollowers() {
        for (int i = 0; i < 3; ++i) {
            if (i != leader_) { try { cluster_.children[i].Signal(SIGCONT); } catch (...) {} }
        }
    }
private:
    Cluster& cluster_; int leader_;
};

void TestCommittedWritesAndBarrierReads() {
    Cluster cluster; const auto leader = cluster.Leader();
    auto follower = wire::KeyValue::NewStub(LocalChannel(cluster.addresses[(leader + 1) % 3]));
    grpc::ClientContext rejected_context; rejected_context.set_deadline(std::chrono::system_clock::now() + 1s);
    wire::Command rejected_command; rejected_command.set_operation(wire::PUT); rejected_command.set_key("rejected");
    wire::WriteReply rejected;
    Check(follower->Write(&rejected_context, rejected_command, &rejected).ok() &&
          rejected.outcome() == wire::CLIENT_NOT_LEADER && !rejected.has_proposal(), "Real follower rejects before proposing");
    auto options = Options(cluster);
    std::rotate(options.members.begin(), options.members.begin() + ((leader + 1) % 3), options.members.end());
    rpc::Client client(options);
    const auto write = client.Put("name", "Alice"); cluster.WaitApplied(Receipt(write));
    wire::NodeStatus before, after; Check(cluster.Inspect(leader, before).ok(), "Inspect leader");
    const auto read = client.Get("name");
    Check(read.code == Code::Ok && read.value == "Alice", "Barrier read sees acknowledged write");
    Check(cluster.Inspect(leader, after).ok() && after.last_log_index() > before.last_log_index(), "Read appends a fresh barrier");
    const std::string key("b\0k", 3), value("v\0x", 3);
    Receipt(client.Put(key, value)); Check(client.Get(key).value == value, "Client preserves binary bytes");
    Receipt(client.Put("", ""));
    const auto empty = client.Get(""); Check(empty.code == Code::Ok && empty.value && empty.value->empty(), "Empty value differs from missing key");
    Receipt(client.Delete("name")); const auto missing = client.Get("name");
    Check(missing.code == Code::Ok && !missing.value, "Committed delete is visible to consistent read");
    Receipt(client.Delete("absent"));
}

void TestDiscoveryAfterLeaderCrash() {
    Cluster cluster; const auto old = cluster.Leader(); rpc::Client client(Options(cluster));
    Receipt(client.Put("name", "Alice")); cluster.children[old].Stop(SIGKILL);
    const auto read = client.Get("name");
    Check(read.code == Code::Ok && read.value == "Alice", "Client discovers newly elected leader");
    const auto write = client.Put("name", "Bob"); Receipt(write);
    cluster.Start(old, "existing"); cluster.WaitUp(old); cluster.WaitApplied(Receipt(write));
    Check(cluster.Get(old, "name") == "Bob", "Old leader catches up with confirmed client write");
}

void TestNoQuorumNeverReturnsStaleSuccessAndUnknownMayCommit() {
    Cluster cluster; const auto leader = cluster.Leader();
    auto options = Options(cluster); options.rpc_timeout = 3s; rpc::Client client(options);
    Receipt(client.Put("name", "Alice"));
    rpc::ClientResult uncertain;
    {
        PausedFollowers paused(cluster, leader);
        auto stub = wire::KeyValue::NewStub(LocalChannel(cluster.addresses[leader]));
        grpc::ClientContext context; context.set_deadline(std::chrono::system_clock::now() + 2s);
        wire::GetRequest input; input.set_key("name"); wire::ReadReply reply;
        const auto status = stub->Read(&context, input, &reply);
        Check(status.ok() && reply.outcome() == wire::CLIENT_UNKNOWN && !reply.has_result(),
              "Leader with old local value cannot return a read without fresh quorum");
        Check(cluster.Get(leader, "name") == "Alice", "Diagnostic read still has local data");
        wire::NodeStatus before, after; Check(cluster.Inspect(leader, before).ok(), "Inspect minority");
        uncertain = client.Put("pending", "later");
        Check(uncertain.code == Code::Unknown && uncertain.proposal.has_value(), "No quorum yields explicit uncertain receipt");
        Check(cluster.Inspect(leader, after).ok() && after.last_log_index() == before.last_log_index() + 1,
              "Uncertain write is submitted only once");
        Check(!cluster.Get(leader, "pending"), "Uncommitted value is not visible");
        rpc::Client short_client(Options(cluster, 1500ms));
        const auto read = short_client.Get("name"); Check(read.code == Code::Unavailable && !read.value, "Client deadline never falls back to stale read");
    }
    Until([&] { return client.Get("name").code == Code::Ok; }, "Restored cluster can fence reads again");
    const auto settled = client.Get("pending");
    Check(settled.code == Code::Ok && (!settled.value || settled.value == "later"),
          "Unknown may be committed or overwritten during a new election; neither outcome was promised");
}

class FakeNode final : public wire::NodeAdmin::Service, public wire::KeyValue::Service {
public:
    FakeNode(std::string address, raft::NodeId identity) : id(identity) {
        grpc::ServerBuilder builder; builder.AddChannelArgument(GRPC_ARG_ALLOW_REUSEPORT, 0);
        builder.AddListeningPort(address, grpc::InsecureServerCredentials());
        builder.RegisterService(static_cast<wire::NodeAdmin::Service*>(this));
        builder.RegisterService(static_cast<wire::KeyValue::Service*>(this));
        server = builder.BuildAndStart(); Check(server != nullptr, "Start fake response server");
    }
    ~FakeNode() override { server->Shutdown(); server->Wait(); }
    grpc::Status Inspect(grpc::ServerContext*, const wire::Empty*, wire::NodeStatus* output) override {
        output->set_id(id); output->set_term(1); output->set_role(leader ? wire::LEADER : wire::FOLLOWER); return grpc::Status::OK;
    }
    grpc::Status Write(grpc::ServerContext*, const wire::Command* input, wire::WriteReply* output) override {
        ++writes;
        if (before_reply) { before_reply(*input); }
        if (mode == 1) { output->set_outcome(wire::CLIENT_NOT_LEADER); output->set_leader_id(2); }
        else if (mode == 2) { return {grpc::StatusCode::UNAVAILABLE, "Reply lost after possible application"}; }
        else if (mode == 4) { return {grpc::StatusCode::RESOURCE_EXHAUSTED, "Transport failure after possible application"}; }
        else {
            output->set_outcome(mode == 3 ? wire::CLIENT_UNKNOWN : wire::CLIENT_OK);
            output->mutable_proposal()->set_index(2); output->mutable_proposal()->set_term(1);
        }
        return grpc::Status::OK;
    }
    grpc::Status Read(grpc::ServerContext*, const wire::GetRequest*, wire::ReadReply* output) override {
        ++reads;
        if (mode == 2) { return {grpc::StatusCode::UNAVAILABLE, "Read reply lost"}; }
        output->set_outcome(wire::CLIENT_OK); output->mutable_result()->set_found(true); output->mutable_result()->set_value("fresh");
        return grpc::Status::OK;
    }
    raft::NodeId id; std::atomic<bool> leader{true}; std::atomic<int> mode{0}, writes{0}, reads{0};
    std::function<void(const wire::Command&)> before_reply;
    std::unique_ptr<grpc::Server> server;
};

void TestSafeRedirectAndNoReplayAfterLostReply() {
    const auto addresses = Addresses(); FakeNode first(addresses[0], 1), second(addresses[1], 2), third(addresses[2], 3);
    third.leader = false;
    rpc::ClientOptions options; options.members = {{1, addresses[0]}, {2, addresses[1]}, {3, addresses[2]}};
    options.timeout = 1s; rpc::Client client(options);
    first.mode = 1; Check(client.Put("k", "v").code == Code::Ok && first.writes == 1 && second.writes == 1,
                         "Explicit pre-proposal rejection allows retry at hinted leader");
    first.mode = 2; Check(client.Put("k", "v").code == Code::Unknown && first.writes == 2 && second.writes == 1,
                         "Lost reply is never replayed to another leader");
    first.mode = 3; Check(client.Delete("k").code == Code::Unknown && first.writes == 3 && second.writes == 1,
                         "Explicit uncertain outcome is also never replayed");
    first.mode = 2; Check(client.Get("k").value == "fresh" && first.reads == 1 && second.reads == 1,
                         "Reads may retry another leader without duplicating user writes");
    Check(client.Put("k", std::string(1024 * 1024, 'x')).code == Code::Rejected && first.writes == 3,
          "Invalid payload is rejected locally before any write RPC");
    first.leader = false; second.leader = false;
    Check(client.Put("k", "v").code == Code::Unavailable && first.writes == 3 && second.writes == 1,
          "No discovered leader means no write was submitted");
    first.leader = true; first.mode = 4;
    Check(client.Put("k", "v").code == Code::Unknown && first.writes == 4 && second.writes == 1,
          "Even a resource status after submission cannot certify non-application");
}

void TestLostReplyAfterRealCommitIsUnknownWithoutReplay() {
    Cluster cluster; cluster.Leader(); rpc::Client backing(Options(cluster));
    const auto addresses = Addresses(); FakeNode proxy(addresses[0], 1), second(addresses[1], 2), third(addresses[2], 3);
    std::atomic<bool> committed{false}; proxy.mode = 2;
    proxy.before_reply = [&](const wire::Command& command) {
        committed = backing.Put(command.key(), command.value()).code == Code::Ok;
    };
    rpc::ClientOptions options; options.members = {{1,addresses[0]}, {2,addresses[1]}, {3,addresses[2]}};
    options.rpc_timeout = 3s; rpc::Client client(options);
    const auto result = client.Put("lost-reply", "committed");
    Check(result.code == Code::Unknown && committed && proxy.writes == 1 && second.writes == 0 && third.writes == 0,
          "A real committed write with a lost reply is unknown and never replayed");
    const auto read = backing.Get("lost-reply");
    Check(read.code == Code::Ok && read.value == "committed", "Unknown did not undo the real committed state");
}

void TestConcurrentClientsAndActualCli() {
    Cluster cluster; cluster.Leader(); rpc::Client client(Options(cluster));
    std::vector<std::thread> workers;
    // Each thread writes a separate array element.
    std::array<int, 8> succeeded{};
    for (int i = 0; i < 8; ++i) {
        workers.emplace_back([&, i] {
            const auto key = "parallel" + std::to_string(i);
            const auto write = client.Put(key, key); const auto read = client.Get(key);
            succeeded[i] = write.code == Code::Ok && read.code == Code::Ok && read.value == key;
        });
    }
    for (auto& worker : workers) { worker.join(); }
    for (const auto success : succeeded) { Check(success, "Concurrent operations progress while RPC waits release state lock"); }
    int calls = 0;
    std::string cli_members = cluster.members;
    const auto invoke = [&](std::vector<std::string> arguments, int code) {
        Child cli; const auto log = cluster.directory.Path("client" + std::to_string(calls++) + ".log");
        arguments.insert(arguments.begin(), {client_binary, cli_members});
        const auto operation = arguments[2]; cli.Start(std::move(arguments), log);
        const auto status = cli.Wait(); Check(WIFEXITED(status) && WEXITSTATUS(status) == code, "Client CLI outcome for " + operation + ": " + Read(log));
        return Read(log);
    };
    Check(invoke({"put", "cli", "Alice"}, 0).find("OK index=") == 0, "CLI reports applied write");
    Check(invoke({"get", "cli"}, 0) == "Alice\n", "CLI consistent read");
    invoke({"delete", "cli"}, 0); invoke({"get", "cli"}, 3);
    invoke({"--timeout-ms", "0", "get", "cli"}, 2);
    const auto leader = cluster.Leader();
    cli_members.clear();
    for (int offset = 0; offset < 3; ++offset) {
        const auto index = (leader + offset) % 3;
        if (offset) { cli_members += ','; }
        cli_members += std::to_string(index + 1) + '=' + cluster.addresses[index];
    }
    {
        PausedFollowers paused(cluster, leader);
        Check(invoke({"--timeout-ms", "700", "put", "uncertain-cli", "v"}, 4).find("UNKNOWN:") == 0,
              "CLI distinguishes uncertain write from rejection");
    }
    Until([&] { return client.Get("uncertain-cli").code == Code::Ok; }, "Cluster resumes consistent reads");
    const auto settled = client.Get("uncertain-cli");
    Check(settled.code == Code::Ok && (!settled.value || settled.value == "v"), "CLI timeout promises no particular commit outcome");
}

void TestInvalidClientConfiguration() {
    rpc::ClientOptions options;
    bool rejected = false;
    try { rpc::Client client(options); } catch (const std::invalid_argument&) { rejected = true; }
    Check(rejected, "Client requires three members");
    const auto addresses = Addresses(); options.members = {{1,addresses[0]}, {2,addresses[1]}, {3,addresses[2]}};
    options.timeout = 0ms; rejected = false;
    try { rpc::Client client(options); } catch (const std::invalid_argument&) { rejected = true; }
    Check(rejected, "Client requires bounded positive timeout");
    options.timeout = 100ms; options.members[2].id = 2; rejected = false;
    try { rpc::Client client(options); } catch (const std::invalid_argument&) { rejected = true; }
    Check(rejected, "Client rejects duplicate identities");
}

void TestShutdownReleasesWaitingClientCall() {
    Cluster cluster; const auto leader = cluster.Leader(); rpc::Client client(Options(cluster));
    Receipt(client.Put("seed", "v")); PausedFollowers paused(cluster, leader);
    wire::NodeStatus before; Check(cluster.Inspect(leader, before).ok(), "Inspect before pending call");
    auto pending = std::async(std::launch::async, [&] {
        auto stub = wire::KeyValue::NewStub(LocalChannel(cluster.addresses[leader]));
        grpc::ClientContext context; context.set_deadline(std::chrono::system_clock::now() + 3s);
        wire::Command command; command.set_operation(wire::PUT); command.set_key("shutdown"); command.set_value("v");
        wire::WriteReply reply; const auto status = stub->Write(&context, command, &reply);
        return status.ok() && reply.outcome() == wire::CLIENT_OK;
    });
    Until([&] { wire::NodeStatus state; return cluster.Inspect(leader, state).ok() && state.last_log_index() > before.last_log_index(); },
          "Pending write reached leader log");
    cluster.children[leader].Stop();
    Check(!pending.get(), "Shutdown never turns an uncommitted waiting write into success");
}
}  // namespace

int main(int argc, char** argv) {
    try {
        Check(argc == 4, "Pass kvnode, kvadmin and kvclient paths"); node_binary = argv[1]; admin_binary = argv[2]; client_binary = argv[3];
        const auto run = [](const char* name, auto test) {
            try { test(); } catch (const std::exception& error) { throw std::runtime_error(std::string(name) + ": " + error.what()); }
        };
        run("Committed writes and reads", TestCommittedWritesAndBarrierReads);
        run("Failover discovery", TestDiscoveryAfterLeaderCrash);
        run("No quorum", TestNoQuorumNeverReturnsStaleSuccessAndUnknownMayCommit);
        run("Safe redirects", TestSafeRedirectAndNoReplayAfterLostReply);
        run("Lost reply after real commit", TestLostReplyAfterRealCommitIsUnknownWithoutReplay);
        run("Concurrent calls and CLI", TestConcurrentClientsAndActualCli);
        run("Invalid configuration", TestInvalidClientConfiguration);
        run("Shutdown", TestShutdownReleasesWaitingClientCall);
        std::cout << "All 8 client tests passed (commit acknowledgements, read barriers, discovery, uncertain outcomes).\n";
    } catch (const std::exception& error) { std::cerr << "Client test failed: " << error.what() << '\n'; return 1; }
}
