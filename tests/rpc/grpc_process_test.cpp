#include "kvstore/rpc/node_server.h"
#include "rpc/protocol_codec.h"
#include "raft_node.grpc.pb.h"

#include <grpcpp/grpcpp.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <spawn.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <csignal>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <iterator>
#include <mutex>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <system_error>
#include <thread>

extern char** environ;
namespace {
namespace wire = kvstore::wire;
namespace rpc = kvstore::rpc;
namespace raft = kvstore::raft;
using namespace std::chrono_literals;
std::string node_binary, admin_binary;

std::shared_ptr<grpc::Channel> LocalChannel(const std::string& address) {
    grpc::ChannelArguments arguments; arguments.SetInt(GRPC_ARG_ENABLE_HTTP_PROXY, 0);
    return grpc::CreateCustomChannel(address, grpc::InsecureChannelCredentials(), arguments);
}

void Check(bool condition, const std::string& message) { if (!condition) { throw std::runtime_error(message); } }
void Until(const std::function<bool()>& predicate, const std::string& message, std::chrono::seconds timeout = 10s) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    do { if (predicate()) { return; } std::this_thread::sleep_for(40ms); } while (std::chrono::steady_clock::now() < deadline);
    throw std::runtime_error(message);
}
class Directory {
public:
    Directory() {
        char pattern[] = "/tmp/kvstore-grpc-XXXXXX"; const auto path = ::mkdtemp(pattern);
        Check(path != nullptr, "Create process test directory"); path_ = path;
    }
    ~Directory() { std::error_code ignored; std::filesystem::remove_all(path_, ignored); }
    std::string Path(const std::string& name) const { return (path_ / name).string(); }
private:
    std::filesystem::path path_;
};
std::string Read(const std::string& path) {
    std::ifstream input(path); return {std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
}

class Child {
public:
    ~Child() {
        if (pid_ == 0) { return; }
        ::kill(pid_, SIGTERM);
        for (int i = 0; i < 100; ++i) {
            int status = 0; const auto result = ::waitpid(pid_, &status, WNOHANG);
            if (result == pid_ || (result == -1 && errno == ECHILD)) { pid_ = 0; return; }
            std::this_thread::sleep_for(10ms);
        }
        ::kill(pid_, SIGKILL); while (::waitpid(pid_, nullptr, 0) == -1 && errno == EINTR) {} pid_ = 0;
    }
    void Start(std::vector<std::string> arguments, const std::string& log) {
        Check(pid_ == 0, "Child already running"); log_ = log;
        std::vector<char*> pointers; for (auto& argument : arguments) { pointers.push_back(argument.data()); } pointers.push_back(nullptr);
        posix_spawn_file_actions_t actions;
        Check(::posix_spawn_file_actions_init(&actions) == 0, "Initialize spawn actions");
        struct Cleanup { posix_spawn_file_actions_t* actions; ~Cleanup() { ::posix_spawn_file_actions_destroy(actions); } } cleanup{&actions};
        Check(::posix_spawn_file_actions_addopen(&actions, STDOUT_FILENO, log.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0600) == 0 &&
              ::posix_spawn_file_actions_adddup2(&actions, STDOUT_FILENO, STDERR_FILENO) == 0, "Redirect child logs");
        pid_t spawned = 0;
        Check(::posix_spawn(&spawned, arguments[0].c_str(), &actions, nullptr, pointers.data(), environ) == 0, "Start child process");
        pid_ = spawned;
    }
    int Wait() {
        Check(pid_ != 0, "No child to wait for");
        int status = 0;
        Until([&] {
            const auto result = ::waitpid(pid_, &status, WNOHANG);
            if (result == pid_) { pid_ = 0; return true; }
            Check(result == 0 || (result == -1 && errno == EINTR), "Wait for child"); return false;
        }, "Child shutdown timed out: " + Read(log_), 5s);
        return status;
    }
    void Stop(int signal = SIGTERM) {
        Check(pid_ != 0 && ::kill(pid_, signal) == 0, "Signal child"); const auto status = Wait();
        Check(signal == SIGKILL ? WIFSIGNALED(status) && WTERMSIG(status) == SIGKILL : WIFEXITED(status) && WEXITSTATUS(status) == 0,
              "Expected clean shutdown or selected crash: " + Read(log_));
    }
private:
    pid_t pid_ = 0;
    std::string log_;
};

std::array<std::string, 3> Addresses() {
    std::array<int, 3> sockets{-1, -1, -1};
    struct Close { std::array<int, 3>& descriptors; ~Close() { for (const auto fd : descriptors) { if (fd != -1) { ::close(fd); } } } } cleanup{sockets};
    std::array<std::string, 3> result;
    for (std::size_t i = 0; i < sockets.size(); ++i) {
        sockets[i] = ::socket(AF_INET, SOCK_STREAM, 0);
        if (sockets[i] < 0) { throw std::system_error(errno, std::generic_category(), "Create loopback socket"); }
        sockaddr_in address{}; address.sin_family = AF_INET; address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        if (::bind(sockets[i], reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0) {
            throw std::system_error(errno, std::generic_category(), "Reserve loopback port");
        }
        socklen_t size = sizeof(address); Check(::getsockname(sockets[i], reinterpret_cast<sockaddr*>(&address), &size) == 0, "Find selected port");
        result[i] = "127.0.0.1:" + std::to_string(ntohs(address.sin_port));
    }
    return result;
}

class Cluster {
public:
    explicit Cluster(bool start_all = true) : addresses(Addresses()) {
        for (std::size_t i = 0; i < 3; ++i) {
            if (i != 0) { members += ','; }
            members += std::to_string(i + 1) + '=' + addresses[i];
            stubs[i] = wire::NodeAdmin::NewStub(LocalChannel(addresses[i]));
        }
        for (int i = 0; i < (start_all ? 3 : 1); ++i) { Start(i, "new"); }
        for (int i = 0; i < (start_all ? 3 : 1); ++i) { WaitUp(i); }
    }
    void Start(int node, const std::string& mode) {
        children[node].Start({node_binary, "--id", std::to_string(node + 1), "--directory", directory.Path("node" + std::to_string(node + 1)),
                             "--mode", mode, "--members", members}, directory.Path("node" + std::to_string(node + 1) + ".log"));
    }
    void WaitUp(int node) {
        try { Until([&] { wire::NodeStatus status; return Inspect(node, status).ok(); }, "Node did not start"); }
        catch (const std::exception& error) { throw std::runtime_error(std::string(error.what()) + ": " + Read(directory.Path("node" + std::to_string(node + 1) + ".log"))); }
    }
    grpc::Status Inspect(int node, wire::NodeStatus& response) {
        grpc::ClientContext context; context.set_deadline(std::chrono::system_clock::now() + 250ms);
        wire::Empty request; return stubs[node]->Inspect(&context, request, &response);
    }
    int Leader(int excluded = -1) {
        int result = -1;
        Until([&] {
            result = -1;
            for (int i = 0; i < 3; ++i) {
                if (i == excluded) { continue; } wire::NodeStatus status;
                if (Inspect(i, status).ok() && status.role() == wire::LEADER) { if (result != -1) { return false; } result = i; }
            }
            return result != -1;
        }, "Connected majority did not elect a leader");
        return result;
    }
    grpc::Status ProposeAt(int node, wire::Operation operation, const std::string& key, const std::string& value, wire::Proposal& proposal) {
        grpc::ClientContext context; context.set_deadline(std::chrono::system_clock::now() + 3s);
        wire::Command command; command.set_operation(operation); command.set_key(key); command.set_value(value);
        return stubs[node]->Propose(&context, command, &proposal);
    }
    wire::Proposal Put(int leader, const std::string& key, const std::string& value) {
        wire::Proposal result; const auto status = ProposeAt(leader, wire::PUT, key, value, result);
        Check(status.ok(), "Leader accepts proposal: " + status.error_message()); return result;
    }
    std::optional<std::string> Get(int node, const std::string& key) {
        grpc::ClientContext context; context.set_deadline(std::chrono::system_clock::now() + 1s);
        wire::GetRequest request; wire::GetResponse response; request.set_key(key);
        const auto status = stubs[node]->LocalGet(&context, request, &response);
        Check(status.ok(), "LocalGet RPC succeeds: " + status.error_message());
        return response.found() ? std::optional<std::string>{response.value()} : std::nullopt;
    }
    void WaitApplied(const wire::Proposal& proposal, int excluded = -1) {
        Until([&] {
            for (int i = 0; i < 3; ++i) {
                if (i == excluded) { continue; }
                grpc::ClientContext context; context.set_deadline(std::chrono::system_clock::now() + 300ms);
                wire::AppliedResponse response;
                if (!stubs[i]->Applied(&context, proposal, &response).ok() || !response.applied()) { return false; }
            }
            return true;
        }, "Proposal did not apply on expected nodes");
    }
    Directory directory;
    std::array<std::string, 3> addresses;
    std::string members;
    std::array<std::unique_ptr<wire::NodeAdmin::Stub>, 3> stubs;
    std::array<Child, 3> children;
};

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
