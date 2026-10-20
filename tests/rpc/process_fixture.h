#pragma once

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
    void Signal(int signal) { Check(pid_ != 0 && ::kill(pid_, signal) == 0, "Signal owned child"); }
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
    explicit Cluster(bool start_all = true,
                     std::function<std::string(const std::array<std::string, 3>&)> route_factory = {}) : addresses(Addresses()) {
        for (std::size_t i = 0; i < 3; ++i) {
            if (i != 0) { members += ','; }
            members += std::to_string(i + 1) + '=' + addresses[i];
            stubs[i] = wire::NodeAdmin::NewStub(LocalChannel(addresses[i]));
        }
        if (route_factory) { raft_routes = route_factory(addresses); }
        for (int i = 0; i < (start_all ? 3 : 1); ++i) { Start(i, "new"); }
        for (int i = 0; i < (start_all ? 3 : 1); ++i) { WaitUp(i); }
    }
    void Start(int node, const std::string& mode) {
        std::vector<std::string> arguments{node_binary, "--id", std::to_string(node + 1), "--directory", directory.Path("node" + std::to_string(node + 1)),
                                           "--mode", mode, "--members", members};
        if (!raft_routes.empty()) { arguments.insert(arguments.end(), {"--raft-routes", raft_routes}); }
        children[node].Start(std::move(arguments), directory.Path("node" + std::to_string(node + 1) + ".log"));
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
    std::string raft_routes;
    std::array<std::unique_ptr<wire::NodeAdmin::Stub>, 3> stubs;
    std::array<Child, 3> children;
};

}  // namespace
