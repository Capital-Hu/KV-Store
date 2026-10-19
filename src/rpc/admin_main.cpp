#include "kvstore/rpc/node_server.h"
#include "raft_node.grpc.pb.h"

#include <grpcpp/grpcpp.h>
#include <iostream>
#include <stdexcept>

int main(int argc, char** argv) {
    constexpr auto usage = "kvadmin 127.0.0.1:PORT status | propose-put KEY VALUE | propose-delete KEY | local-get KEY | applied INDEX TERM";
    try {
        if (argc == 2 && std::string(argv[1]) == "--help") { std::cout << usage << '\n'; return 0; }
        if (argc < 3) { throw std::invalid_argument(usage); }
        kvstore::rpc::ValidateAddress(argv[1]);
        const std::string operation = argv[2];
        grpc::ChannelArguments arguments; arguments.SetInt(GRPC_ARG_ENABLE_HTTP_PROXY, 0);
        auto stub = kvstore::wire::NodeAdmin::NewStub(grpc::CreateCustomChannel(argv[1], grpc::InsecureChannelCredentials(), arguments));
        grpc::ClientContext context;
        context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds{2});
        grpc::Status status;
        if (operation == "status" && argc == 3) {
            kvstore::wire::Empty request; kvstore::wire::NodeStatus response;
            status = stub->Inspect(&context, request, &response);
            if (status.ok()) {
                std::cout << "id=" << response.id() << " role=" << kvstore::wire::Role_Name(response.role())
                          << " term=" << response.term() << " leader=" << response.leader_id()
                          << " log=" << response.last_log_index() << " commit=" << response.commit_index()
                          << " applied=" << response.last_applied() << '\n';
            }
        } else if ((operation == "propose-put" && argc == 5) || (operation == "propose-delete" && argc == 4)) {
            kvstore::wire::Command request; kvstore::wire::Proposal response;
            request.set_operation(operation == "propose-put" ? kvstore::wire::PUT : kvstore::wire::DELETE);
            request.set_key(argv[3]); if (operation == "propose-put") { request.set_value(argv[4]); }
            status = stub->Propose(&context, request, &response);
            if (status.ok()) { std::cout << "ACCEPTED index=" << response.index() << " term=" << response.term() << " (not a commit acknowledgement)\n"; }
        } else if (operation == "local-get" && argc == 4) {
            kvstore::wire::GetRequest request; kvstore::wire::GetResponse response; request.set_key(argv[3]);
            status = stub->LocalGet(&context, request, &response);
            if (status.ok()) {
                if (response.found()) { std::cout.write(response.value().data(), response.value().size()); std::cout << '\n'; }
                else { std::cout << "<not found>\n"; return 3; }
            }
        } else if (operation == "applied" && argc == 5) {
            kvstore::wire::Proposal request; kvstore::wire::AppliedResponse response;
            request.set_index(kvstore::rpc::ParseUnsigned(argv[3])); request.set_term(kvstore::rpc::ParseUnsigned(argv[4]));
            status = stub->Applied(&context, request, &response);
            if (status.ok()) { std::cout << (response.applied() ? "true" : "false") << '\n'; }
        } else { throw std::invalid_argument(usage); }
        if (!status.ok()) { std::cerr << "RPC " << status.error_code() << ": " << status.error_message() << '\n'; return 1; }
        return 0;
    } catch (const std::exception& error) { std::cerr << error.what() << '\n'; return 2; }
}
