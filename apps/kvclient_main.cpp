#include "kvstore/rpc/client.h"

#include <iostream>
#include <stdexcept>

int main(int argc, char** argv) {
    constexpr auto usage = "kvclient MEMBERS [--timeout-ms N] put KEY VALUE | get KEY | delete KEY";
    try {
        if (argc == 2 && std::string(argv[1]) == "--help") { std::cout << usage << '\n'; return 0; }
        if (argc < 4) { throw std::invalid_argument(usage); }
        kvstore::rpc::ClientOptions options; options.members = kvstore::rpc::ParseMembers(argv[1]);
        int position = 2;
        if (std::string(argv[position]) == "--timeout-ms") {
            if (argc < 6) { throw std::invalid_argument(usage); }
            const auto value = kvstore::rpc::ParseUnsigned(argv[position + 1]);
            if (value == 0 || value > 30000) { throw std::invalid_argument("Timeout must be 1..30000 milliseconds"); }
            options.timeout = std::chrono::milliseconds{value}; position += 2;
        }
        const std::string operation = argv[position];
        if (!((operation == "put" && argc == position + 3) ||
            ((operation == "get" || operation == "delete") && argc == position + 2))) { throw std::invalid_argument(usage); }
        kvstore::rpc::Client client(std::move(options));
        const auto result = operation == "put" ? client.Put(argv[position + 1], argv[position + 2]) :
            operation == "delete" ? client.Delete(argv[position + 1]) : client.Get(argv[position + 1]);
        if (result.code == kvstore::rpc::ClientCode::Unknown) { std::cerr << "UNKNOWN: " << result.message << '\n'; return 4; }
        if (result.code != kvstore::rpc::ClientCode::Ok) { std::cerr << result.message << '\n'; return 1; }
        if (operation == "get") {
            if (!result.value) { std::cout << "<not found>\n"; return 3; }
            std::cout.write(result.value->data(), result.value->size()); std::cout << '\n';
        } else { std::cout << "OK index=" << result.proposal->index << " term=" << result.proposal->term << '\n'; }
        return 0;
    } catch (const std::exception& error) { std::cerr << error.what() << '\n'; return 2; }
}
