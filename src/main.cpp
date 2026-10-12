#include "kvstore/store.h"

#include <exception>
#include <iostream>
#include <string>

int main(int argc, char* argv[]) {
    if (argc < 3) {
        std::cerr << "Usage: kvstore <wal-path> put <key> <value>\n"
                     "       kvstore <wal-path> get <key>\n"
                     "       kvstore <wal-path> delete <key>\n"
                     "       kvstore <wal-path> recover-tail\n";
        return 2;
    }
    const std::string command = argv[2];
    if (!((command == "put" && argc == 5) ||
          ((command == "get" || command == "delete") && argc == 4) ||
          (command == "recover-tail" && argc == 3))) {
        std::cerr << "Invalid command or argument count.\n";
        return 2;
    }
    try {
        const auto mode = command == "recover-tail"
                              ? kvstore::WalRecoveryMode::TruncateIncompleteTail
                              : kvstore::WalRecoveryMode::Strict;
        kvstore::Store store(argv[1], mode);
        if (command == "put") {
            store.Put(argv[3], argv[4]);
            std::cout << "OK\n";
        } else if (command == "get") {
            const auto value = store.Get(argv[3]);
            if (!value.has_value()) {
                std::cout << "<not found>\n";
                return 1;
            }
            std::cout << value.value() << '\n';
        } else if (command == "delete") {
            std::cout << (store.Delete(argv[3]) ? "deleted\n" : "<not found>\n");
        } else {
            std::cout << "Truncated " << store.truncated_tail_bytes() << " incomplete tail bytes.\n";
        }
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "Error: " << error.what() << '\n';
        return 1;
    }
}
