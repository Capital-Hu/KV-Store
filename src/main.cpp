#include "kvstore/memtable.h"

#include <iostream>

int main() {
    kvstore::MemTable table;

    table.Put("name", "Alice");
    std::cout << "After Put: " << table.Get("name").value_or("<not found>") << '\n';

    table.Put("name", "Bob");
    std::cout << "After overwrite: " << table.Get("name").value_or("<not found>") << '\n';

    table.Delete("name");
    std::cout << "After Delete: " << table.Get("name").value_or("<not found>") << '\n';

    return 0;
}
