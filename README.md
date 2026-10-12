# Replicated Key-Value Store

一个逐步实现、逐步讲解的 C++ 学习项目。先完成单机 LSM 存储引擎，再完成三节点 Raft 复制。两部分都是最终验收要求。

当前完成 **第 3 步：WAL 文件写入与启动回放**。Store 先追加并同步 WAL，再更新内存表；重新启动会按顺序回放日志恢复数据。主程序支持 `put/get/delete` 命令。当前按单线程、单个 WAL 所有者使用，尚未实现 SSTable。

## 从哪里看

- [GOAL.md](GOAL.md)：最终目标、完成条件、推进规则。
- [ROADMAP.md](ROADMAP.md)：第一阶段的拆分，以及第二阶段的方向。
- [第 0 步讲解](docs/steps/00-framework.md)：本次每个文件的作用、程序如何运行、C++ 与 Java 的区别。
- [第 1 步讲解](docs/steps/01-memtable.md)：内存表接口、实现、C++ 语法与验证结果。
- [第 2 步讲解](docs/steps/02-wal-record.md)：WAL 字节格式、校验、解码状态与 C++ 写法。
- [第 3 步讲解](docs/steps/03-persistent-store.md)：持久化顺序、启动回放、尾部恢复、文件资源与测试。
- `include/kvstore/memtable.h`：MemTable 的声明。
- `src/memtable.cpp`：MemTable 的实现。
- `src/main.cpp`：命令行入口。
- `tests/memtable_test.cpp`：内存读写语义测试。
- `include/kvstore/wal_record.h`：WAL 记录与编解码接口。
- `src/wal_record.cpp`：字节编码、解码和 CRC32 校验。
- `tests/wal_record_test.cpp`：WAL 格式测试。
- `include/kvstore/store.h`：持久化 Store 接口及恢复模式。
- `src/store.cpp`：文件生命周期、日志写入同步和回放。
- `tests/store_test.cpp`：持久化、恢复和写入故障测试。
- `CMakeLists.txt`：构建入口。

现在不预建 Raft、gRPC 或缓存模块；到对应步骤再引入文件和依赖。后续代码只在这个子文件夹中开发。

## 构建和运行

下面所有命令都在项目根目录执行。先在终端进入：

```sh
cd /Users/huxiangyu/Documents/CV/replicated-kv-store
```

### 方式一：直接编译，当前环境可用

需要支持 C++17 的编译器。这种方式不需要 CMake，也不依赖 VS Code 的运行按钮。

#### 编译并运行测试

```sh
mkdir -p build
c++ -std=c++17 -Wall -Wextra -Wpedantic -I include src/memtable.cpp tests/memtable_test.cpp -o build/memtable_test
./build/memtable_test
```

第一行创建输出目录，第二行编译并链接测试程序，第三行运行它。测试入口是 `tests/memtable_test.cpp` 中的 `main()`，运行后自动检查内存读写行为。

成功时输出：

```text
All 8 MemTable tests passed.
```

如果测试失败，会打印失败原因并返回非零退出码。编译失败时先处理编译错误，不继续运行旧的可执行文件。

#### 编译并运行项目程序

```sh
mkdir -p build
c++ -std=c++17 -Wall -Wextra -Wpedantic -I include src/memtable.cpp src/wal_record.cpp src/store.cpp src/main.cpp -o build/kvstore
./build/kvstore build/demo.wal put name Alice
./build/kvstore build/demo.wal get name
```

程序入口是 `src/main.cpp` 中的 `main()`。第二个参数位置的 `build/demo.wal` 是日志文件路径；文件会按需创建，但父目录必须已经存在。每条命令启动一个新进程，后面的 Get 会从前一个进程留下的 WAL 恢复数据。

输出：

```text
OK
Alice
```

#### 编译并运行 WAL 记录测试

```sh
mkdir -p build
c++ -std=c++17 -Wall -Wextra -Wpedantic -I include src/wal_record.cpp tests/wal_record_test.cpp -o build/wal_record_test
./build/wal_record_test
```

成功时输出 `All 12 WAL record tests passed.`。这组测试只处理内存中的字节，不会生成 WAL 文件。

#### 编译并运行持久化 Store 测试

```sh
mkdir -p build
c++ -std=c++17 -Wall -Wextra -Wpedantic -I include src/memtable.cpp src/wal_record.cpp src/store.cpp tests/store_test.cpp -o build/store_test
./build/store_test
```

成功时输出 `All 12 Store tests passed.`。测试在独立临时目录中创建 WAL，结束时清理；强杀和文件大小限制只用于测试子进程。

#### 这些编译参数是什么意思

| 参数或文件 | 作用 |
| --- | --- |
| `-std=c++17` | 使用 C++17 标准，支持 `std::optional` |
| `-Wall -Wextra -Wpedantic` | 开启常用编译警告，帮助发现问题 |
| `-I include` | 从项目的 `include/` 目录搜索头文件，因此能找到 `kvstore/memtable.h` |
| `src/memtable.cpp` / `src/wal_record.cpp` / `src/store.cpp` | 提供内存表、编解码和持久化实现，按要构建的程序选择 |
| `tests/*_test.cpp` / `src/main.cpp` | 各自是独立程序入口，一次选其中一个 |
| `-o build/...` | 指定生成的可执行文件位置 |

头文件通过 `#include` 读取，不需要把 `.h` 单独列到编译命令中。各个入口文件都有 `main()`，不能把它们一起构建成同一个可执行程序。

修改代码后，需要重新执行对应的编译命令，再运行；只运行 `./build/...` 不会自动重新编译。

### 方式二：通过 CMake 构建

需要 CMake 3.16 或以上版本，以及支持 C++17 的编译器。当前环境已有 CMake，可以使用这种方式。

首次配置构建目录时执行一次：

```sh
cmake -S . -B build -DBUILD_TESTING=ON
```

以后普通修改 `.cpp` 或 `.h` 后，直接使用下面相应的两行命令。第一行编译，第二行运行；每行成功后再执行下一行。删除 `build/` 或更改配置选项后，再执行上面的配置命令。

运行内存表测试：

```sh
cmake --build build --target memtable_test
./build/memtable_test
```

运行 WAL 记录测试：

```sh
cmake --build build --target wal_record_test
./build/wal_record_test
```

运行持久化 Store 测试：

```sh
cmake --build build --target store_test
./build/store_test
```

构建并运行项目程序：

```sh
cmake --build build --target kvstore
./build/kvstore build/demo.wal put name Alice
./build/kvstore build/demo.wal get name
```

Put 输出 `OK`，下一次启动的 Get 输出 `Alice`。也可以执行：

```sh
./build/kvstore build/demo.wal put name Bob
./build/kvstore build/demo.wal get name
./build/kvstore build/demo.wal delete name
./build/kvstore build/demo.wal get name
```

依次输出 `OK`、`Bob`、`deleted`、`<not found>`。当前 CLI 的 Get 未找到时退出码为 1；Delete 未找到仍正常完成，退出码为 0。非法参数返回 2，存储错误返回 1。这些命令使用的 WAL 会留在 `build/` 下，后续运行会继续使用它。

这些可执行文件路径适用于当前 macOS 环境下的常规单配置构建。CMake 会从 `CMakeLists.txt` 读取头文件目录、C++ 标准和源文件依赖，构建时不需要手写 `-I` 和源文件列表。若要统一验证，可运行 `cmake --build build` 构建全部目标，再运行 `(cd build && ctest --output-on-failure)` 执行三组测试。

### 不完整尾部与损坏处理

普通命令采用严格模式：发现不完整尾部或校验损坏即报错，不改动已有文件。已确认损坏的记录不会被自动跳过。

如果明确接受丢弃不完整尾部，可以显式运行：

```sh
./build/kvstore build/demo.wal recover-tail
```

它只保留完整、合法的记录前缀，报告截去的字节数，然后同步文件。它不是通用修复工具：损坏的长度也可能表现为不完整；选用此模式意味着接受丢弃从该位置开始的剩余字节。已确认的格式或 CRC 损坏在此模式下仍然报错，文件保持原样。

同一个 WAL 同时只能被一个 Store 打开，父目录需已存在；当前使用 macOS/Linux 文件接口，本机验证环境是 macOS。成功写入前同步文件，macOS 还请求 `F_FULLFSYNC` 刷新设备缓存；新文件的父目录也同步。已验证进程强杀后的恢复，没有进行真实断电实验。

### 当前验证状态

使用本机 CMake 4.4.4 和 Apple Clang 21 构建成功，CTest 登记的三组测试均通过：8 个 MemTable、12 个 WAL 记录、12 个 Store 测试函数，共 32 个。Store 测试包括正常重开、连续大记录回放、严格恢复保留文件、显式截尾后追加、损坏拒绝、文件所有权互斥、进程 SIGKILL 恢复和真实部分写入失败后的停止服务。

启用编译警告、AddressSanitizer 和 UndefinedBehaviorSanitizer 的独立构建也通过这三组测试，无编译警告或检查错误报告；CLI 跨进程读写、删除、截尾恢复与退出码已另行验证。

所有编译输出均放在已忽略的 `build/` 目录中。

## 协作节奏

每次只做一个已讨论清楚的小步骤，说明改动、原因、数据流、必要的 C++ 知识，并验证结果。你说 OK、完成讨论后，才进入下一步；提问本身不代表同意继续开发。
