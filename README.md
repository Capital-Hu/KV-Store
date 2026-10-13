# Replicated Key-Value Store

一个逐步实现、逐步讲解的 C++ 学习项目。先完成单机 LSM 存储引擎，再完成三节点 Raft 复制。两部分都是最终验收要求。

当前完成 **第 5 步：Store 刷盘、文件集合和 WAL 回收**。写入先同步当前 WAL，再更新 MemTable；可以手动 flush，也会在待刷盘数据达到默认 4 MiB 阈值时同步刷盘。读取先查 MemTable，再从新到旧查询 SSTable；启动依据 manifest 恢复有效文件集合，安全回收旧 WAL 和未发布文件。当前按单线程使用，尚未实现 compaction、后台线程或查询索引。

## 从哪里看

- [GOAL.md](GOAL.md)：最终目标、完成条件、推进规则。
- [ROADMAP.md](ROADMAP.md)：第一阶段的拆分，以及第二阶段的方向。
- [第 0 步讲解](docs/steps/00-framework.md)：本次每个文件的作用、程序如何运行、C++ 与 Java 的区别。
- [第 1 步讲解](docs/steps/01-memtable.md)：内存表接口、实现、C++ 语法与验证结果。
- [第 2 步讲解](docs/steps/02-wal-record.md)：WAL 字节格式、校验、解码状态与 C++ 写法。
- [第 3 步讲解](docs/steps/03-persistent-store.md)：持久化顺序、启动回放、尾部恢复、文件资源与测试。
- [第 4 步讲解](docs/steps/04-sstable.md)：SSTable 文件格式、删除标记、查询结果、读写流程与测试。
- [第 5a 步讲解](docs/steps/05a-memtable-export.md)：内存删除标记、Get/Lookup 的区别、有序导出和 SSTable 转换测试。
- [第 5 步讲解](docs/steps/05-store-flush.md)：文件清单、同步发布、读写与恢复、WAL 轮换回收和中断测试。
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
- `include/kvstore/sstable.h`：SSTable 条目、写文件和查询接口。
- `src/sstable.cpp`：独立文件读写、格式校验和顺序查询。
- `tests/sstable_test.cpp`：有序文件、删除标记、损坏与截断测试。
- `include/kvstore/manifest.h`、`src/manifest.cpp`：有效文件清单及带校验的格式。
- `src/file_io.h`、`src/file_io.cpp`：共享文件 I/O、同步和锁。
- `src/coding.h`：共享小端整数和 CRC 编码辅助函数，原 WAL/SSTable 格式不变。
- `tests/manifest_test.cpp`、`tests/store_flush_test.cpp`：清单与刷盘恢复测试。
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
All 12 MemTable tests passed.
```

如果测试失败，会打印失败原因并返回非零退出码。编译失败时先处理编译错误，不继续运行旧的可执行文件。

#### 编译并运行项目程序

```sh
mkdir -p build
c++ -std=c++17 -Wall -Wextra -Wpedantic -I include src/memtable.cpp src/wal_record.cpp src/sstable.cpp src/manifest.cpp src/file_io.cpp src/store.cpp src/main.cpp -o build/kvstore
./build/kvstore build/demo.wal put name Alice
./build/kvstore build/demo.wal get name
```

程序入口是 `src/main.cpp` 中的 `main()`。`build/demo.wal` 是 Store 基础路径，初次使用时也作为编号 0 的 WAL 路径；父目录必须已经存在。每条命令启动一个新进程，Get 会依据 manifest 加载 SSTable，并回放当前 WAL。

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
c++ -std=c++17 -Wall -Wextra -Wpedantic -I include src/memtable.cpp src/wal_record.cpp src/sstable.cpp src/manifest.cpp src/file_io.cpp src/store.cpp tests/store_test.cpp -o build/store_test
./build/store_test
```

成功时输出 `All 12 Store tests passed.`。测试在独立临时目录中创建 WAL，结束时清理；强杀和文件大小限制只用于测试子进程。

#### 编译并运行 SSTable 测试

```sh
mkdir -p build
c++ -std=c++17 -Wall -Wextra -Wpedantic -I include src/memtable.cpp src/wal_record.cpp src/sstable.cpp tests/sstable_test.cpp -o build/sstable_test
./build/sstable_test
```

成功时输出 `All 13 SSTable tests passed.`。测试创建并清理自己的临时 `.sst` 文件，不使用手动运行 main 时的 WAL。SSTable 条目复用 WAL 编解码器，所以这里需要编译 `src/wal_record.cpp`；第 5a 步新增了 MemTable 导出转 SSTable 测试，因此也需要 `src/memtable.cpp`。

#### 编译并运行清单和 Store 刷盘测试

```sh
mkdir -p build
c++ -std=c++17 -Wall -Wextra -Wpedantic -I include src/manifest.cpp tests/manifest_test.cpp -o build/manifest_test
./build/manifest_test
c++ -std=c++17 -Wall -Wextra -Wpedantic -I include src/memtable.cpp src/wal_record.cpp src/sstable.cpp src/manifest.cpp src/file_io.cpp src/store.cpp tests/store_flush_test.cpp -o build/store_flush_test
./build/store_flush_test
```

分别输出 `All 5 manifest tests passed.` 和 `All 17 Store flush tests passed.`。刷盘测试只使用独立临时目录，文件大小限制和强杀只作用于测试子进程。

#### 这些编译参数是什么意思

| 参数或文件 | 作用 |
| --- | --- |
| `-std=c++17` | 使用 C++17 标准，支持 `std::optional` |
| `-Wall -Wextra -Wpedantic` | 开启常用编译警告，帮助发现问题 |
| `-I include` | 从项目的 `include/` 目录搜索头文件，因此能找到 `kvstore/memtable.h` |
| `src/*.cpp` 中对应的实现文件 | 提供各模块的定义，按程序依赖选择；当前 Store 依赖 MemTable、WAL、SSTable、manifest 和 file_io，推荐用 CMake 管理 |
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

运行 SSTable 测试：

```sh
cmake --build build --target sstable_test
./build/sstable_test
```

成功输出 `All 13 SSTable tests passed.`。

运行本次新增的清单和刷盘测试：

```sh
cmake --build build --target manifest_test store_flush_test
./build/manifest_test
./build/store_flush_test
```

分别输出 `All 5 manifest tests passed.` 和 `All 17 Store flush tests passed.`。

构建并运行项目程序：

```sh
cmake --build build --target kvstore
./build/kvstore build/demo.wal put name Alice
./build/kvstore build/demo.wal get name
```

Put 输出 `OK`，下一次启动的 Get 输出 `Alice`。手动刷盘后再次读取：

```sh
./build/kvstore build/demo.wal flush
./build/kvstore build/demo.wal get name
```

flush 输出当前 SSTable 数量，Get 仍输出 `Alice`，此时从 SSTable 查询。刷新后的 `build/demo.wal` 文件可能已经被回收，继续传这个基础路径即可打开同一组数据。可以用 `ls build/demo.wal*` 查看 `.lock`、`.manifest`、`.sst.N` 和当前 `.wal.N` 文件。

也可以执行：

```sh
./build/kvstore build/demo.wal put name Bob
./build/kvstore build/demo.wal get name
./build/kvstore build/demo.wal delete name
./build/kvstore build/demo.wal get name
```

依次输出 `OK`、`Bob`、`deleted`、`<not found>`。Get 未找到时退出码为 1；Delete 未找到仍正常完成，退出码为 0。非法参数返回 2，存储错误返回 1。数据文件留在 `build/` 下，后续运行继续使用。删除标记会随刷盘进入 SSTable，避免旧值复活。

这些可执行文件路径适用于当前 macOS 环境下的常规单配置构建。CMake 会从 `CMakeLists.txt` 读取头文件目录、C++ 标准和源文件依赖，构建时不需要手写 `-I` 和源文件列表。若要统一验证，可运行 `cmake --build build` 构建全部目标，再运行 `(cd build && ctest --output-on-failure)` 执行六组测试。

### 不完整尾部与损坏处理

普通命令采用严格模式：当前 WAL 有不完整尾部或校验损坏即报错，不修改这些损坏内容。manifest 或有效 SSTable 缺失/损坏也会拒绝打开，不扫描目录猜测恢复来源。

如果明确接受丢弃不完整尾部，可以显式运行：

```sh
./build/kvstore build/demo.wal recover-tail
```

它只保留完整、合法的记录前缀，报告截去的字节数，然后同步文件。它不是通用修复工具：损坏的长度也可能表现为不完整；选用此模式意味着接受丢弃从该位置开始的剩余字节。已确认的格式或 CRC 损坏在此模式下仍然报错，文件保持原样。

同一个基础路径同时只能被一个 Store 打开，使用稳定 `.lock` 文件保持跨 WAL 轮换的独占锁。父目录需已存在；当前使用 macOS/Linux 文件接口，本机验证环境是 macOS。成功写入前同步 WAL，macOS 还请求 `F_FULLFSYNC` 刷新设备缓存。

刷盘先同步 SSTable、新 WAL 及目录，再同步临时 manifest、rename 发布并同步目录，最后回收旧 WAL。启动只加载清单指定的文件，校验完成后才清理保留命名空间中未被引用的编号文件。不要在这些路径放其他数据或从外部修改它们。已验证进程强杀和部分写入后的恢复，没有进行真实断电实验。

默认自动刷盘阈值为 4 MiB，统计 MemTable 最新条目的编码大小，不是实际堆内存或整个 WAL 历史大小；重复覆盖少量 key 时仍可手动 flush 控制 WAL 增长。退出不自动刷盘，未刷盘状态由 WAL 恢复。

### 当前验证状态

本机 CMake 4.4.4 和 Apple Clang 21 构建成功，六组 CTest 均通过，共 71 个测试函数：12 个 MemTable、12 个 WAL、12 个原 Store、13 个 SSTable、5 个 manifest、17 个 Store 刷盘测试。

新增测试涵盖新旧文件覆盖、删除不复活、阈值触发、旧日志退休、缺失/损坏拒绝、孤立文件清理、轮换后尾部恢复和稳定锁；在 8 个刷盘边界分别强杀首次/后续刷盘进程，注入异常，并制造真实的部分 SSTable/清单写入。启用编译警告、AddressSanitizer 和 UndefinedBehaviorSanitizer 的独立构建也通过六组测试，无编译警告或检查错误报告。另用 CLI 跨进程执行 90 次混合操作、穿插 flush 与查询，对照独立内存模型并验证旧 WAL 回收。

独立的 WriteSSTable 仍只负责格式写出；Store::Flush 负责补齐同步、清单发布与回收协议。当前没有 compaction、后台并发、块索引、Bloom filter、block cache、Raft 或事务，不声称完成真实断电验证。

所有编译输出均放在已忽略的 `build/` 目录中。

## 协作节奏

每次只做一个已讨论清楚的小步骤，说明改动、原因、数据流、必要的 C++ 知识，并验证结果。你说 OK、完成讨论后，才进入下一步；提问本身不代表同意继续开发。
