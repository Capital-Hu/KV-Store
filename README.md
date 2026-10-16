# Replicated Key-Value Store

一个逐步实现、逐步讲解的 C++ 学习项目。先完成单机 LSM 存储引擎，再完成三节点 Raft 复制。两部分都是最终验收要求。

当前完成 **第 14 步：Raft 日志复制、冲突修正与提交**。Leader 先持久化提议，再向其他节点复制；多数确认且满足当前任期规则后推进提交。支持分批追赶、冲突后缀修正、重试及当选 NoOp；提供可运行的三节点进程内演示。下一步将已提交命令接入 Store，之后接 gRPC 和客户端。

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
- [第 6 步讲解](docs/steps/06-compaction.md)：流式有序合并、删除标记规则、SSTable 游标/逐条写入、同步发布与回收。
- [第 7a 步讲解](docs/steps/07a-background-compaction.md)：后台线程、锁的范围、新旧文件交接、等待与退出。
- [第 7b 步讲解](docs/steps/07b-background-flush.md)：冻结表、两次清单发布、双 WAL 恢复、背压与两个后台线程退出。
- [第 8 步讲解](docs/steps/08-bloom-filter.md)：位数组、哈希、假阳性与无漏判、删除标记、重建与扫描量测量。
- [第 9 步讲解](docs/steps/09-block-index-cache.md)：逻辑块、二分索引、共享 LRU、容量计费、失效与游标。
- [第 10 步讲解](docs/steps/10-engine-integration.md)：独立模型、成功通知、四层读取与 38 个进程强杀场景。
- [第 11 步讲解](docs/steps/11-raft-model.md)：独立 Raft 模块、初始状态、消息字段、逻辑时间与 C++ 写法。
- [第 12 步讲解](docs/steps/12-raft-storage.md)：Raft 日志格式、任期/投票同步、后缀替换、恢复模式与故障测试。
- [第 13 步讲解](docs/steps/13-raft-election.md)：消息信箱、随机超时、持久化选举、心跳、重选和演示。
- [第 14 步讲解](docs/steps/14-raft-replication.md)：提议、分批复制、冲突修正、多数提交、NoOp、重启与演示。
- [单机架构说明](docs/architecture.md)：文件职责、完整读写路径、后台工作、锁与启动恢复。
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
- `src/sstable.cpp`：文件校验、块索引、精确查询与缓存接入。
- `tests/sstable_test.cpp`：有序文件、删除标记、损坏与截断测试。
- `include/kvstore/manifest.h`、`src/manifest.cpp`：有效文件清单及带校验的格式。
- `src/file_io.h`、`src/file_io.cpp`：共享文件 I/O、同步和锁。
- `src/coding.h`：共享小端整数和 CRC 编码辅助函数，原 WAL/SSTable 格式不变。
- `tests/manifest_test.cpp`、`tests/store_flush_test.cpp`：清单与刷盘恢复测试。
- `include/kvstore/compaction.h`、`src/compaction.cpp`：按输入新旧顺序逐条合并 SSTable。
- `tests/compaction_test.cpp`：合并、pending 状态保持、故障恢复与模型测试。
- `src/background_compaction.cpp`：后台调度、等待、合并及发布交接。
- `tests/background_test.cpp`：可控并发交错、自动触发、异常与退出测试。
- `src/background_flush.cpp`：冻结与 WAL 切换、后台刷盘及完成通知。
- `tests/background_flush_test.cpp`：冻结期间查询与写入、背压、双任务交接、异常和退出。
- `include/kvstore/bloom_filter.h`、`src/bloom_filter.cpp`：内存 Bloom filter 与容量降级。
- `tests/bloom_filter_test.cpp`、`tests/sstable_bloom_test.cpp`：过滤器及 SSTable/Store 接入验证。
- `include/kvstore/block_cache.h`、`src/block_cache.cpp`：线程安全共享 LRU、字节预算及缓存编号。
- `tests/block_cache_test.cpp`、`tests/sstable_index_test.cpp`：缓存与块索引接入验证。
- `tests/engine_integration_test.cpp`：单机模块集成、混合操作、成功通知与进程强杀恢复。
- `include/kvstore/raft/types.h`：Raft 角色、命令、日志条目、持久状态和消息结构。
- `include/kvstore/raft/node.h`、`src/raft/node.cpp`：独立节点模型、状态观察与逻辑时间。
- `src/raft/replication.cpp`：提议、按 peer 复制和重试、冲突修正及提交。
- `tests/raft_model_test.cpp`：节点配置、初始状态、独立时钟及时间边界测试。
- `include/kvstore/raft/storage.h`、`src/raft/storage.cpp`：独占 Raft 持久化、同步、后缀替换与重启回放。
- `src/raft/storage_codec.h`、`src/raft/storage_codec.cpp`：内部文件格式和状态约束，不是公共网络编码。
- `tests/raft_storage_test.cpp`：Raft 持久化、损坏/截断、进程强杀、真实部分写入及 Node 恢复。
- `src/raft_demo.cpp`：单进程内选主、复制提交、隔离重选、冲突追赶与重开的演示。
- `tests/raft_election_test.cpp`：选举/心跳、乱序/重复/丢消息、重启与协议持久化失败验证。
- `tests/raft_replication_test.cpp`：复制/提交规则、追赶/冲突、消息故障、持久化边界及重开验证。
- `CMakeLists.txt`：构建入口。

Raft 通过独立的 `kvstore_raft` 目标编译，不链接单机引擎；两者共用由原有 `src/file_io.cpp` 编译的低层 `kvstore_file_io`，不共享日志或 KV 状态。gRPC 到通信步骤再引入依赖。后续代码只在这个子文件夹中开发。

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
c++ -std=c++17 -pthread -Wall -Wextra -Wpedantic -I include src/memtable.cpp src/wal_record.cpp src/sstable.cpp src/bloom_filter.cpp src/block_cache.cpp src/manifest.cpp src/file_io.cpp src/compaction.cpp src/store.cpp src/background_compaction.cpp src/background_flush.cpp src/main.cpp -o build/kvstore
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
c++ -std=c++17 -pthread -Wall -Wextra -Wpedantic -I include src/memtable.cpp src/wal_record.cpp src/sstable.cpp src/bloom_filter.cpp src/block_cache.cpp src/manifest.cpp src/file_io.cpp src/compaction.cpp src/store.cpp src/background_compaction.cpp src/background_flush.cpp tests/store_test.cpp -o build/store_test
./build/store_test
```

成功时输出 `All 12 Store tests passed.`。测试在独立临时目录中创建 WAL，结束时清理；强杀和文件大小限制只用于测试子进程。

#### 编译并运行 SSTable 测试

```sh
mkdir -p build
c++ -std=c++17 -pthread -Wall -Wextra -Wpedantic -I include src/memtable.cpp src/wal_record.cpp src/sstable.cpp src/bloom_filter.cpp src/block_cache.cpp src/file_io.cpp tests/sstable_test.cpp -o build/sstable_test
./build/sstable_test
```

成功时输出 `All 17 SSTable tests passed.`。测试创建并清理自己的临时 `.sst` 文件，不使用手动运行 main 时的 WAL。条目复用 WAL 编解码器，导出测试依赖 MemTable，streaming writer 复用 file_io，因此需要上述实现源文件。

#### 编译并运行清单和 Store 刷盘测试

```sh
mkdir -p build
c++ -std=c++17 -Wall -Wextra -Wpedantic -I include src/manifest.cpp tests/manifest_test.cpp -o build/manifest_test
./build/manifest_test
c++ -std=c++17 -pthread -Wall -Wextra -Wpedantic -I include src/memtable.cpp src/wal_record.cpp src/sstable.cpp src/bloom_filter.cpp src/block_cache.cpp src/manifest.cpp src/file_io.cpp src/compaction.cpp src/store.cpp src/background_compaction.cpp src/background_flush.cpp tests/store_flush_test.cpp -o build/store_flush_test
./build/store_flush_test
```

分别输出 `All 6 manifest tests passed.` 和 `All 21 Store flush tests passed.`。刷盘测试只使用独立临时目录，文件大小限制和强杀只作用于测试子进程。

#### 编译并运行 compaction 测试

```sh
mkdir -p build
c++ -std=c++17 -pthread -Wall -Wextra -Wpedantic -I include src/memtable.cpp src/wal_record.cpp src/sstable.cpp src/bloom_filter.cpp src/block_cache.cpp src/manifest.cpp src/file_io.cpp src/compaction.cpp src/store.cpp src/background_compaction.cpp src/background_flush.cpp tests/compaction_test.cpp -o build/compaction_test
./build/compaction_test
```

成功时输出 `All 16 compaction tests passed.`。测试文件和故障子进程都独立于手动使用的 Store。

#### 这些编译参数是什么意思

| 参数或文件 | 作用 |
| --- | --- |
| `-std=c++17` | 使用 C++17 标准，支持 `std::optional` |
| `-Wall -Wextra -Wpedantic` | 开启常用编译警告，帮助发现问题 |
| `-pthread` | 编译和链接使用线程的 Store 程序；CMake 会通过 Threads::Threads 处理平台差异 |
| `-I include` | 从项目的 `include/` 目录搜索头文件，因此能找到 `kvstore/memtable.h` |
| `src/*.cpp` 中对应的实现文件 | 提供各模块的定义，按程序依赖选择；当前 Store 依赖 MemTable、WAL、SSTable、manifest、file_io、compaction 和后台调度，推荐用 CMake 管理 |
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

成功输出 `All 17 SSTable tests passed.`。

运行清单和刷盘测试：

```sh
cmake --build build --target manifest_test store_flush_test
./build/manifest_test
./build/store_flush_test
```

分别输出 `All 6 manifest tests passed.` 和 `All 21 Store flush tests passed.`。

运行 compaction 测试：

```sh
cmake --build build --target compaction_test
./build/compaction_test
```

成功输出 `All 16 compaction tests passed.`。

运行后台 compaction 测试：

```sh
cmake --build build --target background_test
./build/background_test
```

成功输出 `All 9 background tests passed.`。

运行后台刷盘测试：

```sh
cmake --build build --target background_flush_test
./build/background_flush_test
```

成功输出 `All 10 background flush tests passed.`。

运行 Bloom 测试：

```sh
cmake --build build --target bloom_filter_test sstable_bloom_test
./build/bloom_filter_test
./build/sstable_bloom_test
```

分别输出 `All 7 Bloom filter tests passed.` 和 `All 9 SSTable Bloom tests passed.`，同时打印固定样本误判率与扫描记录数对比。

运行块索引和缓存测试：

```sh
cmake --build build --target block_cache_test sstable_index_test
./build/block_cache_test
./build/sstable_index_test
```

分别输出 `All 8 block cache tests passed.` 和 `All 12 SSTable index/cache tests passed.`。

运行单机集成测试：

```sh
cmake --build build --target engine_integration_test
./build/engine_integration_test
```

成功输出 `All 5 engine integration tests passed (38 process-crash scenarios).`。测试使用独立临时目录，包含正常操作和测试子进程的强杀恢复，不会修改手动使用的数据；运行时间比单个模块测试长。`--in-process` 只运行前两个场景，用于线程检查。

运行 Raft 基础模型测试：

```sh
cmake --build build --target raft_model_test
./build/raft_model_test
```

成功输出 `All 4 Raft model tests passed.`。测试使用不带 Storage 的被动模型，只操作内存，不创建数据文件或连接网络。带 Storage 的节点从第 13 步起启用选举；单机 `kvstore` 命令的用法不变。

运行 Raft 持久化测试：

```sh
cmake --build build --target raft_storage_test
./build/raft_storage_test
```

成功输出 `All 15 Raft storage tests passed (8 process crashes and 2 real partial writes).`。只在独立临时目录中创建 Raft 数据，强杀和文件大小限制只作用于测试子进程，不影响手动使用的 Store 文件。

观察 Raft 复制与提交演示（建议先运行）：

```sh
cmake --build build --target raft_demo
./build/raft_demo
```

会显示：选出 Leader 并提交 NoOp → 本地 Put 时日志前进、提交未动 → 多数确认后提交 → 隔离的旧 Leader 无法独自提交 → 新 Leader 提交 Delete → 重连修正旧后缀 → 重开后重新确认提交位置。重点比较 `last_log_index` 和 `commit_index`。演示用逻辑时间和三份临时持久化状态，没有网络连接；结束自动清理数据。

运行 Raft 选举测试：

```sh
cmake --build build --target raft_election_test
./build/raft_election_test
```

成功输出 `All 20 Raft election tests passed (2 persistence-boundary process crashes).`。覆盖可控交付、消息故障、日志新旧限制、任期切换及成功释放消息前的同步边界。

运行本次新增的 Raft 复制测试：

```sh
cmake --build build --target raft_replication_test
./build/raft_replication_test
```

成功输出 `All 17 Raft replication tests passed (2 persistence-boundary process crashes).`。覆盖落后追赶、冲突后缀、当前任期提交限制、少数派隔离、重开、消息故障和持久化失败。旧构建目录若提示找不到这个新目标，先执行一次上面的 CMake 配置命令，使构建规则包含新目标。

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

这些可执行文件路径适用于当前 macOS 环境下的常规单配置构建。CMake 会从 `CMakeLists.txt` 读取头文件目录、C++ 标准和源文件依赖，构建时不需要手写 `-I` 和源文件列表。若要统一验证，可运行 `cmake --build build` 构建全部目标，再运行 `ctest --test-dir build --output-on-failure` 执行十八组测试。

### Bloom filter 如何影响查询

已有 Get 命令不变。每张 SSTable 打开时构建内存位数组，查询一定不存在的 key 时可直接跳过该文件。可能存在继续执行精确查询；本轮默认通过索引定位候选块，因此假阳性只增加工作量，不改变返回值。Value 与 Tombstone 都加入过滤器，避免跳过删除后读到旧值。

过滤器按每 key 10 bit、七次探测配置，最少 8 字节，单 reader 位数组上限 8 MiB，超过则保守关闭过滤并扫描。重启仍完整校验 SSTable 并重建过滤器；CLI 每条命令启动新进程，因此不能用它直接证明重复查询的总体延迟改善。长时间打开的 Store 才能持续使用已建好的过滤器。

固定样本：加入 10,000 个 key，检查无漏判；20,000 个缺失 key 的误判率为 158/20,000，即 0.79%。另一张含 128 条记录的表，200 次缺失查询跳过 199 次扫描，开启/关闭时分别解析 128/25,600 条记录。这组 Bloom 对照显式关闭块索引，继续隔离过滤器的作用；这是打开之后的逻辑扫描量，不是物理磁盘读取数或延迟 benchmark。详情见第 8 步记录。

### 块索引和共享缓存

一张 SSTable 的完整记录按约 4 KiB 分组，单条大记录不拆开。打开时记录每块末尾 key、偏移、长度和记录序号；Get 二分选一个候选块。缓存命中后仍解析块内记录确认结果，未命中先读并校验整块，再尝试接纳。Next 全扫描及 compaction 输入绕过缓存。

每个 Store 一个共享缓存，默认按 8 MiB 原始块字节计费；第五个构造参数设为 0 即禁用接纳。预算不包含容器元数据、索引、Bloom 或调用方暂时持有的已淘汰块。单块超出容量会绕过缓存，索引计费超过默认 16 MiB 则退回顺序查询，结果不变。

缓存键为独立 reader 编号与块偏移，关闭旧 reader 会清理它的缓存项；compaction 发布新 reader 后，旧缓存不会被新表误用。构造时仍完整校验 KVS1，索引/缓存重开重建；独立 SSTableReader 默认建索引但不创建私有缓存，Store 的查询 reader 才接入共享对象。

固定样本中，查询 256 条记录里的最后一条，索引比较 21 条并读取一个块，顺序基准比较 256 条；冷读还额外校验该块 21 条。缓存命中不再加载文件块，但仍做精确查找。这不是物理 I/O 或延迟 benchmark，详情见第 9 步记录。

### 合并 SSTable

先用一个新的示例基础路径制造两个文件，再执行 compact：

```sh
./build/kvstore build/step6-demo.wal put name Alice
./build/kvstore build/step6-demo.wal flush
./build/kvstore build/step6-demo.wal put name Bob
./build/kvstore build/step6-demo.wal flush
./build/kvstore build/step6-demo.wal compact
./build/kvstore build/step6-demo.wal get name
```

compact 输出 `OK (1 SSTables)`，Get 输出 `Bob`；旧 SSTable 在新结果及清单同步后回收。Compact 本身不请求刷盘；已排队的后台刷盘可以与它并行完成。可写表和冻结表都优先于 SSTable。仅合并一个文件时也会重写以清理删除标记；全删除结果保存为合法空表。

独立 MergeSSTables 默认保留删除标记；Store 后台任务选取开始时全部 SSTable，覆盖全部更旧的磁盘状态，且没有历史快照，允许清理这些输入中的删除标记。合并期间新增的 SSTable 按原顺序保留。后续若改为任意部分合并，必须继续遮住输入外的旧值。

默认每次后台刷盘发布后，SSTable 数达到 4 就请求后台合并；构造函数第四个参数设为 0 可关闭自动合并。CLI 每条命令都是独立进程，退出时会等后台收尾；要观察合并期间继续读写，请看本次后台测试。`flush` 输出的是查询时的文件数，随后可能被后台合并减少。

### 不完整尾部与损坏处理

普通命令采用严格模式：当前 WAL 有不完整尾部或校验损坏即报错，不修改这些损坏内容。清单若指定冻结 WAL，该文件也必须存在、非空且完整合法，即使 recover-tail 模式也不截断它。manifest 或有效 SSTable 缺失/损坏会拒绝打开，不扫描目录猜测恢复来源。

如果明确接受丢弃不完整尾部，可以显式运行：

```sh
./build/kvstore build/demo.wal recover-tail
```

它只保留完整、合法的记录前缀，报告截去的字节数，然后同步文件。它不是通用修复工具：损坏的长度也可能表现为不完整；选用此模式意味着接受丢弃从该位置开始的剩余字节。已确认的格式或 CRC 损坏在此模式下仍然报错，文件保持原样。

同一个基础路径同时只能被一个 Store 打开，使用稳定 `.lock` 文件保持跨 WAL 轮换的独占锁。父目录需已存在；当前使用 macOS/Linux 文件接口，本机验证环境是 macOS。成功写入前同步 WAL，macOS 还请求 `F_FULLFSYNC` 刷新设备缓存。

冻结时先同步新 WAL 及目录，再通过临时 manifest、rename 和目录同步发布新旧两个 WAL，随后切换可写表。后台生成并同步 SSTable 后，第二次发布清单，接入 reader、移除冻结表，最后回收旧 WAL。启动校验清单指定的全部 SSTable 和有效 WAL 后，才清理未被引用的编号文件，并自动继续刷出恢复的冻结表。无冻结 WAL 时仍使用 KVM1 清单；冻结期间使用增加旧 WAL 编号的 KVM2，已有 KVM1 可以读取。不要在这些路径放其他数据或从外部修改它们。已验证进程强杀和部分写入后的恢复，没有进行真实断电实验。

默认自动刷盘阈值为 4 MiB，统计可写 MemTable 最新条目的编码大小，不是实际堆内存或整个 WAL 历史大小。达到阈值会冻结并排队；已有冻结表时释放锁等待，满表后的其他写入也会等待。关闭自动阈值后由调用方控制可写表大小；重复覆盖少量 key 时仍可手动 flush 控制 WAL 增长。退出会完成已冻结的表及其触发的合并，不额外冻结尚未请求刷盘的可写表，剩余状态由当前 WAL 恢复。

### 当前验证状态

本机 CMake 4.4.4 和 Apple Clang 21 构建成功，十八组 CTest 均通过，共 212 个测试函数：单机引擎 156 个、Raft 基础模型 4 个、持久化 15 个、选举 20 个、复制 17 个。新增验证覆盖多数派提交、冲突修正、分批追赶、消息故障及重开，并覆盖 2 个日志同步后、输出前的进程强杀场景。原有故障恢复测试继续通过。

普通构建与开启编译警告、AddressSanitizer、UndefinedBehaviorSanitizer 的构建均通过完整十八组测试，演示在两种构建中运行成功，最终构建无警告。第 9 步已有的五组 ThreadSanitizer 检查结果和第 10 步集成测试两个进程内场景的检查结果保留；Raft 当前仍是单线程事件模型，本轮不新增线程检查。详细场景与验证范围见第 10～14 步记录。

当前前台操作仍共用 Store 状态锁，共享缓存内部另有互斥锁。Bloom 和索引均打开时重建，缓存存原始编码字节。尚无 Bloom/块索引持久化、块压缩或事务；Raft 已能进程内选举和复制提交日志，尚未执行到 Store 或接入客户端服务，未进行真实断电或吞吐量/延迟实验。

### Raft 持久化的当前边界

每个节点独立使用 `.raft`、`.raft.lock`，`.tmp` 只用于首次发布文件头。CreateNew 仅用于未使用的新路径，OpenExisting 要求数据和锁文件存在；不因文件丢失自动重置任期。每次变化以带头部和 payload 校验的完整记录追加，先同步后更新内存，开始写入后的错误会使该 Storage 停止服务。

默认 Strict 模式拒绝不完整尾部。显式 TruncateIncompleteTail 只丢弃最后不完整变化，已确认的 CRC/格式损坏仍拒绝打开。当前逻辑日志最多 64 MiB 编码条目、1,000,000 条，单个 key + value 最多 1 MiB，单次变化 payload 最多 16 MiB；历史追加文件暂不压缩。Node 恢复后仍从 follower 和未提交/未执行位置开始，日志长度不等于已提交进度。完整接口、格式与限制见第 12 步。

带 Storage 的 Node 接受 AdvanceTime / Step 事件，用 DrainMessages 输出待发消息。超时前持久化新任期与自投票，合法赞成票达到多数后当选；收到有效高任期 RPC 会持久化并退位。第 14 步增加 Propose 和 CommittedEntries：Propose 返回本地日志位置与任期，不等于提交成功；只有多数确认且满足当前任期规则才推进 commit_index。隔离旧 Leader 的未提交提议可能被覆盖。每个 peer 同时等待一份请求，按心跳重试，最多 64 条、2 MiB 编码条目的批次追赶；新 Leader 追加 NoOp 确认旧前缀。NoOp 磁盘标签为 3，新程序可读原 Put/Delete 格式，旧程序不支持新标签。CommittedEntries 返回副本，不执行命令，last_applied 保持 0。默认不带 Storage 的构造保留被动模型，消息处理要求持久化状态。

所有编译输出均放在已忽略的 `build/` 目录中。

## 协作节奏

每次只做一个已讨论清楚的小步骤，说明改动、原因、数据流、必要的 C++ 知识，并验证结果。你说 OK、完成讨论后，才进入下一步；提问本身不代表同意继续开发。
