# Replicated Key-Value Store

一个逐步实现、逐步讲解的 C++ 学习项目。先完成单机 LSM 存储引擎，再完成三节点 Raft 复制。两部分都是最终验收要求。

当前完成 **第 8 步：SSTable Bloom filter**。查询先查可写表、冻结表，再从新到旧访问 SSTable；每张表先用内存 Bloom filter 判断，确定没有就跳过，可能存在才精确扫描。删除标记的 key 同样加入过滤器。过滤器在打开文件的完整校验期间重建，暂不持久化，SSTable/WAL/manifest 的文件格式不变。第 7 步的后台刷盘、合并、双 WAL 恢复与背压继续使用。

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
- `include/kvstore/compaction.h`、`src/compaction.cpp`：按输入新旧顺序逐条合并 SSTable。
- `tests/compaction_test.cpp`：合并、pending 状态保持、故障恢复与模型测试。
- `src/background_compaction.cpp`：后台调度、等待、合并及发布交接。
- `tests/background_test.cpp`：可控并发交错、自动触发、异常与退出测试。
- `src/background_flush.cpp`：冻结与 WAL 切换、后台刷盘及完成通知。
- `tests/background_flush_test.cpp`：冻结期间查询与写入、背压、双任务交接、异常和退出。
- `include/kvstore/bloom_filter.h`、`src/bloom_filter.cpp`：内存 Bloom filter 与容量降级。
- `tests/bloom_filter_test.cpp`、`tests/sstable_bloom_test.cpp`：过滤器及 SSTable/Store 接入验证。
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
c++ -std=c++17 -pthread -Wall -Wextra -Wpedantic -I include src/memtable.cpp src/wal_record.cpp src/sstable.cpp src/bloom_filter.cpp src/manifest.cpp src/file_io.cpp src/compaction.cpp src/store.cpp src/background_compaction.cpp src/background_flush.cpp src/main.cpp -o build/kvstore
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
c++ -std=c++17 -pthread -Wall -Wextra -Wpedantic -I include src/memtable.cpp src/wal_record.cpp src/sstable.cpp src/bloom_filter.cpp src/manifest.cpp src/file_io.cpp src/compaction.cpp src/store.cpp src/background_compaction.cpp src/background_flush.cpp tests/store_test.cpp -o build/store_test
./build/store_test
```

成功时输出 `All 12 Store tests passed.`。测试在独立临时目录中创建 WAL，结束时清理；强杀和文件大小限制只用于测试子进程。

#### 编译并运行 SSTable 测试

```sh
mkdir -p build
c++ -std=c++17 -Wall -Wextra -Wpedantic -I include src/memtable.cpp src/wal_record.cpp src/sstable.cpp src/bloom_filter.cpp src/file_io.cpp tests/sstable_test.cpp -o build/sstable_test
./build/sstable_test
```

成功时输出 `All 17 SSTable tests passed.`。测试创建并清理自己的临时 `.sst` 文件，不使用手动运行 main 时的 WAL。条目复用 WAL 编解码器，导出测试依赖 MemTable，streaming writer 复用 file_io，因此需要上述实现源文件。

#### 编译并运行清单和 Store 刷盘测试

```sh
mkdir -p build
c++ -std=c++17 -Wall -Wextra -Wpedantic -I include src/manifest.cpp tests/manifest_test.cpp -o build/manifest_test
./build/manifest_test
c++ -std=c++17 -pthread -Wall -Wextra -Wpedantic -I include src/memtable.cpp src/wal_record.cpp src/sstable.cpp src/bloom_filter.cpp src/manifest.cpp src/file_io.cpp src/compaction.cpp src/store.cpp src/background_compaction.cpp src/background_flush.cpp tests/store_flush_test.cpp -o build/store_flush_test
./build/store_flush_test
```

分别输出 `All 6 manifest tests passed.` 和 `All 21 Store flush tests passed.`。刷盘测试只使用独立临时目录，文件大小限制和强杀只作用于测试子进程。

#### 编译并运行 compaction 测试

```sh
mkdir -p build
c++ -std=c++17 -pthread -Wall -Wextra -Wpedantic -I include src/memtable.cpp src/wal_record.cpp src/sstable.cpp src/bloom_filter.cpp src/manifest.cpp src/file_io.cpp src/compaction.cpp src/store.cpp src/background_compaction.cpp src/background_flush.cpp tests/compaction_test.cpp -o build/compaction_test
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

运行本次新增的 Bloom 测试：

```sh
cmake --build build --target bloom_filter_test sstable_bloom_test
./build/bloom_filter_test
./build/sstable_bloom_test
```

分别输出 `All 7 Bloom filter tests passed.` 和 `All 9 SSTable Bloom tests passed.`，同时打印固定样本误判率与扫描记录数对比。

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

这些可执行文件路径适用于当前 macOS 环境下的常规单配置构建。CMake 会从 `CMakeLists.txt` 读取头文件目录、C++ 标准和源文件依赖，构建时不需要手写 `-I` 和源文件列表。若要统一验证，可运行 `cmake --build build` 构建全部目标，再运行 `(cd build && ctest --output-on-failure)` 执行十一组测试。

### Bloom filter 如何影响查询

已有 Get 命令不变。每张 SSTable 打开时构建内存位数组，查询一定不存在的 key 时可直接跳过该文件。可能存在仍执行精确扫描，因此假阳性只增加工作量，不改变返回值。Value 与 Tombstone 都加入过滤器，避免跳过删除后读到旧值。

过滤器按每 key 10 bit、七次探测配置，最少 8 字节，单 reader 位数组上限 8 MiB，超过则保守关闭过滤并扫描。重启仍完整校验 SSTable 并重建过滤器；CLI 每条命令启动新进程，因此不能用它直接证明重复查询的总体延迟改善。长时间打开的 Store 才能持续使用已建好的过滤器。

固定样本：加入 10,000 个 key，检查无漏判；20,000 个缺失 key 的误判率为 158/20,000，即 0.79%。另一张含 128 条记录的表，200 次缺失查询跳过 199 次扫描，开启/关闭时分别解析 128/25,600 条记录。这是打开之后的逻辑扫描量，不是物理磁盘读取数或延迟 benchmark。详情见第 8 步记录。

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

本机 CMake 4.4.4 和 Apple Clang 21 构建成功，十一组 CTest 均通过，共 131 个测试函数：12 个 MemTable、12 个 WAL、12 个原 Store、17 个 SSTable、6 个 manifest、21 个 Store 刷盘、16 个 compaction、9 个后台合并、10 个后台刷盘、7 个 Bloom filter、9 个 SSTable Bloom 接入测试。

普通构建与开启编译警告、AddressSanitizer、UndefinedBehaviorSanitizer 的构建均通过完整测试。ThreadSanitizer 单独运行后台合并、后台刷盘、SSTable Bloom 接入三组，共 28 个函数，通过且无数据竞争报告。原有强杀、部分写入、双 WAL 恢复与模型测试保留；新增无漏判、假阳性精确回退、删除标记、空/二进制 key、重开重建、逻辑扫描量对照及并发查询验证。

独立 SSTable writer 和 MergeSSTables 负责文件构建，Store 负责同步、清单发布与回收。过滤器在 reader 发布前构建完成，Get 仍由原状态锁保护；compaction 输入全扫描关闭无用过滤器。尚无过滤器持久化、块索引、block cache、Raft 或事务，未进行真实断电实验或延迟性能测量。

所有编译输出均放在已忽略的 `build/` 目录中。

## 协作节奏

每次只做一个已讨论清楚的小步骤，说明改动、原因、数据流、必要的 C++ 知识，并验证结果。你说 OK、完成讨论后，才进入下一步；提问本身不代表同意继续开发。
