# 第 16a 步：按模块整理项目目录

这一步发生在 gRPC 三进程完成之后、客户端语义开发之前。目标是让源代码、程序入口、演示和测试的职责更容易查找；业务逻辑、公开接口和磁盘格式保持原样。

## 整理后的目录

```text
include/kvstore/     公开接口，保持原有头文件引用方式
src/
  engine/           Store、MemTable、WAL、SSTable、缓存、刷盘与合并
  raft/             共识协议、复制及 Raft 持久化
  replica/          ReplicatedStore、COMMIT 记录
  rpc/              gRPC 服务、消息转换和配置
  common/           内部文件操作与字节编码工具
apps/               kvstore、kvnode、kvadmin 的 main()
examples/           raft_demo、replica_demo
tests/
  engine/           存储单元、并发、恢复及集成测试
  raft/             协议与持久化测试
  replica/          Raft 接入 Store 的测试
  rpc/              编码与真实三进程测试
proto/              网络协议定义
scripts/            固定版本依赖构建
docs/              总体架构、单机架构和历史步骤讲解
```

## 为什么这样拆

- `src/engine` 集中存储代码，读写、刷盘和合并属于同一个模块。
- `src/raft` 保持独立；协议层不依赖 Store 或 gRPC。
- `src/replica` 负责连接 Raft 与 Store，和共识协议本身区分开。
- `src/rpc` 负责传输及运行调度；可执行入口搬到 `apps`。
- `src/common` 是多个模块共用的内部工具。私有头文件和对应实现放在一起，不加入公开 API。
- `examples` 放可运行的演示，`tests` 按四个业务模块分组。

`include/kvstore` 保留，因此已有的 `#include "kvstore/store.h"` 等引用不需要修改。内部引用使用 `common/file_io.h`、`replica/replica_checkpoint.h` 等路径，由 CMake 为确实需要它们的目标提供私有 `src` 搜索目录，避免使用容易随层级变化的 `../src/...`。

## 构建规则改了什么

根目录 `CMakeLists.txt` 继续定义核心库、应用和演示；可选的 `src/rpc/CMakeLists.txt` 负责网络库和 Protobuf 生成。原先分散在这两个文件中的测试规则集中到新的 `tests/CMakeLists.txt`。

这是同一个 CMake 构建过程。`add_subdirectory(tests)` 只是继续读取测试规则，不需要手工进入子目录分别构建。`PRIVATE` 头文件目录只用于当前目标的编译，不会作为该库的公开使用要求继续传播。

所有可执行程序仍输出到所选构建目录的根部。原来的 `./build/kvstore`、`./build/memtable_test`、`./build/network/kvnode` 等运行路径保持不变；CTest 的名称、参数、工作目录与超时配置也保留。

CTest 在根目录进入各子目录前初始化，保证重新配置旧构建目录时，原 RPC 子目录中的测试登记也能被正确更新，避免重复发现测试。

## 旧路径到新路径

下面列出本步的源文件与测试文件移动。旧步骤讲解保留当时的路径和设计上下文；当前编译和运行方法以双语 README 为准。

| 原路径 | 当前路径 |
| --- | --- |
| `src/background_compaction.cpp` | `src/engine/background_compaction.cpp` |
| `src/background_flush.cpp` | `src/engine/background_flush.cpp` |
| `src/block_cache.cpp` | `src/engine/block_cache.cpp` |
| `src/bloom_filter.cpp` | `src/engine/bloom_filter.cpp` |
| `src/coding.h` | `src/common/coding.h` |
| `src/compaction.cpp` | `src/engine/compaction.cpp` |
| `src/file_io.cpp` | `src/common/file_io.cpp` |
| `src/file_io.h` | `src/common/file_io.h` |
| `src/main.cpp` | `apps/kvstore_main.cpp` |
| `src/manifest.cpp` | `src/engine/manifest.cpp` |
| `src/memtable.cpp` | `src/engine/memtable.cpp` |
| `src/raft_demo.cpp` | `examples/raft_demo.cpp` |
| `src/replica_checkpoint.cpp` | `src/replica/replica_checkpoint.cpp` |
| `src/replica_checkpoint.h` | `src/replica/replica_checkpoint.h` |
| `src/replica_demo.cpp` | `examples/replica_demo.cpp` |
| `src/replicated_store.cpp` | `src/replica/replicated_store.cpp` |
| `src/rpc/admin_main.cpp` | `apps/kvadmin_main.cpp` |
| `src/rpc/node_main.cpp` | `apps/kvnode_main.cpp` |
| `src/sstable.cpp` | `src/engine/sstable.cpp` |
| `src/store.cpp` | `src/engine/store.cpp` |
| `src/wal_record.cpp` | `src/engine/wal_record.cpp` |
| `tests/background_flush_test.cpp` | `tests/engine/background_flush_test.cpp` |
| `tests/background_test.cpp` | `tests/engine/background_test.cpp` |
| `tests/block_cache_test.cpp` | `tests/engine/block_cache_test.cpp` |
| `tests/bloom_filter_test.cpp` | `tests/engine/bloom_filter_test.cpp` |
| `tests/compaction_test.cpp` | `tests/engine/compaction_test.cpp` |
| `tests/engine_integration_test.cpp` | `tests/engine/engine_integration_test.cpp` |
| `tests/grpc_codec_test.cpp` | `tests/rpc/grpc_codec_test.cpp` |
| `tests/grpc_process_test.cpp` | `tests/rpc/grpc_process_test.cpp` |
| `tests/manifest_test.cpp` | `tests/engine/manifest_test.cpp` |
| `tests/memtable_test.cpp` | `tests/engine/memtable_test.cpp` |
| `tests/raft_election_test.cpp` | `tests/raft/raft_election_test.cpp` |
| `tests/raft_model_test.cpp` | `tests/raft/raft_model_test.cpp` |
| `tests/raft_replication_test.cpp` | `tests/raft/raft_replication_test.cpp` |
| `tests/raft_storage_test.cpp` | `tests/raft/raft_storage_test.cpp` |
| `tests/replicated_store_test.cpp` | `tests/replica/replicated_store_test.cpp` |
| `tests/sstable_bloom_test.cpp` | `tests/engine/sstable_bloom_test.cpp` |
| `tests/sstable_index_test.cpp` | `tests/engine/sstable_index_test.cpp` |
| `tests/sstable_test.cpp` | `tests/engine/sstable_test.cpp` |
| `tests/store_flush_test.cpp` | `tests/engine/store_flush_test.cpp` |
| `tests/store_test.cpp` | `tests/engine/store_test.cpp` |
| `tests/wal_record_test.cpp` | `tests/engine/wal_record_test.cpp` |

原有单机架构说明移到 `docs/storage-engine.md`，`docs/architecture.md` 现在说明完整的三节点系统。旧链接会相应更新。

## 如何重新构建

下面命令在项目根目录执行。已有构建目录会保留此前的配置，包括网络依赖前缀和 sanitizer 参数。

```sh
cmake -S . -B build
cmake --build build
ctest --test-dir build --output-on-failure
```

网络构建：

```sh
cmake -S . -B build/network
cmake --build build/network
ctest --test-dir build/network --output-on-failure
```

初次配置项目时，使用 [英文 README](../../README.md) 或[中文 README](../../README.zh-CN.md) 中的完整命令。

## 验证范围与结果

已逐个核对整理前后的源文件与测试文件：42 个文件的移动都有对应关系；15 个 C++ 文件仅调整了头文件引用，其余 C++ 内容保持一致。公开头文件、proto、依赖构建脚本和编辑器配置与整理前逐字节一致。没有删改测试断言或测试函数。

| 检查 | 本轮结果 |
| --- | --- |
| 默认构建完整 CTest | 19/19 组通过，67.96 秒 |
| 网络构建完整 CTest | 21/21 组通过，90.86 秒 |
| ASan/UBSan 非网络构建完整 CTest | 19/19 组通过，88.24 秒 |
| ASan/UBSan 网络构建完整 CTest | 21/21 组通过，68.64 秒 |
| ThreadSanitizer | 后台合并、后台刷盘、SSTable Bloom、块缓存、块索引五组通过；另有两个进程内集成场景通过 |
| 全新临时目录构建 | 网络库、所有应用、演示与测试构建成功，Protobuf 从头生成；临时产物已清理 |
| 原命令路径检查 | 单机 Put/Get/Flush/Compact/Delete 及重开、两个演示、网络工具帮助均通过 |
| 文档检查 | 本地链接、锚点、代码块和双语命令一致性通过 |

原有测试名称、参数、超时和工作目录已核对；默认仍为 19 组，网络仍为 21 组。修正 CTest 初始化顺序后，旧构建目录也不会重复登记 RPC 测试。恢复 RPC 测试原工作目录后，另行复验这两组测试。

上述耗时仅是本轮回归运行时间，不是性能 benchmark。ASan/UBSan 检查项目代码，第三方 gRPC/Protobuf 仍复用原来的 Release 依赖；TSan 沿用线程场景，不把带 fork/强杀的测试纳入线程检查。

完成这一步后仍停在第 16 阶段，不提前实现第 17 步客户端语义。
