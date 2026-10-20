# 第 12 步：Raft 持久化日志

> 路径说明：本文记录本步骤完成时的文件位置。后续目录已按模块整理，当前路径见[第 16a 步](16a-project-layout.md)；构建与运行命令以[项目 README](../../README.zh-CN.md)为准。

本轮把第 11 步的 current_term、voted_for、log 保存到独立的 Raft 日志文件，提供追加、逻辑后缀替换与重启恢复，并让 Node 能读取恢复后的状态。没有实现选举、RPC 或 Store 接入。

后续第 13 步已让带 Storage 的 Node 接受时间和消息事件，执行选举与心跳；本页保留第 12 步的存储格式和行为说明，格式未改变。当前协议接口见 [第 13 步](13-raft-election.md)。

## 先看为什么要保存这三项

任期不能因为重启倒退。同一任期投过一票后，重启也不能改投另一个节点。已经向其他节点确认保存的日志，重启后仍须存在。

因此，后续处理选举或复制请求时，不能先回复成功、再慢慢落盘。本轮的存储操作只有在文件同步成功、内存状态更新完成后才返回；下一步协议处理会使用这个边界。协议对持久状态的要求见 [Raft 原始论文 Figure 2](https://raft.github.io/raft.pdf)。

## 文件与对象的职责

| 文件或对象 | 保存什么 |
| --- | --- |
| `node1.raft` | 节点/成员身份头，以及任期、投票和日志后缀变化的追加记录 |
| `node1.raft.lock` | 整个打开期间的独占锁；也是这个路径已经使用过的痕迹 |
| `node1.raft.tmp` | 仅用于首次创建时发布完整文件头，成功 rename 后消失 |
| `Storage::state_` | 重放日志得到的当前内存状态 |
| `Node` | 独占持有可选 Storage，通过它观察恢复状态；仍从 follower 开始 |

这里的 Raft 日志与 Store WAL 是两份独立数据。共享的只是已有低层文件操作，例如可靠写入、同步和独占锁，不共享 KV 状态或日志格式。

本轮在 CMake 中把原有 `src/file_io.cpp` 单独编译为 `kvstore_file_io`，再供 `kvstore_engine` 和 `kvstore_raft` 链接。它的实现没有改动，Raft 仍不链接单机引擎；这样不必复制一份文件同步代码，也避免两个库各自包含同一组外部符号。

## 新建与重开必须明确区分

```cpp
namespace raft = kvstore::raft;
raft::NodeConfig config{1, {2, 3}};

// Only for a new, unused path. The parent directory must already exist.
raft::Storage storage("build/node1.raft", config, raft::StorageOpenMode::CreateNew);
```

后续打开同一份数据时使用 `OpenExisting`。该模式要求日志文件及 lock 文件都已存在，不会在日志丢失时自动生成任期 0 的新状态。

CreateNew 使用独占创建 lock 文件；日志或 lock 已存在时都不能重置。文件头包含本机 ID 及两个 peer ID，重开时校验身份；peer 顺序不同不算身份变化。不能拿节点 1 的数据目录作为节点 2 使用。

首次创建依次执行：创建并锁住 lock → 写 tmp 文件头 → 同步文件 → rename 到正式路径 → 同步目录。如果在完整文件头发布之前失败，残留文件会保留；代码拒绝猜测这是未完成初始化还是数据丢失，也不自动清空重新开始。正常重开仅使用正式文件，不拿 tmp 猜测状态。

基础路径在打开时转为绝对路径，父目录必须已经存在。每个打开的 Storage 只有一个所有者，不支持并发调用或复制。

## 磁盘格式

所有整数都采用小端编码，不直接把 C++ struct 内存写入文件。struct 可能包含填充和指针，不能作为稳定磁盘格式。

文件由固定 32 字节文件头及任意多个记录组成：

```text
KVR1 | node_id:u64 | peer_min:u64 | peer_max:u64 | header_crc32:u32
记录 1
记录 2
……
```

文件头 CRC 覆盖前 28 字节。每条记录的外层格式：

```text
RFR1 | payload_length:u32 | frame_header_crc32:u32 | payload | payload_crc32:u32
└──────── 前 8 字节 ────────┘
```

frame_header_crc32 覆盖 magic 和长度，payload_crc32 覆盖 payload。长度损坏时先由头部校验发现，不会直接把被改大的长度当作“不完整尾部”截掉。CRC 检测损坏，不提供纠错。

payload 只有两种：

```text
类型 1：任期和投票一起更新
1:u8 | term:u64 | voted_for:u64
                     └─ 0 表示没有投票对象，实际节点 ID 禁止为 0

类型 2：替换逻辑后缀
2:u8 | first_index:u64 | entry_count:u32 | entries...

每个 entry：
index:u64 | term:u64 | op:u8 | key_length:u32 | value_length:u32 | key | value
                      └─ 1=Put，2=Delete
```

一条记录包含完整的变化。任期与投票不会拆成两次独立写入；日志后缀删除和新后缀写入也不会拆成两次独立写入。

## Append 与 ReplaceSuffix 怎么工作

假设当前逻辑日志为：

```text
index  term  command
1      1     Put(a, old-a)
2      1     Put(b, old-b)
```

已经把 current_term 更新为 2 后，调用：

```cpp
storage.ReplaceSuffix(2, {
    {2, 2, {raft::CommandType::Delete, "b", ""}},
    {3, 2, {raft::CommandType::Put, "c", "new-c"}},
});
```

意思是保留 `[1, 2)`，从位置 2 开始的整个后缀换成给定内容：

```text
1      1     Put(a, old-a)
2      2     Delete(b)
3      2     Put(c, new-c)
```

**磁盘上不就地修改旧记录，也不先把旧文件截短。** 只向文件末尾追加一条“从 2 开始替换”的记录。重启按记录顺序重放后得到新逻辑日志；记录完整就应用整个变化，尾部不完整则严格拒绝或显式回退到先前完整状态。

Append 等价于从当前最后位置 + 1 开始替换。ReplaceSuffix 的 entries 为空时只删除逻辑后缀；完全相同的更新不额外写记录。这里的“逻辑后缀替换”不等于 Raft 快照或物理日志压缩，磁盘上历史记录仍保留。

Storage 尚不知道 commit_index。第 14 步协议层必须保证不能改掉已提交前缀，也不能把收到的每个 AppendEntries 都无条件当作“替换整个后缀”；应先比较日志，找到真正冲突的位置。

## 每次写入的顺序

```text
校验参数、准备编码记录和所需内存
    ↓
向日志追加完整 frame
    ↓
同步文件（沿用已有的 fsync；macOS 还请求 F_FULLFSYNC）
    ↓
更新内存 current_term/voted_for 或安装新日志后缀
    ↓
返回成功
```

日志追加只处理新增变化，不重写整个已有文件。后缀需要的字符串副本和 vector 空间都在写入前准备；vector 按几何方式扩容，避免每追加一条都搬移全部元数据。同步后的正常内存安装只销毁旧后缀、移动已准备的条目。

参数错误在写入前拒绝，Storage 可以继续使用。一旦开始追加后发生写入、同步或后续发布错误，就把 Storage 标记为不可用；继续读取 state 或写入都会报错，必须关闭后重开。失败调用不保证磁盘没有变化，完整但未返回成功的记录可能在重开后存在。

## 参数约束

- 任期只能不变或增加；任期 0 不能投票，投票对象必须是三个成员之一。
- 同一任期已经投票后，不可改投，也不可清空；进入更高任期可以清空或重新选择。
- 日志从 index 1 开始连续排列，没有假的第 0 条记录；条目任期非零、不倒退且不超过 current_term。
- 相同 index、term 不允许对应不同命令。
- key + value 最多 1 MiB，Delete 的 value 必须为空；空 key、空 Put 值、二进制字符串都合法。
- 单条变化 payload 最多 16 MiB；当前逻辑日志最多 1,000,000 条且编码条目合计最多 64 MiB。日志字节计费不含 vector/string 元数据或分配器开销。

这些是第一版明确的容量上限。历史变化记录的文件总长度没有压缩机制，重开需顺序重放；达到逻辑上限时拒绝继续增长，不能把这版当作无限增长的持久化实现。

## 重启与不完整尾部

```text
取得独占锁
    ↓
校验文件头、节点身份和成员配置
    ↓
逐条读取有大小上限的 frame
    ↓
校验头部 CRC、payload CRC、记录格式与状态变化合法性
    ↓
重放任期/投票更新与后缀替换
    ↓
同步文件和目录，向调用者提供恢复状态
```

默认 Strict 模式遇到不完整尾部就拒绝打开，保留原字节。显式选择 `StorageRecoveryMode::TruncateIncompleteTail` 时，只允许截掉最后那条不完整记录，并报告截去多少字节；完整前缀保留并同步，之后可继续追加。

已确认的 header/CRC/格式/状态损坏，在两种模式下都拒绝打开且不修改文件。尾部恢复不是通用修复；如果文件被人为截短，代码也不能判断其中是否曾经有已确认的记录，因此该模式表示接受丢弃不完整的最后变化。

完整但未被调用方确认的记录可以被重放。重开后再次同步，是为了在后续协议使用这份状态之前建立持久化边界。这里验证了进程崩溃与写入故障，未进行真实断电实验。

## Node 如何接入

```cpp
auto disk = std::make_unique<raft::Storage>(
    "build/node1.raft", config, raft::StorageOpenMode::OpenExisting);
raft::Node node(config, std::move(disk));
const auto state = node.status();
```

Node 独占持有 Storage 并读取其恢复状态，不再额外复制一整份持久日志。节点与 Storage 的身份必须一致。仍可以用不带 Storage 的构造方式运行第 11 步内存模型测试。

重启后恢复任期、投票和日志；role 仍是 follower，leader 未知，逻辑时间为 0，commit_index 和 last_applied 也仍为 0。**日志有两条，不代表两条都已提交或执行。** 当前没有状态机应用，更不会调用 Store；第 15 步才设计与 KV 状态一致的执行进度恢复。

## 文件改动与阅读顺序

| 文件 | 作用 |
| --- | --- |
| `include/kvstore/raft/storage.h` | 对外存储接口、打开/恢复模式与容量限制 |
| `src/raft/storage.cpp` | 独占打开、同步追加、内存发布和启动回放 |
| `src/raft/storage_codec.h/.cpp` | 内部磁盘编解码、frame 校验与状态约束 |
| `include/kvstore/raft/types.h` | NodeConfig 移到共享类型头，供 Node 与 Storage 使用 |
| `include/kvstore/raft/node.h`、`src/raft/node.cpp` | 可选持有 Storage，从它观察恢复状态 |
| `CMakeLists.txt` | 公共低层 I/O 目标和新测试；不让 Raft 链接 Store |
| `tests/raft_storage_test.cpp` | 持久化规则、损坏与截断、进程强杀、真实部分写入及 Node 恢复 |

先读 storage.h 的三个写入方法，再看 storage.cpp 中 SetTermAndVote / ReplaceSuffix 的顺序；编码格式可以对照本页后再读 storage_codec.cpp，不必一开始逐行追字节。

## 新的 C++ 写法

`std::unique_ptr<Storage>` 表示一个所有者负责销毁这个 Storage。`std::move(disk)` 把所有权交给 Node，调用方的 disk 随后为空；Node 销毁时自动关闭日志和释放锁，因此 Node 不能复制。

node.h 只前向声明 Storage，Node 的析构函数放在 node.cpp 中定义，那里已经能看到完整 Storage 类型。这与 Java 普通引用赋值不同，C++ 必须明确谁负责释放资源。

编码器中的 string_view 只在解析当前记录时借用字节；解码后的 command 使用 string 拥有副本。写日志前先复制新条目并 reserve 容量，之后用不会分配的 move 安装数据，缩小同步成功后内存发布失败的空间。

## 编译和验证

在项目根目录执行：

```sh
cmake --build build --target raft_storage_test
./build/raft_storage_test
```

成功输出：

```text
All 15 Raft storage tests passed (8 process crashes and 2 real partial writes).
```

测试只使用自己创建的临时目录，强杀与文件大小限制只作用于测试子进程。十五个测试分别覆盖：

1. 首次创建、重开、身份/成员校验及新建不能覆盖。
2. 任期单调、每任期一票、重复操作无额外写入及重启后不能再投一票。
3. 多条追加、空/二进制数据、Delete 和 1 MiB 命令。
4. 后缀替换、删除后缀、继续追加及重放后旧数据不复活。
5. 非法位置、断号、任期、命令等请求不改变内存和磁盘，且不损坏后续可用性。
6. 批次超过 16 MiB 时在写入前拒绝。
7. 文件头、长度、CRC 和垃圾尾部损坏，两种恢复模式都拒绝修改。
8. 重新计算过 CRC 的非法记录仍被语义校验拒绝，包括非法数量/长度和同任期改票。
9. 两类记录在每一个字节边界被截断，Strict 保留原文件，显式恢复只保留完整前缀；恢复后继续写入。
10. 独占打开、独立节点文件及丢失日志不自动重建。
11. 写完、同步后、内存更新后的异常传播与不可用状态。
12. 两类记录各在三个内部位置及成功返回后强杀，共 8 个场景。
13. RLIMIT_FSIZE 制造两次真实部分写入，验证 EFBIG、停止服务、完整前缀恢复与继续使用。
14. Node 独占接入、重启状态观察及不能把日志长度当成提交/执行位置。
15. 独立逻辑模型与连续 25 轮关闭/重放对照。

本机 Apple Clang 21、CMake 4.4.4 的普通构建及开启编译警告、AddressSanitizer、UndefinedBehaviorSanitizer 的构建，均通过完整 16 组 CTest，共 175 个测试函数；构建无警告，未报告内存或未定义行为错误。低层 I/O 拆分后，原有单机及 Raft 基础模型测试继续通过。当前 Raft 对象按单线程使用，本轮不新增线程竞争检查。

这些测试尚不能证明选举或复制协议正确；它们建立下一步使用的持久化边界。下一步第 13 步才加入选举、心跳和角色变化。
