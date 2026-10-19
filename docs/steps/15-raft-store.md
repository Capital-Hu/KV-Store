# 第 15 步：把已提交日志执行到 Store

这一步接通之前独立开发的两部分：Raft 确认命令顺序和提交位置，Store 执行命令并提供实际 KV 数据。新增的 ReplicatedStore 负责协调两者；Raft 库本身仍不依赖 Store。

本文件保留第 15 步交付范围。第 16 步已用网络运行层串行调用 ReplicatedStore，支持本机三个独立进程，见 [第 16 步说明](16-grpc-processes.md)；replica_demo 继续保留进程内演示。

## 先运行演示

在项目根目录执行：

```sh
cmake -S . -B build -DBUILD_TESTING=ON
cmake --build build --target replica_demo
./build/replica_demo
```

第一行让旧构建目录认识新增目标，这次执行一次即可。后续普通代码修改继续用后两行。

原来的 raft_demo 保留第 14 步的纯协议演示；新的 replica_demo 才真正读写 Store。演示使用三份独立临时目录，结束自动清理，没有联网。

| 阶段 | 重点看什么 |
| --- | --- |
| 1 | 三份独立 Raft 与 Store，数据为空 |
| 2 | NoOp 提交并执行，applied=1，但没有新增 key |
| 3 | Put name=Alice 仅进入 Leader 日志，Store 查询仍未找到 |
| 4 | 多数确认后，三个 Store 都返回 Alice |
| 5 | 每个 Store 单独刷盘，出现 SSTable |
| 6 | 隔离节点 1，新 Leader 提交 Bob；节点 1 本地仍返回 Alice |
| 7 | 恢复通信，节点 1 修正未提交后缀，执行到 Bob |
| 8 | 三节点全部关闭重开，在交付任何消息之前已经恢复 Bob |
| 9 | 重新选主，提交 Delete，三个 Store 均返回未找到 |

运行本次测试：

```sh
cmake --build build --target replicated_store_test
./build/replicated_store_test
```

成功输出 `All 15 replicated Store tests passed (12 process crashes and 1 real partial write).`。

## 增加了哪一层

```text
调用方：依次交付时间、消息、写入提议
                   ↓
            ReplicatedStore
             /           \
     raft::Node          Store
     选举、复制、提交      WAL → MemTable → SSTable
             \           /
        已提交命令按 index 依次执行
```

ReplicatedStore 独占一个 Node 和一个 Store，不向调用方暴露可写 Store；用户命令必须先经过 Raft。只在集成库 `kvstore_replica` 链接两者，原 `kvstore_raft`、`kvstore_engine` 和单机 CLI 保留独立使用方式。

| 文件 | 职责及改动 |
| --- | --- |
| `include/kvstore/replicated_store.h`，新增 | 连接层的事件、提议、本地查询和本地刷盘接口 |
| `src/replicated_store.cpp`，新增 | 独占目录、恢复、保存提交位置、顺序执行和错误停止 |
| `src/replica_checkpoint.h/.cpp`，新增 | 内部 COMMIT 文件编解码及日志前缀指纹 |
| `include/kvstore/raft/node.h`、`src/raft/node.cpp` | 允许从已验证的持久提交位置恢复，提供逐条执行确认 MarkApplied |
| `src/replica_demo.cpp`，新增 | 观察真实 Store 查询、隔离、追赶与重开 |
| `tests/replicated_store_test.cpp`，新增 | 协议与引擎接入、恢复及故障测试 |
| `CMakeLists.txt` | 增加集成库、演示和测试目标 |

README、GOAL、ROADMAP 同步更新本轮状态。Store、WAL、SSTable、manifest 的实现和格式都没有修改。

## 一次写入现在怎样执行

```text
Propose(Put name=Alice)
    ↓
Node 追加并同步 Raft 日志，发出复制请求
    ↓ 等待后续消息事件，不在 Propose 里等待网络
Node 收到确认，commit_index 从 1 前进到 2
    ↓
ReplicatedStore.SaveCommit(2)
    写 COMMIT.tmp → 同步文件 → rename 成 COMMIT → 同步目录
    ↓
ReplicatedStore.ApplyCommitted()
    Store.Put(name, Alice)
        ↓ 原有 Store 内部流程
        Store WAL 写入并同步 → 更新 MemTable → 必要时请求后台刷盘
    ↓
Node.MarkApplied(2)
    last_applied 从 1 前进到 2
    ↓
当前事件返回，调用方可以取走待发消息或查询本地数据
```

Follower 也执行同样的连接层流程。收到日志并落盘，不代表立即执行；它需要从合法 AppendEntries 的 leader_commit 得知提交位置。Leader 和 Follower 都只执行自己已经确认提交的前缀。

每次 Step / AdvanceTime / Propose 返回前，连接层处理当前所有新提交条目。没有新提交时不重写 COMMIT，也不重复执行已有条目。执行异常会停止连接层，后续读、写和 DrainMessages 都报错，避免通过正常接口暴露部分执行状态或释放本轮待发消息。

## 三个位置分别是什么

| 字段 | 含义 | 是否等于客户端成功 |
| --- | --- | --- |
| `last_log_index` | 本机 Raft 日志最后一条，可能尚未提交 | 否 |
| `commit_index` | 本机已确认安全提交到哪里 | 还需要执行 |
| `last_applied` | 本机已按顺序执行到 Store 的位置，包括跳过执行的 NoOp | 可用于观察本地执行，但还没有客户端响应协议 |

正常接口返回后满足 `last_applied == commit_index <= last_log_index`。执行过程中允许 last_applied 落后于 commit_index；这段过程在同一调用内完成，调用方不能并发查询。连接层仍是单线程对象，事件和 LocalGet 都要串行调用；Store 内部已有的两个后台线程与互斥锁继续负责刷盘、合并。

MarkApplied 只允许确认下一条已提交日志，不能跳号、重复确认或越过 commit_index。NoOp 不调用 Store.Put/Delete，但也会前进一个执行位置。规则与 [Raft 论文 Figure 2 的提交后顺序应用](https://raft.github.io/raft.pdf) 对应。

Propose 仍返回 `(index, term)`，表示本地日志接受提议，没有等待多数或执行。`IsApplied(proposal)` 同时检查位置已经执行且该位置任期仍与提议相同，避免把后来覆盖到相同 index 的新命令误认为旧提议成功。它不是网络客户端的超时、重试或去重实现。

## 为什么先保存提交位置

Store 数据与 Raft 是两套持久化文件，不能靠两次普通写入就声称原子完成。这里选择保存可恢复的执行依据，再允许执行数据变化。

假设已提交到 4，这次又确认提交到 7：先持久化 COMMIT=7，然后按顺序执行第 5、6、7 条。COMMIT 表示“这些日志已经确认提交，允许恢复执行”，不表示“Store 已经全部写完”。

| 崩溃位置 | 重启使用什么 | 结果 |
| --- | --- | --- |
| 新 COMMIT 只写到临时文件，尚未 rename | 旧 COMMIT=4 | 不执行第 5～7 条，等待之后重新确认 |
| 新 COMMIT 已发布，但还没写 Store | 新 COMMIT=7 | 启动重放补齐第 5～7 条 |
| Store 已写完第 5 条，还没确认 last_applied | 新 COMMIT=7 | 启动重放再次执行第 5 条，并完成后续条目 |
| 一批执行到中间，或刚更新 last_applied 就被杀 | 新 COMMIT=7 | 启动重放整个已提交前缀，最后得到执行到 7 的数据 |

实际断电时，rename 后、目录同步前可能恢复为旧文件或新文件；程序只接受最后可见且校验合法的正式 COMMIT。临时文件不会被当成新的提交依据。测试覆盖进程强杀，没有实测断电。

## 重启为什么重放整个前缀

本版不单独持久化 last_applied，也不把它和 KV 数据分别写文件后假装原子一致。恢复流程是：

```text
独占目录
    ↓
读取并校验 COMMIT
    ↓
恢复 Raft 日志，检查 COMMIT 位置、任期和整个前缀指纹
    ↓
Node 从 Follower、commit_index=已保存位置、last_applied=0 开始
    ↓
打开 Store，恢复其原有 WAL / SSTable
    ↓
把 Raft 日志 [1, 已保存提交位置] 从头顺序执行到 Store
    ↓
last_applied 到达 commit_index，构造完成，才允许查询和交付新事件
```

构造 Node 的 recovered_commit 参数必须来自已经校验的持久提交依据，不能传日志长度。恢复后的提交下界也保护已经执行过的前缀，避免从 commit_index=0 开始时误允许覆盖它。

Store 里可能是已执行前缀的完整结果，也可能是崩溃时只执行到一半的结果。当前命令只有 Put 和 Delete：重放完整前缀后，每个 key 最后一次 Put 决定最终值，最后一次 Delete 决定不存在；之前已执行过哪些条目不改变最终结果。连接层不允许绕过 Raft 写 Store，因此 Store 不应存在该已提交前缀之外的业务写入。

这是依赖当前命令语义的恢复策略，不保证命令只执行一次。Store.Delete 的“之前是否存在”返回值在重放时可能不同，连接层不把它作为持久客户端结果。将来增加自增、事务、外部副作用或持久请求去重时，必须改成能把数据变化与执行进度一起持久化的方案；不能直接沿用本版重放方式。

代价也很明确：重开要重新执行整个已提交前缀，会再次追加 Store WAL，并可能触发刷盘；执行和元数据同步会阻塞当前协议事件。第一版保留完整 Raft 日志，先选择容易解释和验证的恢复方式；本轮没有性能结论，也没有增加 Raft 快照或截断。

## 每个节点目录放什么

```text
node-directory/
    LOCK                    连接层的稳定独占锁
    COMMIT                  当前持久提交依据
    COMMIT.tmp              下一版本的临时文件，发布成功后消失
    raft                    Raft 的任期、投票和日志
    raft.lock               Raft Storage 独占锁
    data.lock               Store 独占锁
    data.manifest           Store 有效文件集合
    data / data.wal.N       Store 的当前及冻结 WAL
    data.sst.N              Store 的 SSTables
```

COMMIT 是固定 52 字节、小端编码：`KVC1 | 本节点及两个排序后的 peer ID | commit_index | 该条目 term | 前缀指纹 | 文件 CRC32`。每次以旧指纹和下一条日志的完整内容生成新指纹；启动重算整个已提交前缀，用于发现意外混放或不一致。指纹和 CRC32 不承担安全认证功能。

CreateNew 要求目录不存在；OpenExisting 要求已有 LOCK、COMMIT、Raft 文件和 Store 元数据，不会因为文件丢失就初始化一个空节点。初始化中途崩溃、尚无正式 COMMIT 的目录会拒绝重开。两种模式都不能同时打开同一目录；目录内文件只由这个连接层及其拥有的 Raft/Store 使用，不要用单机 CLI 直接写 data 路径，也不要独立替换其中某套数据。

## 本地读取与故障边界

LocalGet 读取本节点已经执行的 Store。隔离的旧 Leader 仍可能读到 Alice，而多数派已把 name 更新为 Bob。因此它是观察本机状态的接口，不承诺线性一致；真正的客户端查询规则到第 17 步处理。

COMMIT、Raft 或 Store 的 CRC/格式错误仍然拒绝打开，不能凭空修复。默认两套日志都采用严格恢复，不完整尾部也会报错。需要显式接受截尾时，可分别设置 Options.store_recovery 或 Options.raft_recovery；截掉未完成的变化后，仍要通过提交依据与日志前缀校验。Store 丢失的完整文件或元数据不会在本轮自动重建。

## 这次涉及的 C++ 写法

`std::unique_ptr<Node>` 和 `std::unique_ptr<Store>` 表示连接层独占两个对象。连接层销毁时，两者自动释放，Store 析构先等待其后台工作退出；这不是 Java 的共享引用和垃圾回收。

成员按声明顺序的反方向析构，所以 DirectoryLock 声明在 node/store 之前，让它最后释放。构造中途抛异常，已经构造的成员也会自动析构，不需要等一个“构造成功”的对象再手动清理。

DirectoryLock 在头文件里只声明，在 cpp 中定义；持有它的 unique_ptr 可以放在头文件，但拥有者的析构函数放到 cpp 定义，此时类型已经完整。和前面的 Storage 所有权一样，这是减少头文件暴露内部实现的一种做法。

`switch (command.type)` 根据枚举分派 Put、Delete、NoOp；每个分支末尾的 break 结束该分支，避免继续落入下一个分支。

## 验证记录

新增 15 个测试函数：

1. 新建/重开、配置校验、独占目录、错误调用不破坏可用性。
2. 恢复提交下界和严格逐条执行确认，不能跳号或越界。
3. 未提交提议不进入 Store，NoOp 不创建 key，确认后所有节点执行。
4. Put/Delete 顺序、删除不存在的 key、空字符串、二进制字节、重复心跳不重复应用。
5. 隔离节点的未提交写入不可见，本地旧读、重连后数据追赶和旧提议识别。
6. 无网络时重开恢复已提交 KV，保留但不执行未提交尾部，保护恢复后的提交前缀。
7. 实际 SSTable 刷盘、合并、删除及恢复重放。
8. COMMIT 逐字节截断、CRC 损坏、超长和临时文件处理。
9. 身份、位置、任期及整个日志前缀与 COMMIT 的绑定。
10. 必需文件丢失及未完成初始化拒绝打开，不自动变成空节点。
11. Leader/Follower 在提交发布与执行的五个阶段注入异常，失败后禁止读取及释放消息。
12. 真实 Store 刷盘准备失败后停止，重开依据已持久化提交补齐操作。
13. 12 个子进程强杀场景：两种角色各五个边界，以及批量执行中途两个边界。
14. 子进程限制文件大小，造成真实的一字节 Store WAL 部分写入；严格拒绝，显式截尾后重放恢复。
15. 两个固定种子各 250 个混合命令、消息乱序/重复/丢弃、时间和重启事件；实际 Store 查询对照独立 map，包含自动刷盘和合并，通信恢复后收敛。

普通构建和开启编译警告、AddressSanitizer、UndefinedBehaviorSanitizer 的构建通过全部 19 组 CTest，共 227 个测试函数；两种构建均可运行 replica_demo。已有单机与纯 Raft 验证继续通过。没有真实三进程网络、实际断电或吞吐量/尾延迟结论；这些还在后续步骤。

建议先观察演示阶段 3、4、8，再读 `ApplyCommitted`，理解正常执行后读 `SaveCommit` 与构造函数的恢复顺序。本步结束后先讨论；下一步才接 gRPC 和三个真实进程。
