# 第 14 步：Raft 日志复制、冲突修正与提交

> 路径说明：本文记录本步骤完成时的文件位置。后续目录已按模块整理，当前路径见[第 16a 步](16a-project-layout.md)；构建与运行命令以[项目 README](../../README.zh-CN.md)为准。

本轮让三个 Raft 节点复制 Put/Delete 日志，并确认哪些日志已提交。仍然是单线程事件模型、三个对象和三份独立磁盘文件。下一步才把已提交命令交给 Store 执行；本轮 `last_applied` 保持 0。

本文件保留第 14 步交付范围；第 15 步已新增 ReplicatedStore 和 replica_demo 来执行到实际 Store，见 [第 15 步说明](15-raft-store.md)。原 raft_demo 仍保留纯协议演示。

## 先运行，再看代码

在项目根目录执行：

```sh
cmake --build build --target raft_demo
./build/raft_demo
```

演示现在有九个阶段。重点看三个位置：

| 阶段 | 节点 1 的 last_log_index | 节点 1 的 commit_index | 含义 |
| --- | --- | --- | --- |
| 2，节点 1 当选，空操作提交 | 1 | 1 | 建立当前任期的提交边界 |
| 3，本地写入 Put name=Alice，消息尚未交付 | 2 | 1 | 本机落盘，但还没得到多数确认 |
| 4，交付消息 | 2 | 2 | 多数确认，随后通知其他节点 |
| 5，隔离节点 1，再写 name=isolated | 3 | 2 | 旧 Leader 只能在本机追加，不能独自提交 |
| 6，节点 2 当选并提交空操作、Delete name | 3 | 2 | 节点 1 仍隔离；节点 2、3 已到 index=4 |
| 7，恢复通信 | 4 | 4 | 节点 1 的未提交后缀被新 Leader 修正 |
| 8，重开节点 1 | 4 | 0 | 日志落盘，提交位置暂不持久化 |
| 9，收到匹配的 Leader 心跳 | 4 | 4 | 重新获知已提交位置 |

输出中的 `last_entry` 展示日志尾部命令。这些命令还没有执行到 KV 引擎；演示不会调用 Store.Get，也不创建 Store WAL。

运行新增测试：

```sh
cmake --build build --target raft_replication_test
./build/raft_replication_test
```

成功输出 `All 17 Raft replication tests passed (2 persistence-boundary process crashes).`。如果旧构建目录第一次构建这个新目标提示找不到规则，先执行一次 `cmake -S . -B build -DBUILD_TESTING=ON`，让 CMake 加入新目标；普通代码修改不需要重复配置。

## 本次改了哪些文件

| 文件 | 改动及原因 |
| --- | --- |
| `include/kvstore/raft/node.h` | 增加提议、读取已提交前缀的接口，以及每个 follower 的复制进度 |
| `src/raft/replication.cpp`，新增 | 集中实现发送、重试、接收、修正后缀和多数派提交，方便与选举分开阅读 |
| `src/raft/node.cpp` | 当选时初始化复制进度、持久化当前任期空操作；角色变化时清理旧复制请求 |
| `include/kvstore/raft/types.h` | 增加 NoOp 命令类型，保留原来的消息字段 |
| `src/raft/storage_codec.h/.cpp` | 共用命令校验，支持持久化 NoOp；文件布局和 Put/Delete 编码保留 |
| `include/kvstore/raft/storage.h` | 更新后缀替换接口的注释：协议层现已负责保护已提交前缀 |
| `src/raft_demo.cpp` | 在原演示上增加写入、提交、冲突追赶及重开观察 |
| `tests/raft_replication_test.cpp`，新增 | 日志复制与安全性测试 |
| `tests/raft_election_test.cpp` | 保留选举验证，更新“只允许空心跳”的上一轮范围断言 |
| `CMakeLists.txt` | 将复制实现接入原 Raft 库，增加一个独立测试目标 |

Store 和底层文件 I/O 实现没有修改；Raft 仍不链接单机引擎。README、GOAL、ROADMAP 更新本轮状态。

## 一次 Put 如何走完

```text
调用方：Leader.Propose(Put name=Alice)
    ↓
Leader 分配 index、使用当前 term，追加并同步自己的 Raft 日志
    ↓
返回 Proposal{index, term}；待发队列放入 AppendEntries
    ↓ 调用方交付消息
Follower 检查前缀，追加/修正日志，同步后回复 success
    ↓ 调用方交回 Leader
Leader 更新该 follower 的 match_index，检查多数派和任期
    ↓
推进 commit_index，发送携带新 leader_commit 的 AppendEntries
    ↓
Follower 也推进 commit_index
```

`Propose` 返回仅表示命令已经进入这位 Leader 的本地持久日志。它没有等待网络，也不是客户端写成功的响应。隔离的旧 Leader 仍可能接受本地提议，但不能获得多数派确认，提议也可能被之后的 Leader 覆盖。

调用方保留 `(index, term)`；观察提交时还要确认该位置最终属于同一条提议。单看 `commit_index >= index` 不足以判断一个旧 Leader 上的提议成功，因为这个 index 可能在后续任期被其他命令替换。客户端成功、超时和重试语义到接入步骤再定义。

`CommittedEntries(after)` 返回 `(after, commit_index]` 的日志副本，包括 NoOp；没有越过提交位置的读取权限，也不推进执行位置。调用方可以保存一个游标避免每次复制完整前缀。重启后需重新确认提交位置，再继续读取。

## Leader 怎么知道该给谁发什么

每个 follower 都有独立的 Peer 状态：

| 字段 | 含义 | 举例 |
| --- | --- | --- |
| `next_index` | 下一次尝试发送的第一条日志位置 | 5：发送从第 5 条开始的内容，先检查第 4 条 |
| `match_index` | 已经通过该 peer 回复确认匹配的位置 | 4：确认其前 4 条与 Leader 一致 |
| `pending` | 正在等待回复的一份请求和 rpc_id | 包含原始前缀、日志批次和 leader_commit |
| `acknowledged_commit` | 该 peer 已确认获知的提交位置 | 用来判断是否还要补发提交通知 |

刚当选时，`match_index=0`，`next_index=当选前日志末尾+1`。先持久化当前任期 NoOp，再从这个位置发出第一批。不能因为对方投过票，就假定它拥有完整日志。

每个 peer 同时只等待一份请求；每批最多 64 条、按 Raft 后缀记录编码计不超过 2 MiB，完整条目不拆开，单条最大命令仍能放入一批。收到成功回复后立即发送下一批；某个慢节点不会挡住另一个节点的复制。

心跳到期时，若没有等待回复的请求，发送新的 AppendEntries；若有，就重发原请求，保留相同 rpc_id 和内容。这样慢响应不会因为每轮心跳都更换编号而永远失效。更新的命令与提交位置等该请求回复后再发送。重复成功、已经处理完的拒绝、旧任期或不匹配编号的响应不会重复推进或倒退复制进度。

## Follower 怎样补日志和修正冲突

请求携带 `prev_log_index / prev_log_term`，表示“这批日志之前的最后一条，应该在这个位置、属于这个任期”。

```text
Leader：  [1,t1,A] [2,t2,B] [3,t3,C] [4,t3,D]
Follower：[1,t1,A] [2,t2,B] [3,t2,X] [4,t2,Y] [5,t2,Z]

收到 prev=(2,t2)，entries=[3,t3,C] [4,t3,D]
                         ↓
保留前两条，从 index=3 开始替换整个后缀
Follower：[1,t1,A] [2,t2,B] [3,t3,C] [4,t3,D]
```

若 prev 不存在或任期不同，Follower 返回 false，日志和提交位置不动。Leader 将这个 peer 的 `next_index` 减 1，重新尝试，直到找到共同前缀。当前采用逐条回退，落后很多时请求数量较多；未实现按冲突任期跳跃的优化。

前缀匹配后，跳过已经完全一致的条目，只在第一条任期不同或本机尚不存在的位置调用 Storage.ReplaceSuffix。同步成功才回复 true。空心跳、重复请求或只覆盖相同前缀的短请求，不会擅自截掉本机后面的条目。

例如本机有 10 条，请求只确认到第 5 条，即使 leader_commit=10，也只能根据这份请求确认到第 5 条。计算为：

```text
covered = prev_log_index + entries.size()
commit_index = max(原 commit_index, min(leader_commit, covered))
```

已提交的位置不得被替换；相同 `(index, term)` 也不得出现不同命令。这两种情况代表协议历史已经违反约束，节点停止服务并保留磁盘，不尝试“修复”已提交数据。正常格式错误被提前忽略；写入开始后的持久化失败也会停止节点，不放出成功回复。

## 多数派与 NoOp

三节点中至少两个节点持久化同一前缀，才达到多数。Leader 将自己的日志末尾和两个 follower 的 match_index 排序，中间值就是多数节点共同覆盖到的位置；还要求该位置的日志属于当前任期，才能据此推进提交。

例如覆盖进度为 `[8, 8, 2]`，多数覆盖到 8；`[8, 2, 2]` 则只覆盖到 2。当前任期的第 8 条提交后，它之前的整个前缀也被确认。具体规则对照 [Raft 原始论文 Figure 2、§5.3 和 §5.4.2](https://raft.github.io/raft.pdf)。

如果重启后已有旧任期日志，而一直没有新客户端写入，新 Leader 不能只统计这些旧日志的副本数就推进提交。因此当选时自动追加一条当前任期的 NoOp；它参与正常持久化、复制和多数确认，但不修改任何 key。NoOp 提交后，前面的旧日志也有了提交边界。

NoOp 的 key/value 都必须为空，Propose 不允许调用方直接提交 NoOp。磁盘操作标签增加 `3=NoOp`，保留 `1=Put`、`2=Delete` 及原 KVR1 文件布局；新程序可以读取旧文件，旧程序不能读取新增 NoOp。这不是网络协议格式。

## 重启与执行的边界

本轮持久化的仍是任期、投票和日志；commit_index 和复制进度是内存状态。重开后先从 Follower、commit_index=0 开始，保留日志，从现任 Leader 的匹配请求重新得知提交位置；全体重启则靠新 Leader 的当前任期 NoOp 重新确认前缀。

日志存在不等于本机已经确认提交，提交也不等于执行到 Store。第 15 步需要解决执行顺序，以及执行进度和 Store 状态一起恢复的问题。本轮不增加线程、锁、gRPC、客户端查询、Raft 快照或日志物理压缩。原有 64 MiB 活跃日志、100 万条、1 MiB 命令和 16 MiB 存储变化限制保留；历史追加文件继续增长。

## 这次涉及的 C++ 写法

`std::optional<PendingAppend>` 表示“可能有一份待确认请求”；有值时它拥有整个对象，`peer.pending->request` 通过 optional 的 `->` 访问其中成员。`reset()` 销毁这份对象，恢复为空。

`{entries.begin() + different, entries.end()}` 这里给 vector 的范围构造函数提供两个迭代器，复制 `[different, end)` 的条目；不是取数组中两个元素。范围左闭右开，与 Java 的 `subList(different, size)` 边界相似，但这里生成独立副本。

`std::array<LogIndex, 3>` 是长度固定为 3 的容器；这里对应固定三成员配置。`std::move(request)` 允许把 request 内部字符串和 vector 的所有权转给 pending，减少复制；移动后不再依赖原 request 的内容。这和 Java 多个引用指向同一对象的语义不同。

## 验证记录

新增 17 个测试函数，分为以下场景：

1. 仅 Leader 接受提议，非法命令不写磁盘、不破坏可用状态。
2. 当选不直接提交；NoOp 和普通提议都必须等待多数确认，一个 follower 不可用仍可提交。
3. 读取提交游标的范围、副本所有权与边界。
4. 缺少前缀、任期不匹配拒绝；匹配后才持久化和推进提交。
5. 重复、短请求与延迟空心跳不截尾、不超额提交、不倒退提交。
6. 第一处冲突后缀替换、Delete 保存与重开恢复。
7. 覆盖已提交条目或同 index/term 不同命令时，保留磁盘并停止服务。
8. 非法位置、任期、类型、大小、NoOp 内容及条目顺序被忽略。
9. 错误编号、旧任期、重复成功和延迟拒绝不改变正确进度。
10. 多轮心跳重发保持原始请求，延迟回复仍可完成这一轮。
11. 140 条旧日志逐条回退、分三批追赶；旧任期多数不足以提交，直到当前任期 NoOp 被确认。
12. 接近 1 MiB 的大命令按字节预算分批，最终一致。
13. 少数派不能独自提交；新 Leader 的提交保留，旧 Leader 重连后修正未提交后缀。
14. 三节点全部重开，恢复日志而不猜测提交；无需新客户端命令也能重新确认旧前缀。
15. Follower 追加、Leader 提议、当选追加 NoOp，在写完/同步完/更新内存三个阶段注入失败；失败后不释放消息。
16. 两个真实子进程在日志同步后、消息输出前被强杀，重开恢复已写日志但不宣称提交。
17. 三个固定种子各 500 个混合命令、时间、乱序、重复、丢弃和重开事件；跨重启记录提交历史，验证同位置不分歧、已知提交不丢失、新 Leader 包含既有提交，恢复通信后收敛。

普通构建及开启 `-Wall -Wextra -Wpedantic`、AddressSanitizer、UndefinedBehaviorSanitizer 的构建通过全部 18 组 CTest，共 212 个测试函数；两种构建的演示均成功。Raft 当前单线程，本轮不增加 ThreadSanitizer 检查。测试是可控消息环境中的实现验证，不等于真实三进程故障测试、形式化证明、实际断电验证或性能 benchmark。

建议阅读顺序：先看演示阶段 3～5，再看 Propose → Follower 的 Handle(AppendEntriesRequest) → Leader 的 Handle(AppendEntriesResponse) → AdvanceCommit。理解复制闭环后，再看追赶、重试和 NoOp。第 14 步到这里结束，讨论后再进入 Store 接入。
