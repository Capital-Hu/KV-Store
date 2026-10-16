# 第 13 步：Raft 选举、心跳与角色切换

本轮让带 Storage 的节点真正处理超时和消息，能选出 leader、维持心跳，并在故障后重新选举。仍在一个进程内模拟交付，不连接 gRPC，不接受 KV 写入，不复制数据或推进提交。

本文件保留第 13 步当时的交付范围。第 14 步已扩展同一个 raft_demo 和 Node，加入日志复制与提交；运行当前版本时，请按 [第 14 步说明](14-raft-replication.md) 阅读九阶段演示。

## 先运行演示

在项目根目录执行：

```sh
cmake --build build --target raft_demo
./build/raft_demo
```

演示显示六个阶段：

```text
1. 三个节点都是 Follower，term=0。
2. 节点 1 超时，持久化新任期与自投票，拿到多数票，成为 term=1 的 Leader。
3. 节点 1 周期性发送心跳，节点 2、3 继续跟随。
4. 隔离节点 1；节点 2 超时，得到节点 3 的票，成为 term=2 的 Leader。
5. 节点 1 关闭后重开：从自己的文件恢复 term=1，但角色从 Follower 开始。
6. 节点 1 收到节点 2 的更高任期心跳，持久化 term=2，跟随节点 2。
```

这是三个对象与三份独立临时磁盘状态，不是三个联网进程。演示用固定的 150/220/290ms 选举超时和 50ms 心跳间隔，使输出可预测；这些都是逻辑时间，不会 sleep。临时目录在结束时清理，不使用手动运行 Store 的文件。

## 节点如何交换消息

Node 不直接发送网络请求，而是维护待发消息队列：

```text
A.AdvanceTime(达到选举超时)
    ↓
A.outbox：[RequestVote A→B，RequestVote A→C]
    ↓ 调用方 DrainMessages 取出
B.Step(RequestVote)
    ↓
B.outbox：[RequestVoteResponse B→A]
    ↓ 调用方交回 A.Step
A 得到多数票，成为 Leader
    ↓
A.outbox：[AppendEntries A→B，AppendEntries A→C]，entries 为空
```

`AdvanceTime` 输入时间事件，`Step` 输入一条消息，`DrainMessages` 取走当前全部待发消息。测试决定何时交付、先交哪条、复制哪条、丢掉哪条；以后网络层也负责交付同一种事件。

Node 仍是单线程对象，不内部加锁。调用方必须依次处理这些事件，不能让多个 gRPC 回调直接并发修改它。

不带 Storage 的 `Node(config)` 保留第 11 步的被动模型：只能观察初始状态和推进时钟，Step 报错。带 Storage 的构造方式才启用协议，避免模拟选举时绕过持久化要求。

## 新增的消息外层

四种 RPC 内容保留原来的字段，在外面增加：

```cpp
struct Message {
    NodeId from;
    NodeId to;
    std::uint64_t rpc_id;
    MessageBody body;
};
```

body 是 RequestVoteRequest、RequestVoteResponse、AppendEntriesRequest、AppendEntriesResponse 中的一种。rpc_id 是调用方交付消息时保留的请求关联编号，响应原样返回；它不是 term 或日志 index，也不是新的磁盘格式。

节点检查接收方、本机 peer 身份、非零 rpc_id，以及请求中的 candidate/leader ID 是否与发送方一致。错误路由、非成员或明显不合法的日志元数据被忽略，不改变任期和计时。未来真实传输层必须提供可信的 peer 身份，本轮不处理伪造成员的拜占庭行为。

## 超时以后做什么

```text
Follower / Candidate 等待达到 election_timeout
    ↓
Storage.SetTermAndVote(current_term + 1, 自己)
    ↓ 同步成功
变成 Candidate，清除旧轮次未发送消息与计票
    ↓
重新抽取超时，计时归零
    ↓
向另外两个成员发送投票请求
```

节点先有自己的票；得到另一个成员的有效赞成票，就达到三节点多数派。只有当前任期、对应当前请求编号、来自不同 peer 的票才参与计数。重复响应不会重复执行当选过程，旧轮次或错误编号的响应不能使候选者当选。

当选时变成 Leader，已知 leader 设置为自己，立即发送心跳；之后由 AdvanceTime 按心跳间隔继续触发。不追加 no-op 或客户端命令，也不因为当选就提交已有日志，这些属于下一步。

选举规则与持久化时机对照 [Raft 原始论文 Figure 2、§5.2、§5.4.1](https://raft.github.io/raft.pdf)。下面描述的是本轮代码的接口和处理边界。

## 怎么决定投不投票

`Handle(RequestVoteRequest)` 会先接受有效消息里的更高任期并落盘，随后判断：请求任期等于当前任期、当前尚未投票或已投同一候选者、候选者日志至少与本机一样新。

日志新旧按 `(last_log_term, last_log_index)` 比较，先看任期，任期相同再看位置。假设本机末尾是 index=3、term=2：

| 候选者末尾 | 本机判断 |
| --- | --- |
| index=100、term=1 | 更旧，不投；不能只看长度 |
| index=2、term=2 | 更旧，不投 |
| index=3、term=2 | 一样新，可以投 |
| index=1、term=3 | 更晚任期，可以投 |

同意时，先把投票同步到 Storage，再重置选举计时，最后把回复放入 outbox。若收到更高任期但日志过旧的请求，只更新任期、拒绝投票，不重置计时，避免过旧候选者不断拖延本机选举。

同一候选者重复请求可以再次获得相同回复，不额外写相同投票；同任期不能改投别人。重启后仍遵守 Storage 保存的投票记录。

## 高任期、旧任期与角色变化

- 有效成员的四类 RPC 都可能带来更高任期；请求和响应都会先触发持久化更新、转成 Follower、清除旧 leader 和旧计票。
- 较高任期的响应即使不是当前等待的 rpc_id，也会更新任期；只有计票才要求请求编号匹配。
- 旧任期请求返回当前任期及拒绝结果，不重置选举计时；旧任期响应不计票。
- Candidate 收到同任期 leader 心跳，也会转成 Follower，但不会清空自己在这个任期已经投出的票。
- 分票时拿不到多数，候选者等待下一次超时，再增加任期开始新一轮。

节点改变角色时清除的是尚未由 DrainMessages 取走的旧消息。已经被调用方取走、正在传输的消息不能撤回，接收方仍靠任期和 rpc_id 处理它们。

## 心跳做什么，暂时不做什么

Leader 用空 entries 的 AppendEntries 表示心跳，携带自己的最后日志 index/term。Follower 在当前或更新任期的 leader 请求到达时重置选举计时，即使两边日志尚不匹配，也继续等待 leader 后续补齐。

回复 success 之前仍检查 prev_log_index / prev_log_term；没有匹配前缀不能因为“只是心跳”就答应。空心跳也不会删除 follower 额外的日志后缀。

当前只接受 entries 为空且 leader_commit=0 的心跳。非空条目或非零提交进度会返回 false，不修改日志或 commit_index。此时还没有 nextIndex/matchIndex、回退重试、日志追赶和状态机执行。

演示隔离节点 1 后，它仍可能在本地认为自己是旧任期的 Leader；节点 2 是新任期的 Leader。测试检查的是每个任期最多一个当选者，而非任何时刻只能有一个节点自认为 leader。隔离的旧 leader 没有获得“继续成功处理客户端请求”的能力；当前根本没有该接口，后续写提交和一致性查询必须分别处理多数派确认。

## 计时选项

`ElectionOptions` 默认心跳间隔 50ms、选举超时区间 150～300ms。区间内每次重新抽样；默认用随机源初始化，可显式提供 random_seed 使测试可重复。构造时检查心跳为正、选举最小超时大于心跳间隔、最大超时不小于最小值。

NodeStatus 增加 election_elapsed、election_timeout 和 heartbeat_elapsed。logical_time 仍记录调用方累计推进的时间，计时归零不会把它倒退。

一次大跨度 AdvanceTime 最多触发一次超时或一次心跳发送，按“此刻才重新获得执行机会”处理，不补发过去几小时内的所有事件。负数或逻辑时钟溢出在修改前拒绝；任期达到整数上限无法再选举时停止协议，不允许绕回 0。

## 持久化失败与重启

启动新选举、同意投票、接受更高任期都通过第 12 步 Storage 接口同步。只有成功后才修改相应角色或把可发送请求/回复放入 outbox。

如果处理事件时出现持久化或其他不确定错误，Node 进入不可用状态，清除待发消息。之后 Step、AdvanceTime、status、persistent_state 和 DrainMessages 都报错；静态配置仍可查看，usable() 可判断是否已经停止。已经成功取走的旧消息可能仍在传输，不能靠清空本地队列撤回它们。

重开后保留 term/voted_for/log，角色从 Follower 开始，计时与 RPC 关联编号重新建立；新选举必须进入更高任期。commit_index、last_applied 仍为 0，不从日志长度推导。

## 改了哪些文件，按什么顺序看

| 文件 | 本轮改动 |
| --- | --- |
| `include/kvstore/raft/types.h` | MessageBody 与 Message 外层，不改变磁盘日志编码 |
| `include/kvstore/raft/node.h` | ElectionOptions、可观察计时、Step/DrainMessages、计票与待发队列 |
| `src/raft/node.cpp` | 超时选举、持久化顺序、四类消息处理、心跳及错误停止 |
| `src/raft_demo.cpp` | 三对象、三份临时磁盘文件的可运行演示 |
| `tests/raft_election_test.cpp` | 可控消息交付、异常和进程中断测试 |
| `CMakeLists.txt` | 增加 raft_demo 和 raft_election_test；均只链接 Raft 模块 |

先看 AdvanceTime → StartElection，再看两个 RequestVote Handle，最后看 BecomeLeader → SendHeartbeats → AppendEntries Handle。不必一开始把所有辅助函数逐行读完。Storage 和 KV 引擎实现未改，磁盘格式也未改。

新增 C++ 写法主要是 `std::variant` 与 `std::visit`：variant 表示 body 同时只保存四种类型中的一种，visit 根据当前实际类型调用对应的 Handle 重载。它避免用 void 指针手动判断类型；与 Java 的多态接口或带类型分支的数据对象作用相近，但这里消息是直接拥有内容的值对象。

## 运行测试与结果

```sh
cmake --build build --target raft_election_test
./build/raft_election_test
```

成功输出 `All 20 Raft election tests passed (2 persistence-boundary process crashes).`。二十个测试覆盖：

1. 超时边界、自投票、当选、初始及周期心跳。
2. 心跳持续维持原任期。
3. 三方分票后新任期重选。
4. leader 隔离、剩余多数派选举与恢复通信后旧 leader 退位。
5. 错编号、旧任期、重复、非成员和旧轮次响应。
6. 四类 RPC 中的更高任期均触发退位并持久化。
7. 旧请求不重置计时。
8. 投票重置计时、同任期不能改票。
9. 更高任期但旧日志的候选者不能拖延选举。
10. 日志新旧先比较末尾任期，再比较位置。
11. Candidate 接受同任期 leader 心跳，旧回票不能使它重新当选。
12. 心跳检查前缀、不删除额外后缀、不假装接受数据或提交。
13. 身份、路由及基础元数据错误不影响任期或计时。
14. 无效计时配置、被动模型保护，以及 10/20/30 等非示例 ID 正常选举。
15. 固定 seed 的可重复、多轮变化的随机超时。
16. 大跨度时钟、时间溢出及任期上限。
17. 投票与自投票跨重启保留。
18. 发起选举、同意投票或观察高任期时，持久化错误不释放消息且停止服务。
19. 两次在同步完成、协议输出之前强杀进程，重启后仍不能在同任期改票。
20. 三个固定样本各 450 次随机交付/延迟/重复/丢弃/重开事件，历史上每任期最多一个 leader；恢复正常交付后能重新收敛。

普通构建与开启编译警告、AddressSanitizer、UndefinedBehaviorSanitizer 的构建均通过完整 17 组 CTest，共 195 个测试函数。演示也在两种构建中运行成功；补齐系统头文件后完整构建无警告。Raft 仍按单线程事件模型使用，本轮没有新增 ThreadSanitizer 检查。两个真实进程强杀不能代替断电或真实网络故障实验。

第 14 步再实现日志复制、冲突修正和多数派提交；本轮不开始这些工作。
