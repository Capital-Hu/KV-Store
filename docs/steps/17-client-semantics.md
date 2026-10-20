# 第 17 步：客户端、写入确认与一致性读取

这一步新增 C++ Client 库和 kvclient，将“找 Leader、提交写入、确认结果、读取”组织成对调用方明确的接口。NodeAdmin 和 kvadmin 保留，用于观察日志与本地状态。

## 先看四种结果

| ClientCode | 含义 | 是否可以把这次写入当成失败后直接重发 |
| --- | --- | --- |
| Ok | 写入已经提交，并在返回节点完成 Store 执行；读取则已经通过新的读屏障 | 不需要重发 |
| Unavailable | 在总期限内没有完成可用的 Leader 操作；对于写入，尚未发出任何可能被接受的写入 | 可以重新发起新的调用，但仍需结合业务顺序 |
| Unknown | 写入可能已执行，也可能没有；例如已发出请求但回复丢失、等待超时或中途失去 Leader 身份 | 不自动重发 |
| Rejected | 客户端在发送前发现参数非法 | 修正原因后再调用 |

`Unknown` 不是回滚。Leader 失去多数派时，某个写入先返回 Unknown，恢复通信后仍可能提交。若返回过提议编号，可以用原管理工具观察对应 `(index, term)` 是否执行；false 也不证明以后不会执行，返回未知时也可能没有收到编号。

本步没有请求 ID 去重或 exactly-once。即使 Put/Delete 的单次最终状态幂等，也不能随便重试：一次 Alice 写入超时后，别人写入 Bob；随后重试 Alice 仍可能覆盖 Bob。因此只自动重试明确的 CLIENT_NOT_LEADER 回复，该回复保证本节点尚未追加这条用户提议。任何可能已接受的写入都不会在另一个节点自动重放。

## 客户端如何找 Leader

调用方提供三个成员的 ID 和地址。每次操作在总期限内用 Inspect 探测成员身份与角色，优先选择观察到的最高任期中的 Leader，再发业务 RPC；Follower 返回的 Leader ID 只是下一次探测的提示。

角色观察本身不是多数派证明。它可能属于已隔离的旧 Leader；真正的写入成功和读取成功仍要求本次操作通过 Raft 提交路径。读取失败时可以换节点重试，因为读屏障的 NoOp 不修改 KV 数据；不会退回 LocalGet。

客户端关闭 gRPC 内置重试，避免传输层在应用重试策略之外重放写入。相关机制见 [gRPC retry 官方说明](https://grpc.io/docs/guides/retry/)。

## 写入成功现在是什么意思

```text
kvclient Put/Delete
  → Inspect 找 Leader
  → KeyValue.Write
  → Leader 追加并同步 Raft 日志
  → 多数派确认，推进 commit
  → 同步 COMMIT 依据
  → Store 追加并同步 WAL，更新 MemTable
  → 标记 applied
  → 返回 CLIENT_OK
```

成功不要求其他两个节点都已经执行，也不要求数据已经进入 SSTable；多数派复制和当前节点执行完成即可。Store WAL 已负责本地恢复。

服务端等待提交期间使用 unique_lock + condition_variable，等待时释放节点状态锁，因此接收 Raft 回复、计时和其他请求仍可推进状态。最多允许 64 个客户端调用在节点内等待，超额在追加前拒绝；客户端对业务写入的 RPC 错误仍保守地报告 Unknown，因为单靠传输状态不能一般性证明没有执行。

Propose 的旧行为保留：kvadmin 的 ACCEPTED 仍只是提议接收。本步新增 KeyValue 服务，不改变管理接口的承诺。

## Get 为什么可以保证一致性

每次正式 Get 都在 Leader 当前任期追加一条新的 NoOp，并等到它被多数派确认、按日志顺序执行。随后持有同一把节点状态锁检查角色和任期，再读取 Store。

```text
Get 请求到达
  → 创建本次独有的 NoOp 屏障
  → 新的多数派确认
  → 屏障及其前面的命令全部应用
  → 检查仍是该任期 Leader
  → 读取本地 Store，返回值或不存在
```

屏障在 Get 调用期间创建，不能用旧的心跳或以前提交过的 NoOp 代替。隔离的旧 Leader 即使还能读本地 Alice，也无法独自提交这条新屏障，因此不能通过正式 Get 返回旧值。并发写入可能位于读取之前或之后；成功读必须包含在读调用开始前已经成功完成的写入。

这是本项目选择的保守实现：把读也放入共识日志来确定顺序。每次读取都会增加 Raft 日志并触发持久化，NoOp 不修改 Store 的 KV 状态。尚未实现 ReadIndex、租约读取、读屏障合并或 Raft 日志截断。Raft 线性一致交互的背景见[论文第 8 节](https://raft.github.io/raft.pdf)。

本承诺针对成功的 Client.Get / KeyValue.Read。kvadmin local-get 仍读取指定节点的本地已执行状态，可能落后。

## 时间与关闭

- ClientOptions.timeout：一次操作的总期限，默认 5000ms，范围 1..30000ms。
- ClientOptions.rpc_timeout：单次业务 RPC 上限，默认 1500ms，范围 1..30000ms，同时受剩余总时间限制。
- 每次 Inspect 探测最多 150ms；尝试之间最多等待 20ms，并受剩余总时间限制。
- ServerOptions.client_wait_timeout：单个服务端等待提交的上限，默认 1000ms，范围 1..30000ms。

所以写入即使还有客户端预算，服务端也可能先返回 Unknown。等待期间周期性检查取消、停止、任期变化及已执行位置；关闭不会把尚未提交的请求变成成功。底层磁盘同步仍可能阻塞，不承诺硬实时超时。

客户端用 steady_clock 计算总预算，再转换为 gRPC 接受的 system_clock 时间点；macOS 两种时钟的 duration 类型可能不同，需要显式 duration_cast。deadline 只限制调用方等多久，不撤回已经提交的命令，见 [gRPC deadline 官方说明](https://grpc.io/docs/guides/deadlines/)。

## 如何运行

先按 README 启动三个 kvnode。已有网络构建目录更新后：

```sh
cmake --build build/network
```

在另一个终端运行：

```sh
cluster_members='1=127.0.0.1:50051,2=127.0.0.1:50052,3=127.0.0.1:50053'
./build/network/kvclient "$cluster_members" put name Alice
./build/network/kvclient "$cluster_members" get name
./build/network/kvclient "$cluster_members" delete name
./build/network/kvclient "$cluster_members" get name
```

写入成功输出 OK index=N term=T；Get 输出值，删除后输出 <not found>。不需要先手动确认哪个端口是 Leader。

可设置总期限：

```sh
./build/network/kvclient "$cluster_members" --timeout-ms 3000 get name
```

| CLI 退出码 | 含义 |
| --- | --- |
| 0 | 成功；写入已执行，读取找到了值 |
| 1 | 不可用，或请求被拒绝 |
| 2 | 参数/配置错误或程序异常 |
| 3 | 一致性读取成功，但 key 不存在 |
| 4 | 写入结果未知，不自动重试 |

C++ ClientResult 中 Ok + nullopt 表示成功读取到“不存在”；Ok + 空字符串则是存在的空值。库支持二进制 key/value，命令行使用 shell 参数，不能直接传递内含 NUL 的参数。

## 文件与 C++ 细节

| 文件 | 本次作用 |
| --- | --- |
| include/kvstore/rpc/client.h、src/rpc/client.cpp | Client 接口、发现、期限、结果分类和安全重试 |
| apps/kvclient_main.cpp | 命令行入口 |
| proto/raft_node.proto | 增加 KeyValue、WriteReply、ReadReply、ClientOutcome |
| src/rpc/node_server.cpp | 等待应用的写入与屏障读取 |
| include/kvstore/raft/node.h、src/raft/replication.cpp | ProposeReadBarrier；与普通提议共用追加流程 |
| include/kvstore/replicated_store.h、src/replica/replicated_store.cpp | 读屏障的协调层入口 |
| tests/rpc/process_fixture.h | 从原进程测试提取的公共临时目录、子进程和集群工具 |
| tests/rpc/client_process_test.cpp | 客户端结果、读写、故障和 CLI 验证 |

仍使用原有 NoOp 编码，磁盘格式不变；普通 Propose 继续拒绝调用方直接提交 NoOp。Client 用 Pimpl 隐藏生成的 gRPC 类型；unique_ptr 表示唯一所有权，析构在实现文件中定义。服务端等待槽用小型 RAII 对象计数，退出作用域时自动归还。

## 测试

```sh
ctest --test-dir build/network -R '^(raft_replication|client_process)$' --output-on-failure
ctest --test-dir build/network --output-on-failure
```

新增一项 Raft 屏障测试，覆盖 Follower 拒绝、旧确认不能提交新屏障、新多数派提交及重开恢复。新增一个 client_process CTest 组，包含八个测试函数：

1. 真实 Follower 明确拒绝、提交确认、二进制/空值、删除与每次 Get 的新屏障。
2. Leader 强杀后发现新 Leader，确认写入以及旧节点重开追赶。
3. 暂停自己启动的两个 Follower：本地有旧值仍不能成功读；写入只提议一次；恢复后允许提交或被新任期覆盖，不把 Unknown 当作回滚。
4. 可控响应服务：明确拒绝可换节点，丢回复/Unknown 不重发写入，读可以重试，非法输入不发 RPC，没有 Leader 不发写入。
5. 通过受控代理让真实集群完成写入后丢弃回复，验证 Unknown 不撤销数据且不重复写入。
6. 并发客户端读写与实际 kvclient 的 put/get/delete、缺失值、非法期限和 Unknown 退出码。
7. 非法成员和期限配置。
8. 有写入正在等待多数派时关闭 Leader，回调退出且不伪造成功。

进程测试只控制自己启动的子进程，并回收临时目录。测试里的 SIGSTOP 是限定的无多数派场景，不代替第 18 步完整网络隔离、延迟、丢消息矩阵。

本轮最终验证：

| 构建 | 结果 |
| --- | --- |
| 默认非网络构建 | 19/19 组通过，47.98 秒 |
| 普通网络构建 | 22/22 组通过，76.74 秒 |
| ASan/UBSan 网络构建 | 22/22 组通过，77.64 秒 |

新客户端组的八个测试和新增 Raft 屏障测试均通过；原有六个进程测试的函数体保持原样，仅提取公共 fixture。构建日志没有编译警告，文档链接和双语命令已核对。以上是测试耗时，不是性能数字。

暂停 Follower 会累积选举超时，恢复后可能出现新任期，所以不能强行断言那条未提交写入必定成功。该场景检查 Unknown 的两种合法结局；另用“真实提交完成后丢回复”的受控代理验证 Unknown 也可能对应已提交数据。

第三方 gRPC/Protobuf 仍复用 Release 依赖，本轮不新增网络层 ThreadSanitizer 检查。存储引擎、磁盘编码、依赖脚本和编辑器配置没有修改。

本步结束后停下来讨论，第 18 步尚未开始。
