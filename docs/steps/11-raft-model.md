# 第 11 步：Raft 基础模型

本轮建立独立 Raft 模块，描述节点状态、日志条目和消息，并提供手动推进的逻辑时间。尚未实现选举、消息处理、日志复制或持久化，也不调用 Store。完成这一步意味着基础模型可编译、可观察、可测试，不意味着三节点已经能够达成共识。

后续第 12 步已加入可选 Storage，并把 NodeConfig 移到 types.h 供两个模块使用。本页保留第 11 步的行为范围；当前的持久化接口和打开方式见 [第 12 步](12-raft-storage.md)。下方直接编译命令已补齐当前依赖。

## 先看现在能做什么

```cpp
#include "kvstore/raft/node.h"

using namespace std::chrono_literals;
namespace raft = kvstore::raft;

raft::Node a({1, {2, 3}});
raft::Node b({2, {1, 3}});
raft::Node c({3, {1, 2}});

a.AdvanceTime(100ms);
a.AdvanceTime(150ms);

const auto state = a.status();
// state.id == 1
// state.role == raft::Role::Follower
// state.term == 0
// state.logical_time == 250ms
// b.status().logical_time == 0ms
```

这是三个独立的内存对象，还不是三个联网进程。`{1, {2, 3}}` 表示自己的 ID 是 1，另外两个成员是 2 和 3；peers 不包含自己。ID 必须非零，另外两个成员必须互不相同，也不能等于自己。

每个节点初始为 follower，任期 0、日志为空，没有投票对象或已知 leader。逻辑时间推进后这些协议状态仍然不变：选举超时逻辑属于第 13 步。本轮没有自动选举，也没有用 sleep 等真实时间。

## 为什么单独做一个模块

```text
kvstore_engine：现有单机 Store

kvstore_raft：本轮新增的 Raft 基础模型
      ↑
raft_model_test
```

`kvstore_raft` 是独立静态库，只有 C++ 标准库依赖，不链接 `kvstore_engine`。现有 `kvstore` 程序仍使用单机引擎。这样可以先验证协议的状态变化，再单独处理已提交操作如何应用到 Store。

本轮没有添加线程、锁、网络或新文件格式。Node 是单线程模型，未来由调用方按顺序送入时间事件和消息；这不代表未来 gRPC 的多个回调可以直接并发修改同一个 Node。

## 每个文件负责什么

| 文件 | 职责 |
| --- | --- |
| `include/kvstore/raft/types.h` | NodeId、Term、LogIndex、角色、命令、日志条目、持久状态及四种消息结构 |
| `include/kvstore/raft/node.h` | 节点配置、可读状态和 Node 接口 |
| `src/raft/node.cpp` | 校验本机成员配置、返回状态、推进逻辑时间 |
| `tests/raft_model_test.cpp` | 验证节点初始化、配置边界与时钟行为 |
| `CMakeLists.txt` | 添加 `kvstore_raft` 库及 `raft_model_test` 测试入口 |

头文件里的结构描述“数据长什么样”，node.cpp 才包含当前能执行的行为。没有在声明里放空的选举/RPC 处理函数，以免接口看起来已经能运行协议。

## 三种角色

| 角色 | 后续完整协议中的职责 | 本轮状态 |
| --- | --- | --- |
| Follower | 接收 leader 的复制和心跳，响应投票请求 | 所有新节点从这里开始 |
| Candidate | 发起一轮选举，争取多数票 | 只有枚举定义，尚不发生切换 |
| Leader | 接收客户端操作、复制日志并推进提交 | 只有枚举定义，尚未选出 |

三个成员的多数派是两个成员，包含自己在内。`majority_size()` 只计算数量，并不代表节点已经拿到两张票或两个日志确认。

每个 Node 只校验自己的配置是否合法。三个 Node 是否被配置成同一组成员，由搭建集群的调用方负责；本轮还没有集群通信来发现配置不一致。

## 任期、日志和命令

```cpp
struct LogEntry {
    LogIndex index;
    Term term;
    Command command;
};
```

实际日志条目的示意：

```text
index  term  command
1      1     Put(name, Alice)
2      1     Put(city, Vancouver)
3      2     Delete(name)
```

- index：日志中的位置，从 1 开始；0 表示空日志前缀，不在 vector 中放一条假的第 0 条记录。
- term：条目由 leader 接收时所在的任期，用来区分不同任期产生的历史。
- command：后续交给状态机执行的操作。当前只有 Put/Delete，使用自有字符串保存 key/value，不引用 Store 的 WAL 记录类型。

空字符串值仍是合法 Put；Delete 的 value 按约定为空。当前这些 struct 只是内存数据形状，尚未执行入站消息或日志序列的合法性检查，也没有序列化格式。

当前 Node 没有追加日志的公开接口，因此新节点的日志保持为空。后续日志持久化和复制步骤会补上对应操作及检查，不能通过修改状态观察接口绕过协议。

## 节点保存什么状态

`PersistentState` 表达后续需要持久化的内容：

| 字段 | 含义 | 初始值 |
| --- | --- | --- |
| current_term | 节点已知的最新任期 | 0 |
| voted_for | 在当前任期投给了谁 | nullopt，尚未投票 |
| log | 有序的 Raft 日志条目 | 空 vector |

名字中的 Persistent 表示协议对它的持久化要求，**本轮它仍只在内存里**。第 12 步才实现落盘和恢复，并明确什么时候必须同步后才能回复对方。

`NodeStatus` 是一次观察得到的值副本：

| 字段 | 含义 |
| --- | --- |
| id、role、term | 当前节点是谁、处于什么角色和任期 |
| leader_id | 当前知道的 leader；未知为 nullopt |
| last_log_index、last_log_term | 最后一条日志的位置和任期；空日志均为 0 |
| commit_index | 已知已经提交到哪个日志位置 |
| last_applied | 已按顺序执行到哪个日志位置 |
| logical_time | 从这个对象创建开始，调用方累计推进的逻辑时间 |

三件事要区分：日志存在表示已经保存这条命令；commit 表示已经确认可以执行；apply 表示已经执行到状态机。本轮日志为空，last_log_index、commit_index、last_applied 都为 0，没有执行任何 KV 操作。

## 四种消息先定义形状

| 消息 | 字段 | 后续用途 |
| --- | --- | --- |
| RequestVoteRequest | term、candidate_id、last_log_index、last_log_term | 请求对方投票，提供自己的任期和日志新旧信息 |
| RequestVoteResponse | term、vote_granted | 告知对方自己的任期，是否同意投票 |
| AppendEntriesRequest | term、leader_id、prev_log_index、prev_log_term、entries、leader_commit | 说明要接在什么历史后面、追加哪些条目、leader 已提交到哪里 |
| AppendEntriesResponse | term、success | 告知自己的任期，以及是否接受本次追加请求 |

entries 为空时可以作为心跳，但仍必须检查任期和前缀，不能因为没有条目就无条件返回成功。这些规则在后面的处理函数中实现。本轮只定义字段，不收发消息，不自动回复。

字段参考 [Raft 原始论文 Figure 2](https://raft.github.io/raft.pdf)。这是内存结构，后续 gRPC 的 protobuf 格式会在通信步骤中定义。

## 手动时间怎么用

`AdvanceTime(100ms)` 表示“告诉这个节点，逻辑上又过去了 100 毫秒”，函数本身立即返回，不等待 100 毫秒。

```text
初始：       A=0ms，   B=0ms，C=0ms
A 前进 100： A=100ms， B=0ms，C=0ms
A 前进 150： A=250ms， B=0ms，C=0ms
```

未来测试就能精确决定哪个节点先超时，以及在什么时刻送达消息。实际服务可以由外部事件循环根据经过的时间调用同一接口。当前没有超时阈值、随机选举时间或后台计时线程。

负数会抛出 `invalid_argument`，超出毫秒计数可表示的上限会抛出 `overflow_error`。检查在加法前执行，因此出错时不会改变原有时间，也不会发生整数绕回。

## 这轮涉及的 C++ 写法

- `namespace kvstore::raft`：C++17 的嵌套命名空间简写，可以理解为把 Raft 类型放在独立的命名区域，不等于 Java 的目录/package 自动导入机制。
- `using NodeId = std::uint64_t`：给类型取别名，表达用途；它不是新的独立类型，编译器仍把 NodeId、Term、LogIndex 看作同一种整数类型。
- `enum class`：通过 `Role::Follower` 这样的名字访问枚举值，不会自动当作整数使用。
- `Node({1, {2, 3}})`：花括号初始化 NodeConfig 及其中的 vector；C++ 不需要 Java 的 `new`，这里对象直接存在于当前作用域。
- `Node(NodeConfig config)` 接收一份值，再用 `std::move(config)` 把本地副本的内容交给成员，避免再复制 peers。传入普通左值配置时，先复制到参数，不会把调用方的配置搬空。
- `status()` 返回值副本；`persistent_state()` 返回 `const` 引用，只读查看节点拥有的状态。引用不是独立存储，不能在 Node 销毁后继续使用。
- `100ms` 来自 `std::chrono_literals`，带时间单位；AdvanceTime 接收 `std::chrono::milliseconds`，避免把秒或毫秒当作无单位整数混用。

## 编译、运行与验证

在项目根目录执行：

```sh
cmake --build build --target raft_model_test
./build/raft_model_test
```

若是全新构建目录，先按 README 配置 CMake。直接编译也可以，只需这个独立模块：

```sh
c++ -std=c++17 -Wall -Wextra -Wpedantic -I include src/raft/node.cpp src/raft/storage.cpp src/raft/storage_codec.cpp src/file_io.cpp tests/raft_model_test.cpp -o build/raft_model_test
./build/raft_model_test
```

成功输出 `All 4 Raft model tests passed.`。四个测试覆盖：

1. 三个节点的初始角色、任期、空日志、提交/应用位置及多数派。
2. 零 ID、成员数量不对、自身出现在 peers、重复 peer 等非法配置。
3. 逻辑时间只推进指定节点，多次推进可累加；当前不会自动启动选举。
4. 负数及溢出被拒绝，出错前后的状态不变，最大合法时间仍可表示。

本机普通构建及开启 `-Wall -Wextra -Wpedantic`、AddressSanitizer、UndefinedBehaviorSanitizer 的构建均通过完整 15 组 CTest，共 160 个测试函数，构建无警告，未报告内存或未定义行为错误。原有单机引擎的 156 个测试继续通过。当前模型没有并发线程，因此本轮没有增加 ThreadSanitizer 检查。

下一步是第 12 步 Raft 日志与投票状态持久化。等本步讨论清楚，再设计磁盘格式和同步边界。
