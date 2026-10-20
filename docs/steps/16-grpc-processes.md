# 第 16 步：gRPC 与三个独立进程

> 路径说明：本文记录本步骤完成时的文件位置。后续目录已按模块整理，当前路径见[第 16a 步](16a-project-layout.md)；构建与运行命令以[项目 README](../../README.zh-CN.md)为准。

这一步把之前由演示程序转交的消息，换成真正的本机 TCP/gRPC 传输。每个 kvnode 进程独占自己的 ReplicatedStore 和数据目录，监听不同端口。Raft、Store、COMMIT 的实现及磁盘格式保持原有职责。

## 先构建网络功能

本机原有 Protobuf 29.3，但没有 gRPC。本轮把固定版本 gRPC 1.84.0 和编译需要的依赖构建到项目已忽略的 build/deps 内，使用匹配的本地 protoc 与插件，没有升级系统工具。脚本固定 gRPC 提交 `3252a89f10d8e92997862167ca7d095ecda85973`，依赖使用该提交记录的子模块版本。

实际本地生成器版本为 protoc 35.1，和这份 gRPC 的依赖匹配；原有系统 protoc 29.3 保留。

第一次执行，需要联网下载并编译依赖，耗时明显长于项目本身：

```sh
sh scripts/build-grpc.sh
```

已经成功构建的依赖可以复用。配置并编译网络程序：

```sh
cmake -S . -B build/network -DKVSTORE_BUILD_GRPC=ON -DBUILD_TESTING=ON -DCMAKE_PREFIX_PATH="$PWD/build/deps/install"
cmake --build build/network --target kvnode kvadmin
```

以后修改代码后，直接重新编译即可。CMake 根据 proto 文件生成的 `.pb.*`、`.grpc.pb.*` 都在 build/network/src/rpc/generated 内，不手改或提交生成文件。

编辑网络源文件时，IDE 也应使用启用了 gRPC 的 CMake 配置，让头文件目录和生成文件与实际构建一致。

默认 KVSTORE_BUILD_GRPC=OFF，原来的 build、单机 CLI、raft_demo 和 replica_demo 继续不需要 gRPC。新的 build/network 单独启用网络。其他环境若已有兼容的 gRPC/Protobuf CMake 包，也可以把 CMAKE_PREFIX_PATH 指向它们的安装前缀。

## 启动三个节点

下面命令都在项目根目录执行。第一次启动需要三个尚不存在的节点目录，父目录可提前创建：

```sh
mkdir -p build/cluster
```

分别在三个终端运行一条，保持进程运行：

```sh
./build/network/kvnode --id 1 --directory build/cluster/node1 --mode new --members 1=127.0.0.1:50051,2=127.0.0.1:50052,3=127.0.0.1:50053
```

```sh
./build/network/kvnode --id 2 --directory build/cluster/node2 --mode new --members 1=127.0.0.1:50051,2=127.0.0.1:50052,3=127.0.0.1:50053
```

```sh
./build/network/kvnode --id 3 --directory build/cluster/node3 --mode new --members 1=127.0.0.1:50051,2=127.0.0.1:50052,3=127.0.0.1:50053
```

三个进程使用相同的成员映射，各自的 id、目录不同。ID 不是写死的 1/2/3，仍支持任意三个不同的非零 ID；第一版固定成员数为三，不支持运行时改成员。地址目前只接受 `127.0.0.1:PORT`，用于本机可信环境，采用明文 gRPC，没有节点身份认证或跨机器部署配置。

运行一会儿会自动选主；不保证节点 1 当选。用第四个终端查看：

```sh
./build/network/kvadmin 127.0.0.1:50051 status
./build/network/kvadmin 127.0.0.1:50052 status
./build/network/kvadmin 127.0.0.1:50053 status
```

输出含 `id / role / term / leader / log / commit / applied`。leader=0 表示暂时未知；隔离时某个节点仍可能认为自己是旧 Leader，role 不代表当前已获得多数确认。

假设 status 显示 50052 对应 Leader，可发出提议：

```sh
./build/network/kvadmin 127.0.0.1:50052 propose-put name Alice
```

输出 `ACCEPTED index=N term=T`，只代表进入这位 Leader 的本地日志。把输出的 N、T 填到下方，观察该提议是否在本节点执行：

```text
./build/network/kvadmin 127.0.0.1:50052 applied N T
```

返回 false 时可稍后再次观察。随后查看三份实际数据：

```sh
./build/network/kvadmin 127.0.0.1:50051 local-get name
./build/network/kvadmin 127.0.0.1:50052 local-get name
./build/network/kvadmin 127.0.0.1:50053 local-get name
```

提交删除使用 `propose-delete name`。这些都是管理观察命令，LocalGet 仍可能读到落后数据，不承诺线性一致。提议发给 Follower 会报 FAILED_PRECONDITION，不自动转发；工具不自动寻找 Leader、不自动重试写入。

管理工具正常调用返回退出码 0，RPC 错误为 1，参数错误为 2，local-get 未找到为 3；applied 返回 false 是正常观察结果，退出码仍为 0。

## 怎样停止、重开

在节点终端按 Ctrl+C，或者给自己启动的节点发送 SIGTERM。进程会停止接收正常新操作、关闭 gRPC、等待发送和计时线程退出，再让 Store 完成已有的退出流程并释放目录锁。

重开同一节点时，把原命令的 `--mode new` 改成 `--mode existing`；目录和节点身份保持一致。new 不覆盖已有目录，existing 不补建丢失的必需数据文件。端口被占用时启动会失败，不共享监听端口；若启动已初始化数据目录后因监听失败退出，下次使用 existing 打开该目录。

断开一个节点后，其余两个可以选主、继续复制提交；旧节点以 existing 重开后通过网络追赶。这里是本轮的基本进程故障验证，系统性的网络隔离、延迟、丢消息验证仍在第 18 步。

## 一条消息现在如何走

```text
节点 A：ReplicatedStore.DrainMessages()
    ↓
编码成 Protobuf Envelope，放入发给 B 的队列
    ↓ A 的 B 发送线程
通过 gRPC RaftTransport.Deliver 发到 B 的端口
    ↓ B 的 gRPC 回调
解码、检查路由和成员 → ReplicatedStore.Step(message)
    ↓
B 产生 Raft 响应 → DrainMessages → 发给 A 的队列
    ↓ B 的 A 发送线程
通过另一次 Deliver 把响应送回 A
    ↓
A.Step(response)，更新复制进度和提交位置
```

本版把四种 Raft 消息都作为独立 Envelope 发送，延续已有的消息信箱接口。一次 Deliver 返回的 Empty/OK 只是本次交付完成；它不是 RequestVoteResponse 或 AppendEntriesResponse，也不能据此提交日志。真正的 Raft 回复保留原 rpc_id，通过反方向另一条 Envelope 到达。

这种方式让网络层只负责交付，Node 仍处理原来的 Message，不需要了解 gRPC。投票回复、复制回复的任期和编号检查继续由协议层完成。

## proto 文件、编码和生成代码

`proto/raft_node.proto` 定义：

- RaftTransport：一个 Deliver 方法，承载四类 Raft 消息。
- NodeAdmin：Inspect、Propose、LocalGet、Applied，用于本轮观察。
- Envelope：from、to、rpc_id，以及 oneof body。
- Command：Put/Delete/NoOp；key/value 使用 bytes，保留二进制和空字符串。

oneof 表示 body 四种类型中只选择一种，对应原来的 `std::variant`。发送时把 C++ 模型转成生成消息，接收时转换回来。网络编码独立于 Raft 日志和 Store WAL 的磁盘格式。

CMake 调用本地 `protoc` 和 `grpc_cpp_plugin` 生成消息类型、客户端 Stub 和服务端 Service 接口，然后编译链接；生成方式可对照 [gRPC C++ 官方教程](https://grpc.io/docs/languages/cpp/basics/)。以后修改 proto 后重新构建即可触发生成。

## 线程和锁如何安排

每个进程中有：

| 执行者 | 工作 | 是否操作 ReplicatedStore |
| --- | --- | --- |
| gRPC 回调线程 | 接收消息、管理调用 | 持有 state_mutex 后操作 |
| 一个计时线程 | 根据 steady_clock 过去的实际时间调用 AdvanceTime | 持有同一把 state_mutex |
| 每个 peer 一个发送线程，共两个 | 从各自队列取消息，执行有 deadline 的网络 RPC | 不直接操作 Store/Node |
| Store 已有后台线程 | 刷盘、合并 | 仍使用 Store 内部原有锁 |

state_mutex 保护整个 ReplicatedStore 调用及其 DrainMessages。多个回调可以并发到达，但实际进入节点的事件串行执行，满足第 15 步的约束。Store 执行和 fsync 也在当前事件内，仍会阻塞该节点其他事件。

发送队列各有单独的 mutex / condition_variable。调用方持有 state_mutex 时可以入队；发送线程只在取队列元素时持有队列锁，发网络请求时释放它，也不占 state_mutex。因此不会出现“持有节点状态锁等待远端，而远端正在等待本机回复”的循环。

计时使用 steady_clock，避免系统时间调整影响选举计时。运行层默认 10ms tick、100ms 心跳、600～1000ms 随机选举超时；这是本地学习服务的默认配置，没有性能调优结论。

## 超时、队列和错误

每个节点间 RPC 默认 150ms deadline；一个 peer 的等待不挡住另一个 peer。发送失败按消息丢失处理，Raft 心跳/选举计时负责之后重发，不把传输错误伪造成投票拒绝或日志冲突。

每个 peer 最多排队 16 条，已在队列里的同编号同类型重发会合并；满队列丢弃最旧项，依赖协议的重试恢复。网络单消息限制 4 MiB；日志批次继续最多 64 条、按原记录编码不超过 2 MiB，单条命令不超过 1 MiB。接收方在进入 Node 前检查字段、类型、来源成员及目标节点，明显非法消息不改变任期。

管理工具默认等待 2 秒。超时只表示调用方没等到结果，不能证明服务端没有接受提议；因此本轮不自动重试写入。gRPC deadline 的含义参见 [官方说明](https://grpc.io/docs/guides/deadlines/)。正式客户端的 Leader 发现、超时结果与重试规则到第 17 步再实现。

持久化或应用错误使运行层停止正常服务，进程报告错误退出，需要 existing 重开。关闭会停止队列和计时等待，等待当前 RPC 在 deadline 内结束；底层磁盘调用仍可能受操作系统 I/O 等待影响，没有硬实时关闭保证。

## 文件职责与 C++ 写法

| 文件 | 作用 |
| --- | --- |
| `proto/raft_node.proto` | 网络消息和服务接口 |
| `include/kvstore/rpc/node_server.h` | 运行配置、成员映射、服务生命周期 |
| `src/rpc/config.cpp` | 配置解析和回环地址校验 |
| `src/rpc/protocol_codec.h/.cpp` | 原模型与 Protobuf 的转换、输入边界校验 |
| `src/rpc/node_server.cpp` | 回调、状态锁、发送队列、计时及关闭 |
| `src/rpc/node_main.cpp` | kvnode 参数和信号处理 |
| `src/rpc/admin_main.cpp` | kvadmin 单次管理 RPC |
| `src/rpc/CMakeLists.txt` | 可选依赖查找、生成代码、网络目标和测试 |
| `scripts/build-grpc.sh` | 固定版本依赖的项目内构建 |
| `tests/grpc_codec_test.cpp` | 编码、非法输入和配置 |
| `tests/grpc_process_test.cpp` | 自动启动、验证和回收真实子进程 |

运行实现继承生成的两个 Service，用 `override` 实现 RPC 回调。C++ 这里可多重继承两个接口；与 Java 实现多个接口的用途相近。`if constexpr` 在消息转换时按 variant 的具体类型选择编译分支；`std::visit` 负责把实际消息交给对应转换逻辑。

`std::atomic<bool>` 用于线程间停止/失败标记；节点状态本身仍靠 mutex 保护。信号处理函数只设置编译期确认无内部锁的原子标记，不在信号回调中加锁、写磁盘或关闭 gRPC；主线程观察到标记后执行正常关闭。

## 验证入口

```sh
cmake --build build/network
ctest --test-dir build/network --output-on-failure
```

新增两组：8 个编码/配置测试函数，以及 6 个真实进程测试函数。进程测试自动使用独立临时目录和系统选择的空闲回环端口，只向自己启动的子进程发信号，退出时回收它们。

编码验证覆盖四类消息、二进制字节、枚举/位置/任期、批次边界和可配置成员。进程验证覆盖三份真实 Store 的 Put/Delete、Follower 拒绝提议、非法与超大 RPC、不可达节点、优雅关闭、全体重启、Leader 强杀重选、旧 Leader 重开追赶、并发管理调用，以及端口冲突和错误启动参数。

默认不启用网络的 19 组回归测试通过；普通网络构建和项目代码开启 `-Wall -Wextra -Wpedantic -fsanitize=address,undefined -fno-omit-frame-pointer` 的构建均通过完整 21 组 CTest，共 241 个测试函数。两次完整运行分别为 86.37s、93.17s，这是验证耗时，不是 benchmark。真实进程组在两种构建中均通过，包含实际管理工具全部命令；依赖构建脚本也已完整执行成功。生成代码的 Clang nullability 扩展警告只在生成目标上定向关闭，项目手写代码保留上述警告选项。

第三方依赖复用 Release 库，没有把全部 gRPC/Protobuf 依赖重新插桩，也没有新增 ThreadSanitizer 构建。本轮用并发 RPC 和关闭场景验证运行层，已有单机及协议测试全部保留。本步不包含正式客户端的一致性读、业务重试/去重、跨机器认证、系统性网络故障实验或 benchmark。

第 16 步到这里结束。建议先运行三节点和 status，再看 proto → Deliver → Collect → SendLoop 的消息路径，最后阅读状态锁、TickLoop 和关闭逻辑。讨论完成后再进入第 17 步。
