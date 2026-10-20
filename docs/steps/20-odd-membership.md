# 第 20 步：启动时配置正奇数个 Raft 成员

在原三节点版本上扩展静态成员配置。现在可以创建 1、3、5、7……个成员的新集群；默认启动示例和 benchmark 仍使用三个。每个节点仍保存完整副本，节点 ID、地址和目录由调用方指定。

本步不包含运行中增删成员、learner、分片或跨机器联网。现有目录绑定原来的成员 ID 集合，不能通过修改 --members 把旧集群直接扩成其他规模。每个节点和 Client 必须使用同一份成员配置。

## N 从哪里来

kvnode 根据 --members 的列表长度确定 N；Client 根据传入的成员列表确定 N，kvclient 将该列表作为第一个位置参数。节点和 Client 不额外提供可能与成员列表冲突的 --nodes 参数。列表必须非空且为奇数，ID 非零且唯一、地址唯一，地址继续限制在 127.0.0.1。

benchmark 运行脚本新增 --nodes N，默认 3；它负责分配 N 个端口并生成相应的 --members。实际支持的规模还受可用端口、文件描述符、线程和内存等资源限制。

| N | 多数派 | 可容忍的不可用节点数 |
| --- | --- | --- |
| 1 | 1 | 0 |
| 3 | 2 | 1 |
| 5 | 3 | 2 |
| 7 | 4 | 3 |

本项目选择只接受奇数；多数派规则为 floor(N / 2) + 1。单节点适合调试，自己就是多数派，不具备节点故障容错能力。

## 多数派提交改了什么

原版将 Leader 和两个 Follower 的复制位置组成长度为 3 的数组，排序取中间位置。现在收集 Leader 与全部 peer 的位置，排序后取 matched[N - majority_size()]，即第 majority_size() 大的复制位置。

例如五个复制位置为 [8, 8, 10, 10, 10]，第三大是 10，说明三个成员已经保存到 10。位置为 [8, 8, 8, 10, 10] 时，只能得到 8，两个成员保存到 10 还不能推进提交。仍然保留“候选条目属于当前任期”的判断。

选举的票数统计原本已按 peer 遍历。新增单节点边界：记录自投票后直接成为 Leader；Leader 的 NoOp、用户写入和读屏障持久化后立即检查提交，不等待一个不存在的 Follower 回复。之后仍由协调层保存 COMMIT、顺序执行 Store，客户端成功语义保持原样。

## 持久化格式与兼容

三节点继续读写旧 KVR1 日志头和 KVC1 提交依据，字节布局保持不变。其他成员数使用 KVR2/KVC2。日志变更帧、命令编码、Store WAL/SSTable 和 proto 都不变。

```text
KVR2 日志头：
magic(4) | self_id(8) | peer_count(4) | sorted_peer_ids(8 × peer_count) | crc32(4)

KVC2 提交依据：
magic(4) | self_id(8) | peer_count(4) | sorted_peer_ids(8 × peer_count)
         | commit_index(8) | commit_term(8) | prefix_fingerprint(4) | crc32(4)
```

peer_count = N - 1；成员 ID 排序后编码，因此相同成员以不同列表顺序重开仍可识别为相同身份。

| 成员数 | Raft 日志头 | COMMIT |
| --- | --- | --- |
| 1 | KVR2，20 字节 | KVC2，40 字节 |
| 3 | KVR1，32 字节 | KVC1，52 字节 |
| 5 | KVR2，52 字节 | KVC2，72 字节 |
| 7 | KVR2，68 字节 | KVC2，88 字节 |

读取长度由已校验的启动配置计算，不根据磁盘中未经验证的 peer_count 任意分配内存。校验长度、magic、CRC 和完整成员身份后，才能处理后面的日志帧或提交位置。

不完整的成员身份头不能按“残缺日志尾部”截断修复；只有完整身份头之后的不完整帧可以使用已有的显式尾部恢复规则。三节点旧二进制不支持新规模，旧三节点数据不需要迁移。

## 怎样运行

使用已经配置好的网络构建更新程序：

```sh
cmake --build build/network
```

单节点示例，在一个终端启动：

```sh
mkdir -p build/single
./build/network/kvnode --id 1 --directory build/single/node1 --mode new --members 1=127.0.0.1:51011
```

另一个终端使用相同成员列表：

```sh
./build/network/kvclient 1=127.0.0.1:51011 put name Alice
./build/network/kvclient 1=127.0.0.1:51011 get name
```

五节点时，每个节点终端都设置相同列表，分别把 node_id 设为 1 到 5，并执行启动命令：

```sh
cluster_members='1=127.0.0.1:51001,2=127.0.0.1:51002,3=127.0.0.1:51003,4=127.0.0.1:51004,5=127.0.0.1:51005'
node_id=1
mkdir -p build/cluster5
./build/network/kvnode --id "$node_id" --directory "build/cluster5/node$node_id" --mode new --members "$cluster_members"
```

在第六个终端调用 Client，完整成员列表保持一致：

```sh
cluster_members='1=127.0.0.1:51001,2=127.0.0.1:51002,3=127.0.0.1:51003,4=127.0.0.1:51004,5=127.0.0.1:51005'
./build/network/kvclient "$cluster_members" put name Alice
./build/network/kvclient "$cluster_members" get name
```

--mode new 仍要求节点目录不存在；重新打开同一批目录使用 --mode existing，成员 ID 集合保持原样。可调整进程监听端口，但所有调用方和节点的地址配置需要一致。

benchmark 可自动启动并关闭五个新节点，无需手动启动上述集群：

```sh
cmake --build build/release
python3 scripts/benchmark.py --build-dir build/release --nodes 5 --profile mixed --threads 4 --repetitions 1 --operations 200
```

原第 19 步的 12 轮数据是本次扩展前的三节点基线，保留原参数、程序指纹与记录，不改写为新版本或其他节点数的性能结论。

## 改动入口

| 文件 | 变化 |
| --- | --- |
| include/kvstore/raft/types.h | 共享的正奇数成员数量判断 |
| src/raft/node.cpp、replication.cpp | 单节点选主/提交，按实际多数派计算复制位置 |
| src/raft/storage_codec.cpp、storage.cpp | 旧三节点头兼容、新规模身份头与正确的日志帧起点 |
| src/replica/replica_checkpoint.cpp、replicated_store.cpp | 按成员数编码、校验和读取 COMMIT |
| src/rpc/config.cpp、node_server.cpp、client.cpp | 一致的成员数量校验 |
| tests/rpc/process_fixture.h、fault_network.h | 动态数量的子进程、端口、转发端点与链路状态 |
| scripts/benchmark.py、benchmarks/kvbench.cpp | --nodes 参数与结果中的节点数量 |
| tests/replica/odd_membership_test.cpp、tests/rpc/odd_process_test.cpp | 新规模、故障、格式与恢复验证 |

动态测试容器启动后不再扩容；Child 禁止复制，防止同一个进程 PID 被多个对象拥有。现有三节点测试继续使用默认值，原场景的三个成员含义保持原样。

## 验证结果

新增 odd_membership 和 odd_process 两个 CTest 组。原参数校验测试中，仅将已合法的单节点输入改为偶数非法输入；原有三节点功能测试仍然保留。

- odd_membership：单节点自选举、写入/删除/屏障和重开；3/5/7 精确多数派、失去 Leader 后重选、追赶及全体重开；新旧元数据、逐字节损坏/截断、成员不匹配拒绝与日志尾部修复边界。
- odd_process：真实 1/5/7 进程的读写/删除/全体重启；五节点 3/2 分区、少数派拒绝确认与旧读、恢复后冲突修正；强杀两个节点仍可提交、三个不可用时不能确认或一致读取，以及重开追赶；偶数配置在创建数据前拒绝。
- benchmark_smoke：扩展为分别运行 1/3/5 节点，核对实际进程数与结果，继续检查样本、统计和输出保护；0/2/4/6 提前拒绝。
- 旧数据验证：修改前用原 Release 二进制创建了三节点目录，新程序重开后读到全部原值，Raft/COMMIT 文件字节不变；旧三节点头和 COMMIT 的独立编码字节比较也通过。

针对性三组测试首次通过，29.66 秒。完整回归结果如下，原有用例全部通过：

| 构建 | CTest 结果 | 总耗时 |
| --- | --- | --- |
| 默认，不启用 gRPC | 20/20 组通过 | 100.32 秒 |
| Release，gRPC 与 benchmark | 27/27 组通过 | 179.31 秒 |
| ASan/UBSan，gRPC 与 benchmark | 27/27 组通过 | 194.25 秒 |

上述三套构建日志无编译警告或错误。普通网络构建也已成功，注册 25 组测试；完整网络场景包含在上面的 27 组回归中。ASan/UBSan 复用原 Release 第三方依赖，项目代码未报告地址或未定义行为错误；不新增 ThreadSanitizer 检查。测试耗时与 smoke 输出不作为性能结论。

单独运行新测试：

```sh
ctest --test-dir build/release -R '^(odd_membership|odd_process|benchmark_smoke)$' -V --output-on-failure
```

这仍是有限的本机验证，不代表所有成员数、消息交错或跨机器场景均已穷尽；运行中成员变更仍不提供。本步完成后停下来讨论。
