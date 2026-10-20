# 最终目标与验收

工程状态：已完成。原第 0—19 步和用户追加的[第 20 步奇数规模成员配置](docs/steps/20-odd-membership.md)均已实现并验证。默认示例仍为三节点，允许启动时选择正奇数个静态成员；原三节点目录格式兼容，运行中成员集合不变。最新默认构建 20/20、Release 与 ASan/UBSan 构建均 27/27 组通过。[架构回顾与验收材料](docs/final-review.md)已同步。学习状态：个人架构理解留待实际讨论确认，不把文档交付自动等同于个人掌握。

第 19 步新增独立 kvbench 和自动三节点运行脚本，完成两种负载、两种并发、各三次重复，共 12,000 次正式调用，全部 Ok；记录吞吐量、P50/P95/P99、参数、环境及原始样本，见[测量方法](docs/steps/19-benchmark.md)与[本机结果](docs/benchmarks/step19-local.md)。当时启用 benchmark 的 Release 和 ASan/UBSan 构建均 25/25 组通过。这些数字保留为第 20 步改造前的三节点基线。结果限定本机小工作集闭环负载，未做大数据集、冷热缓存或长期容量结论。客户端仍不自动重发不确定写入，没有请求去重或 exactly-once 承诺。当前约定范围已完成，其他优化或扩展按后续授权推进。

目录整理：第 16a 步已完成，源码按 engine/raft/replica/rpc/common 分组，应用入口、演示和测试单独归类。当时默认与 ASan/UBSan 构建均通过原有 19/21 组测试，既有线程检查、全新目录构建和运行命令验证通过，见[整理记录](docs/steps/16a-project-layout.md)。该记录对应第 16a 步，后续进度以顶部状态为准。

## 原始两项工程目标

1. 实现 LSM-based C++ storage engine，包含 write-ahead logging、SSTables、background compaction、Bloom filters、block caching。
2. 用 Raft 和 gRPC 实现三节点复制，验证 leader failover、crash recovery，并测量混合负载下的吞吐量与尾延迟。

最初的两条简历描述用于定义目标，当前工程完成情况和验证范围以上述状态及各步记录为准。

## 工程完成检查表

- [x] 基础目录、C++ 启动入口、CMake 配置和学习路线。
- [x] 内存中的 Put/Get/Delete 语义与验证，见 [第 1 步记录](docs/steps/01-memtable.md)。
- [x] WAL 记录格式与编解码：完整记录、数据不足、格式与校验错误，见 [第 2 步记录](docs/steps/02-wal-record.md)。
- [x] WAL 持久化写入、重启回放与文件损坏处理策略，见 [第 3 步记录](docs/steps/03-persistent-store.md)。已验证进程强杀与部分写入故障；未进行真实断电实验。
- [x] 独立 SSTable 读写、删除标记与文件校验，见 [第 4 步记录](docs/steps/04-sstable.md)。
- [x] MemTable 保留删除标记、区分三种状态并导出有序副本，验证可转换并写出 SSTable，见 [第 5a 步记录](docs/steps/05a-memtable-export.md)。
- [x] MemTable 同步刷盘、持久化文件集合、重启后文件发现及安全 WAL 回收，见 [第 5 步记录](docs/steps/05-store-flush.md)。
- [x] 多层数据读取与删除标记处理，已验证更新覆盖、删除不复活及删除后重插。
- [x] 同步全量 compaction、流式读写、删除标记规则、发布和回收中断验证，见 [第 6 步记录](docs/steps/06-compaction.md)。
- [x] 后台 compaction 及并发正确性验证，见 [第 7a 步记录](docs/steps/07a-background-compaction.md)。
- [x] 后台刷盘：单冻结 MemTable 槽、双 WAL 恢复及自动阈值下的写入背压，见 [第 7b 步记录](docs/steps/07b-background-flush.md)。阈值不是精确堆内存上限；关闭自动阈值后由调用方控制可写表大小。
- [x] Bloom filter 及无假阴性验证，见 [第 8 步记录](docs/steps/08-bloom-filter.md)。每次打开 SSTable 在内存重建；已测固定样本误判率和逻辑扫描量，没有物理 I/O 或延迟性能结论。
- [x] 内存块索引、每 Store 共享且按字节计费的 LRU block cache、淘汰/禁用/旧 reader 清理与查询正确性验证，见 [第 9 步记录](docs/steps/09-block-index-cache.md)。缓存预算不含容器元数据或外部持有的已淘汰块；索引重开重建。
- [x] 单机故障恢复集成验证，见 [第 10 步记录](docs/steps/10-engine-integration.md) 及 [单机架构说明](docs/storage-engine.md)。覆盖独立模型混合操作、四层查询、缓存开关与 38 个进程强杀场景；未进行真实断电实验。
- [x] 独立 Raft 基础模型、初始状态和手动逻辑时间，见 [第 11 步记录](docs/steps/11-raft-model.md)。
- [x] Raft 任期、投票和逻辑日志的持久化及重启恢复，见 [第 12 步记录](docs/steps/12-raft-storage.md)。覆盖同任期投票约束、后缀替换、逐字节截断、强杀和实际部分写入；选举/复制见后续项。
- [x] Raft 持久化选举、心跳、角色切换与可控消息验证，见 [第 13 步记录](docs/steps/13-raft-election.md)。本项覆盖进程内选举与消息故障测试，数据复制和联网见后续项。
- [x] Raft 日志复制、冲突修正与多数派提交，见 [第 14 步记录](docs/steps/14-raft-replication.md)。包含分批追赶、当前任期限制、NoOp、隔离、消息故障和重启验证；Store 接入见下一项。
- [x] 已提交 Raft 操作接入 Store、顺序执行与重启恢复，见 [第 15 步记录](docs/steps/15-raft-store.md)。包括提交依据、完整前缀重放、实际 KV 状态及执行边界故障验证；本地读取不承诺线性一致。
- [x] Raft 选举、日志复制、持久化、状态机应用与本机三进程 gRPC 通信，见 [第 16 步记录](docs/steps/16-grpc-processes.md)。包含编码、成员/路由校验、并发调用、独立发送与关闭验证。
- [x] 第 16a 步目录整理、总体架构文档和原有测试回归，见[目录整理记录](docs/steps/16a-project-layout.md)。公开接口、磁盘格式和可执行程序路径保持原样。
- [x] 第 17 步客户端、Leader 发现、提交确认、读屏障一致性及超时/重试语义，见[客户端说明](docs/steps/17-client-semantics.md)。没有多数派时不返回旧读，不确定写入不重放。
- [x] 本机三进程 leader failover、节点 crash recovery 及有限网络故障矩阵验收，见[第 18 步](docs/steps/18-cluster-faults.md)。覆盖七类场景与两组混合消息故障配置、无多数派时拒绝旧读、冲突修正和重启后已确认数据；不代表所有网络交错的形式化证明。
- [x] 可重复的本机三节点混合负载 benchmark：成功吞吐量、全部/成功调用及各操作的 P50/P95/P99、结果计数、逐条延迟、环境、参数与程序指纹，见[第 19 步](docs/steps/19-benchmark.md)。预装/预热不计入正式时间，每轮新集群，已保存全部 12 轮结果；范围为小工作集闭环负载。
- [x] 用户追加的正奇数规模静态成员配置，见[第 20 步](docs/steps/20-odd-membership.md)。包含单节点、通用多数派、3/5/7 协议与真实进程验证、五节点故障恢复、旧三节点格式兼容，以及 --nodes N 的 benchmark 流程；不含在线成员变更。

## 学习回顾

- [x] 交付读写路径、恢复、锁、提交规则、设计取舍及验收证据的[最终回顾](docs/final-review.md)。
- [ ] 用户能够说明读写路径、恢复过程、Raft 提交过程与主要设计取舍，由实际讨论确认。

工程项需有实现和验证证据才勾选；个人理解项需实际确认。工程目标已经完成，学习确认仍单独保留；不编造性能数字。

## 每一步的固定交付

1. 先说明本次解决的问题与范围。
2. 实现当前步骤，避免提前塞入下一阶段代码。
3. 解释改了哪些文件、为什么加、如何工作，以及相关 C++/Java 差异。
4. 运行与当前功能相称的验证，并记录结果和限制。
5. 停下来讨论；用户明确 OK 后才开始下一步。
