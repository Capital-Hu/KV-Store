# 最终目标与验收

状态：进行中。第 14 步 Raft 日志复制、冲突修正与提交已完成：支持持久化提议、按 peer 分批追赶与重试、当前任期多数派提交、NoOp 及重开后重新确认提交；演示展示隔离旧 Leader 的提议不能独自提交、重连后修正未提交后缀。十八组 CTest、212 个测试函数和内存检查通过，新增两个日志同步后输出前的进程强杀场景。等待本步讨论及明确 OK 后，再开始第 15 步 Store 接入。整体目标尚未完成，状态机执行、gRPC、真实网络故障与性能 benchmark 尚未接入或验证。

## 必须完成的两部分

1. 实现 LSM-based C++ storage engine，包含 write-ahead logging、SSTables、background compaction、Bloom filters、block caching。
2. 用 Raft 和 gRPC 实现三节点复制，验证 leader failover、crash recovery，并测量混合负载下的吞吐量与尾延迟。

简历里的过去式描述是目标文案，不是当前完成状态。本文件和验证记录决定项目实际完成程度。

## 完成检查表

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
- [x] 单机故障恢复集成验证，见 [第 10 步记录](docs/steps/10-engine-integration.md) 及 [单机架构说明](docs/architecture.md)。覆盖独立模型混合操作、四层查询、缓存开关与 38 个进程强杀场景；未进行真实断电实验。
- [x] 独立 Raft 基础模型、初始状态和手动逻辑时间，见 [第 11 步记录](docs/steps/11-raft-model.md)。
- [x] Raft 任期、投票和逻辑日志的持久化及重启恢复，见 [第 12 步记录](docs/steps/12-raft-storage.md)。覆盖同任期投票约束、后缀替换、逐字节截断、强杀和实际部分写入；不代表选举/复制协议已经完成。
- [x] Raft 持久化选举、心跳、角色切换与可控消息验证，见 [第 13 步记录](docs/steps/13-raft-election.md)。包含进程内演示和消息故障测试，不代表三进程服务或数据复制已完成。
- [x] Raft 日志复制、冲突修正与多数派提交，见 [第 14 步记录](docs/steps/14-raft-replication.md)。包含分批追赶、当前任期限制、NoOp、隔离、消息故障和重启验证；尚未执行到 Store。
- [ ] Raft 选举、日志复制、持久化、状态机应用与 gRPC 通信。
- [ ] 三节点 leader failover 和节点 crash recovery 验证。
- [ ] 可重复的混合负载 benchmark：吞吐量、P50/P95/P99 延迟、运行环境和参数。
- [ ] 用户能够说明读写路径、恢复过程、Raft 提交过程与主要设计取舍。

每项需有实现和验证证据才勾选。所有必要项完成后才将整体目标标记完成；不编造性能数字。

## 每一步的固定交付

1. 先说明本次解决的问题与范围。
2. 实现当前步骤，避免提前塞入下一阶段代码。
3. 解释改了哪些文件、为什么加、如何工作，以及相关 C++/Java 差异。
4. 运行与当前功能相称的验证，并记录结果和限制。
5. 停下来讨论；用户明确 OK 后才开始下一步。
