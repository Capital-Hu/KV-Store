# 最终目标与验收

状态：进行中。用户已确认推进；第 3 步 WAL 文件写入与启动回放已实现并验证。等待讨论和明确 OK 后，再开始第 4 步 SSTable 格式与读写。

## 必须完成的两部分

1. 实现 LSM-based C++ storage engine，包含 write-ahead logging、SSTables、background compaction、Bloom filters、block caching。
2. 用 Raft 和 gRPC 实现三节点复制，验证 leader failover、crash recovery，并测量混合负载下的吞吐量与尾延迟。

简历里的过去式描述是目标文案，不是当前完成状态。本文件和验证记录决定项目实际完成程度。

## 完成检查表

- [x] 基础目录、C++ 启动入口、CMake 配置和学习路线。
- [x] 内存中的 Put/Get/Delete 语义与验证，见 [第 1 步记录](docs/steps/01-memtable.md)。
- [x] WAL 记录格式与编解码：完整记录、数据不足、格式与校验错误，见 [第 2 步记录](docs/steps/02-wal-record.md)。
- [x] WAL 持久化写入、重启回放与文件损坏处理策略，见 [第 3 步记录](docs/steps/03-persistent-store.md)。已验证进程强杀与部分写入故障；未进行真实断电实验。
- [ ] SSTable 读写、刷盘及重启后文件发现。
- [ ] 多层数据读取与删除标记处理。
- [ ] 后台 compaction 及并发正确性验证。
- [ ] Bloom filter 及无假阴性的验证。
- [ ] 有容量限制的 block cache 及淘汰行为验证。
- [ ] 单机故障恢复集成验证。
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
