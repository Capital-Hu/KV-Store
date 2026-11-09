# 第 21 步：Raft 快照与日志截断

此前日志没有截断手段，写满 64 MiB 或 100 万条后集群停止服务，重启后新 Leader 追加 NoOp 时还会失败。本步让日志长度保持有界，重启只回放基线之后的条目，并让落后太多的节点通过快照追上。

## 三个位置

| 概念 | 含义 |
| --- | --- |
| 基线 base | Raft 日志保留的起点，base 及之前的条目已删除 |
| 快照点 S | 最新状态机快照对应的日志位置，S ≥ base |
| 保留尾部 | 截断时在 S 之前留下的条目，让轻微落后的节点不必传快照 |

`PersistentState.log` 只保存 base 之后的条目，下标换算集中在 `EntryAt`、`TermAt` 和 `last_index`。base 为 0 时与旧目录完全兼容。

## 什么时候生成

每次应用已提交条目后检查：距基线的已应用条目达到 `snapshot_threshold_entries`（默认 10 万），或日志字节达到 `snapshot_threshold_bytes`（默认 32 MiB）。读屏障也计入，所以只读负载同样会截断。阈值设为 0 可以关闭，`kvnode` 通过 `--snapshot-entries`、`--snapshot-retain` 配置。

步骤严格按序：

1. `Store::CreateCheckpoint` 刷盘，并把存活 SSTable 硬链接到 `snapshot-tmp/`。SSTable 不可变，硬链接不复制数据，后续 compaction 删除原文件也不影响快照。
2. 写入 META，原子重命名为 `snapshot.<S>/`，同步目录。
3. 改写 Raft 日志文件：写临时文件，内容为任期投票、Base 记录和保留条目，再 rename 发布。

快照先于截断持久化，所以任何时刻都有快照覆盖已删除的条目。

## 为什么重启可以不重置 Store

Store 的每次写入都先同步 WAL，所以持久状态至少覆盖到 last_applied ≥ base。从 base+1 回放到 commit 时，区间内写过的 key 会得到最终值，没写过的 key 在回放前后本就相同。因此 Put/Delete 回放到 Store 当前状态上结果正确，不需要记录 applied_index。

COMMIT 文件的指纹是从第 1 条开始的链式校验。截断后无法重算旧条目，所以 Base 记录保存基线处的指纹，启动时从它继续校验。

## 落后节点如何追上

文件和协议分开，避免大文件经过 Raft 状态锁：

1. Leader 发现 `next_index ≤ base`，发送只含元数据的 `InstallSnapshotRequest`。它也重置 Follower 的选举计时器，并在心跳时重发。
2. 每个 peer 有独立的传输线程，通过 `PushSnapshot` 流式发送 SSTable，不阻塞心跳。
3. Follower 收到后校验每张表的记录校验和，发布为 `snapshot-staged/`。文件未到齐时回复失败，Leader 下次心跳再问。
4. 文件到齐后 Follower 安装，回复成功，Leader 把 `match_index` 推进到 S。

协议边界：

- S ≤ commit：已经具备，直接回复成功。
- 本地日志在 S 处任期相同：只推进 commit，保留后续条目。这些条目可能已被计入多数派，不能丢弃。
- 其余情况才丢弃整个日志并替换状态机。

## 安装的崩溃恢复

安装要改三处：Store、Raft 日志和 COMMIT，无法一次原子完成，所以用目录重命名作为提交点：

```text
snapshot-staged → snapshot-install   提交点
Store::ResetToSnapshot               manifest rename 原子切换
Raft 日志重置到 S
写 COMMIT
snapshot-install → snapshot.<S>      结束
```

启动时若存在 `snapshot-install`，就把上述步骤重做一遍。每一步都可重复执行。`snapshot_test` 在四个阶段注入崩溃，验证重开后完成安装并能继续复制。

## 仍然存在的限制

- 生成快照时的刷盘在节点状态锁内进行，可能造成短暂停顿。
- 安装时关闭和重开 Store 也在状态锁内。如果耗时超过选举超时，可能触发一次不必要的选举，但不影响安全性。
- 传输不支持断点续传，失败后整体重传。`--raft-routes` 测试代理未转发 `PushSnapshot`，故障矩阵默认阈值下不会触发快照。
- 日志未提交部分仍受 64 MiB / 100 万条约束，长时间无多数派时写入会被拒绝。

## 验证

`snapshot`（截断与重启、超过原 64 MiB 上限的写入、协议边界、落后节点安装、四阶段崩溃恢复）和 `snapshot_process`（真实三进程：节点强杀后靠快照追上、换主、全集群重启）。启用 gRPC 的 Release 构建 27 组全部通过；ASan/UBSan 构建运行了 Raft、复制、快照、gRPC 进程和故障相关的 12 组，全部通过。未运行 TSan。
