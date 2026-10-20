# 第 6 步：同步 compaction

> 路径说明：本文记录本步骤完成时的文件位置。后续目录已按模块整理，当前路径见[第 16a 步](16a-project-layout.md)；构建与运行命令以[项目 README](../../README.zh-CN.md)为准。

## 本步做了什么

新增 `Store::Compact()` 和 CLI `compact`，把当前全部 SSTable 合并成一个新 SSTable，保留每个 key 的最新磁盘状态，然后同步发布清单、回收旧文件。本步手动触发，在调用线程中完成；不引入后台线程或分层调度。

```text
旧 SSTables → 按 key 合并、同 key 取较新的输入 → 新 SSTable
    保留到新清单同步完成                         ↓
                             同步文件和目录 → 发布清单 → 切换 reader → 回收旧文件
```

Compaction 只整理已经刷盘的数据，不调用 Flush，不清空 MemTable，不轮换或改写当前 WAL。尚未刷盘的值或删除标记仍优先于合并结果；重启仍由当前 WAL 恢复这些更新。

## 合并规则

输入顺序来自 manifest，按旧到新排列，不根据文件名大小重新排序：

```text
旧文件 1             新文件 3              合并结果
age  → 20            age  → 删除标记        name → Bob
name → Alice         name → Bob
```

同一个 key 只输出最新版本。如果较新的记录是删除标记，不能因为略过它又输出更旧的值。

删除标记有两种处理范围：

| 场景 | 处理 |
| --- | --- |
| 只合并部分 SSTable，外面可能还有更旧的值 | 必须保留删除标记，继续遮住外部旧值 |
| 本版 Store::Compact，全部有效 SSTable 都参与，而且没有 MVCC/历史快照读 | 可以一起清理标记及被它遮住的旧版本 |

独立 `MergeSSTables` 默认保留标记；Store 只在明确合并全部 SSTable 时传入清理选项。将来如果改成部分合并或支持历史版本读，不能直接沿用这个清理条件。

即使 MemTable 有一个更新的 Put 或 Delete，也不会影响这个安全性：MemTable/WAL 保留不动，当前查询仍先看内存，重启后也会恢复这些较新的状态。

## 如何逐条合并

`MergeSSTables` 不把输入文件全部读进 map，也不收集完整输出 vector。每个输入 reader 只保存当前条目，优先队列保存这些条目对应的输入编号：

```text
文件 A 当前 key：b ─┐
文件 B 当前 key：a ─┼→ 选最小 key → 输出或略过 → 推进对应 reader
文件 C 当前 key：c ─┘
```

比较器先比较 key，小 key 优先；key 相同时，输入列表中更靠后的文件优先。这样相同 key 的最新记录一定先处理，后续重复 key 可以略过。

代码记住上次处理的 key，包括已经丢弃的删除标记。否则在清理新标记后，会错误地把下一个旧值重新输出。

每个输入使用独立 reader，避免改变 Store 查询 reader 的位置。合并期间临时占用每个输入一个额外文件描述符；当前是全部文件合并，文件数量很大时仍会受描述符和资源限制。

若有 K 个输入、N 条输入记录，队列操作约为 O(N log K)；当前打开文件还会完整校验输入，整体仍需读取全部输入。内存主要是 K 个当前条目、队列和少量编码缓冲，最坏字节数随 K 和最大条目大小变化，不随全部输出数据量增长。这是算法结构说明，没有做独立内存峰值 benchmark。

## SSTable 新增的两个能力

### 顺序读取

```cpp
reader.Rewind();
while (auto entry = reader.Next()) {
    // Process one owned entry at a time.
}
```

Rewind 把游标移到第一个条目，Next 返回下一条或 nullopt。Get 也使用同一个游标并会重新开始扫描，所以交错调用 Get/Next 会改变遍历位置；独立遍历要使用独立 reader。当前仍按单线程使用。

### 逐条写入

```cpp
kvstore::SSTableWriter writer(path);
writer.Append(entry);
writer.Finish();
```

Append 要求 key 严格递增且无重复，每次只编码并写一条。合并开始时不知道最终记录数，因此先写计数为 0 的文件头；Finish 回到偏移 4，补写最终的 4 字节计数，然后关闭文件。完成后的磁盘格式与旧版完全相同，已有字节样本测试继续通过。

计数回填只发生在尚未发布的新文件构建阶段，不是修改已经投入使用的 SSTable。Finish 不做同步和清单发布，这些仍由 Store 协调。

构建中途失败可能留下不完整文件。没有调用 Finish 的非空输出会因计数与内容不匹配而被拒绝；零条目或部分计数写入也可能碰巧留下格式合法的文件，但它仍未被发布，不能据此当成成功结果。析构只关闭资源，不替调用者 Finish。

原来的 `WriteSSTable(path, vector)` 接口保留：先验证整个 vector，再逐条调用 writer。因此乱序/非法输入仍在创建文件前被拒绝；仅 streaming writer 的逐条校验发生在已经创建文件之后。

## Store 的发布流程

假设原清单是：

```text
SSTable [1, 3]，WAL 4，next_file_id 5
```

合并只需要分配一个新 SSTable，不需要新 WAL，最终清单为：

```text
SSTable [5]，WAL 4，next_file_id 6
```

所以以后编号不再固定是“SSTable 奇数、WAL 偶数”；它们仍共用计数器，只是 Flush 分配两个编号，Compact 分配一个。

编号只标识文件：合并结果有更新的文件编号，也不表示其数据比当前 WAL 的待刷盘变更更新；查询仍先看 MemTable。

顺序是：

1. 独立打开并校验全部输入，逐条合并，完成新文件。
2. 同步新 SSTable 和父目录，打开并校验新 reader。
3. 复用 PublishManifest：写并同步临时清单、rename 替换正式清单、同步目录。
4. 切换 Store 的清单和 reader，关闭旧 reader。
5. 逐个删除旧 SSTable，最后同步目录。

新清单发布前不删除任何旧输入。发布后清理到一半退出也没关系，启动时加载清单中的新结果，再清理未被引用的旧文件。任何写出、同步、发布或清理阶段错误都会让当前 Store 停止读写，关闭重开后按有效清单恢复。

没有 SSTable 时 Compact 不做任何事。一个 SSTable 也会被重新合并，以清理已有删除标记；重复执行会再次重写并分配新编号。若所有磁盘 key 都已删除，结果仍是一个合法的 8 字节空 SSTable，而不是删除整个 Store。

## 改了哪些文件

| 文件 | 作用 |
| --- | --- |
| `compaction.h` / `compaction.cpp` | 独立的有序多路合并，输入优先级与删除标记规则 |
| `sstable.h` / `sstable.cpp` | 新增 Rewind/Next、streaming writer；Get 和原 writer 复用这些能力 |
| `store.h` / `store.cpp` | 新增 Compact，复用清单发布，切换 reader 并回收旧 SSTable |
| `main.cpp` | 新增 compact 命令 |
| `CMakeLists.txt` | 引擎加入合并实现，登记 compaction_test |
| `sstable_test.cpp` / `compaction_test.cpp` | 4 个游标/写入测试、15 个合并与恢复测试 |

MemTable、WAL 格式、manifest 格式、同步与文件锁辅助函数、VS Code 配置没有改动。

建议先看 `Store::Compact`，理解已有的发布流程如何复用；再看 `MergeSSTables` 的循环和比较器；最后看 SSTable 的 Next 和 Finish。

## C++ 知识点

- `priority_queue` 的比较器是一个实现 `operator()` 的结构体对象。默认队列取最大值，这里反向比较 key，实现最小 key 优先；同 key 用输入编号决定新旧。
- 比较器借用 heads 容器，容器生命周期覆盖整个队列。某个编号必须先从队列弹出，才能改变它对应的条目，避免悄悄破坏队列顺序。
- `std::move(*heads[source])` 允许把当前条目的字符串资源交给局部变量，随后只读取该输入的下一条。它不是移动磁盘文件。
- `sstables_.swap(replacement)` 交换两个 reader 列表，不复制文件或条目；replacement 此时拥有旧 reader，clear 后关闭它们，再删除旧文件。
- `while (auto entry = reader.Next())` 每轮获取一个 optional，只在它包含条目时进入循环。entry 的字符串是独立拥有的。

## 运行

在项目目录先制造两个 SSTable，再合并：

```sh
cmake --build build --target kvstore
./build/kvstore build/step6-demo.wal put name Alice
./build/kvstore build/step6-demo.wal flush
./build/kvstore build/step6-demo.wal put name Bob
./build/kvstore build/step6-demo.wal flush
./build/kvstore build/step6-demo.wal compact
./build/kvstore build/step6-demo.wal get name
```

compact 输出 `OK (1 SSTables)`，Get 输出 `Bob`。示例文件留在 build 下；通过 `ls build/step6-demo.wal*` 可以观察旧 SSTable 已回收，当前 WAL 未被 compaction 轮换。

```sh
cmake --build build --target sstable_test compaction_test
./build/sstable_test
./build/compaction_test
```

分别输出 `All 17 SSTable tests passed.`、`All 15 compaction tests passed.`。

## 验证结果与边界

新增测试验证新旧值优先级、部分合并保留必要标记、全量合并清理删除历史、删除后重插、空 key/value、二进制及最大记录、坏输入与已有输出保护、pending MemTable/WAL 不变、单文件及全删除结果、反复合并后继续 Flush、稳定锁，以及固定种子 96 次混合操作对照模型。

故障验证在输出完成、文件同步、临时清单同步、rename、目录同步、reader 切换、删除第一个及最后一个旧文件等 8 个边界分别进行。带/不带 pending WAL 各强杀一次，共 16 次 SIGKILL；另在 8 个边界注入异常。子进程文件大小限制制造真实的部分输出头、部分条目和部分临时清单写入，验证旧输入与正式清单保留、重开清理和重试。

本机普通构建和带 `-Wall -Wextra -Wpedantic`、AddressSanitizer、UndefinedBehaviorSanitizer 的构建均通过七组 CTest，共 90 个测试函数。另用独立 CLI 进程执行 90 次混合操作，穿插 Flush/Compact 和查询，对照模型并确认当前 WAL 字节未被 Compact 改写、旧 SSTable 回收正确。

没有真实断电或穷举系统调用故障验证。本步没有层级选择、自动 compaction、后台线程、索引/Bloom/cache 或历史快照。当前 Get 仍顺序扫描候选文件；合并占用调用线程，而且完整输入校验和额外 reader 会消耗时间与描述符。

第 6 步完成后停下来讨论，明确 OK 后才开始第 7 步后台刷盘和 compaction，并重新设计并发期间的文件与内存表生命周期。
