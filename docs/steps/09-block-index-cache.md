# 第 9 步：块索引与共享 LRU block cache

本轮完成两件事：用索引定位候选块，让 Get 不必从文件开头查；用容量受限的缓存复用已经读取的块。先分清三者：

```text
Bloom filter：这个文件是否值得查？
块索引：值得查时，应该读哪一块？
block cache：这块的字节是否已经在内存里？
```

## 当前 Get 流程

```text
可写 MemTable → 冻结 MemTable → 从新到旧访问 SSTable
                                  ↓
                           Bloom：一定没有就跳过
                                  ↓ 可能存在
                           二分索引：定位候选块
                                  ↓
                           查 Store 的共享 block cache
                            ├─ 命中：取得内存字节
                            └─ 未命中：定位文件偏移，读整块、校验，再尝试缓存
                                  ↓
                           解析块内记录，精确比较 key
```

缓存命中不直接代表 key 存在，仍要精确比较。删除标记也保留在块里，Store 读取到最新删除标记后立即返回不存在。

## 本轮的块是什么

磁盘格式仍是 `KVS1 | count:u32 | 有序记录`，没有新增块头、footer 或独立索引文件。块是这串记录中一段连续的完整记录，目标约 4 KiB：

- 加入下一条记录将超过 4096 字节时，结束当前块。
- 记录不拆开，单条超过 4 KiB 时独占一块。
- 原有 key + value 最大 1 MiB 的限制不变。
- 空 SSTable 没有数据块，仍只有 8 字节文件头。

打开 reader 时，沿用原本的完整校验循环，顺便记录块边界。因此已有文件直接可用，也不影响刷盘/合并的写入格式和发布协议。

索引与 Bloom 一样存内存、重开时重建，打开仍然扫描完整文件。缓存也不持久化，重启从冷缓存开始；本轮没有优化打开成本，CLI 每条命令重新启动也会重新打开。

## 索引保存什么，如何定位

每块保留一个 `BlockIndexEntry`：

| 字段 | 作用 |
| --- | --- |
| last_key | 该块最后一个 key，用于二分查找 |
| offset | 块在文件中的字节偏移 |
| bytes | 整块编码字节数 |
| first_entry | 该块第一条记录在整张表中的序号，用于校验条目数和保持游标语义 |

示意：

| 块 | 内容范围 | last_key | offset | bytes |
| --- | --- | --- | --- | --- |
| 0 | a…g | g | 8 | 3900 |
| 1 | h…q | q | 3908 | 4050 |
| 2 | r…z | z | 7958 | 2100 |

查询 p，用 `lower_bound` 找到第一个 `last_key >= p` 的块，也就是块 1。前面的块最大 key 都小于 p，可直接跳过；后面的块所有 key 都大于块 1 的末尾 key，也不可能包含 p。

这里“更大”指同一张表里的 key 排序，不是 SSTable 的新旧版本。一次点查询在一张表中最多选择一个候选块。超过最后一个 key 可以直接返回不存在；块内缺失则精确扫描到第一个更大的 key 后返回。

索引默认预算按 `sizeof(BlockIndexEntry) + last_key.size()` 累计，最多 16 MiB。超过时清空已经构建的部分索引，整张表退回原来的精确顺序查询，Bloom 仍可用；不会带着不完整索引服务查询。该预算不包含 vector 多余容量等分配器开销，不是严格堆内存上限。

## 缓存有几个，存什么

**每个 Store 一个共享缓存，所有有效 SSTable reader 共用默认 8 MiB 的预算。** 独立 `SSTableReader(path)` 默认建立索引，但没有私有缓存；需要缓存时显式传入共享对象。

缓存保存不可变的原始块字节，包含 SSTable 中的完整编码记录，不保存查询结果，也不只缓存单个 key/value。不同 key 落在同一块时可以复用同一份字节。命中后仍解析和检查查询需要经过的记录，未实现“解码后的记录缓存”。

冷读会先验证整块的 CRC、顺序、条目数和末尾 key，与打开时构建的索引一致后才缓存。这样即使查询目标位于块开头，也不会把块尾部损坏的字节放入缓存。读取或校验失败不会留下缓存项。

打开验证、Rewind/Next 顺序扫描、compaction 输入不会预热或访问缓存。compaction 输入还关闭无用的 Bloom 和索引，保持流式扫描，避免挤走热点块。新输出的前台 reader 会正常建立 Bloom、索引并接入同一共享缓存。

相邻 key 组成块、LRU 缓存以及顺序扫描绕过缓存的思路可参考 [LevelDB 官方文档](https://github.com/google/leveldb/blob/main/doc/index.md)。本轮实现沿用本项目 KVS1 编码和内存重建方式，不使用其磁盘格式或依赖库。

## LRU 怎么工作

缓存组合使用：

```text
unordered_map：按缓存键快速找到链表节点
list：维护最近使用顺序，最常用在头，最久没用在尾
```

Lookup 命中时，用 `list::splice` 把节点移到头部，不复制块字节。插入新块后，空间不够就不断从尾部淘汰，直到预算能容纳它。替换已有节点也会更新字节计费并提升为最近使用。

假设容量仅能放两块：

```text
读取 A、B：B → A
再次读取 A：A → B
读取 C：   C → A，淘汰 B
再次读取 B：需要重新从文件读取
```

单块超过整个缓存容量就不接纳，但这次查询仍使用刚读出的字节正常返回。容量设为 0 时，索引继续生效，块每次从文件读取，不保留缓存项。空块不允许接纳，防止大量零计费节点。

`resident_bytes` 统计缓存当前保留的字符串长度。所有公开操作完成时都不超过容量；它不包含链表/hash 元数据、string 额外分配空间、索引、Bloom、工作缓冲，或调用方暂时持有的已淘汰块。原有调用方取得的 shared_ptr 会保活旧字节，淘汰只释放缓存自己的持有。

## 为什么不能只用偏移或路径作为缓存键

不同 SSTable 都可能从偏移 8 开始，路径也可能在关闭后被重用。因此缓存键是：

```text
缓存内独立分配的 reader 编号 + 块偏移
```

这个编号与 manifest 文件编号不同。同一缓存生命周期里不重复，每次打开 reader 都重新分配。两个 reader 即使打开同一路径也不会混用；同一 Store 的多个 SSTable 仍共用同一容量预算。

reader 关闭时调用 `EraseTable()`，清理该编号的所有缓存项。compaction 先发布、切换 reader，关闭旧 reader 时回收旧缓存，最后删除旧文件。新表使用新编号，不会读到旧表的块；重新打开 Store 则创建全新缓存。

Put/Delete 不需要修改旧表缓存：旧 SSTable 本来就不可变，查询的新旧顺序仍优先可写表、冻结表及更新的 SSTable。新删除标记遮住旧值，而不是原地改旧块。

## 锁和 Next 游标

Store 的状态锁仍串行保护 Get、WAL/MemTable 和 reader 集合。BlockCache 自己还有一把锁，保护共享 map、LRU 顺序和统计，独立缓存也支持多线程访问。

常见访问顺序是 Store 状态锁 → 缓存锁；缓存不会回调 Store，避免形成反向锁依赖。构建新 reader 时只短暂申请缓存编号，不会预读块进缓存。

原有 Get/Next 游标语义继续保留：Get 命中或遇到更大 key 后，Next 从它后面继续；Bloom 负判断不改变游标。缓存命中虽不移动文件流，仍更新逻辑记录序号和 next_offset，Next 必要时再 Seek。这也是 `cursor_positioned_`、`next_offset_` 的作用。

## 配置和统计

Store 第五个构造参数控制共享缓存字节容量：

```cpp
kvstore::Store store(path, kvstore::WalRecoveryMode::Strict,
                     4 * 1024 * 1024, 4, 8 * 1024 * 1024);
auto stats = store.block_cache_stats();
```

最后一个参数设为 0 即关闭接纳。现有构造调用仍使用默认值，CLI 命令不变，本轮没有增加缓存命令行参数。

独立 reader 可以显式配置，或关闭索引用作顺序基准：

```cpp
auto cache = std::make_shared<kvstore::BlockCache>(8192);
kvstore::SSTableReadOptions options;
options.block_cache = cache;
kvstore::SSTableReader reader(path, options);
```

设置 `options.use_block_index = false` 会使用顺序查询；`index_budget_bytes` 可设置索引计费预算。第 8 步 Bloom 对照测试显式关闭索引，继续单独验证 Bloom 的原有扫描量样本。

| 统计 | 含义 |
| --- | --- |
| index_lookups | 索引查找次数，包含不需要读块的范围拒绝 |
| table_scans | 需要精确查看数据的次数；有索引时是一块，关闭时是原顺序扫描 |
| entries_examined | 查询比较经过的记录数 |
| block_reads / block_bytes_read | 点查询发起的块读取次数、完整读出的块字节数 |
| block_entries_validated | 冷读时校验整块的记录数，与查询比较分开计数 |
| cache_hits / cache_misses | reader 对共享缓存的命中/未命中次数 |
| Store cache stats | 全局命中、未命中、淘汰、跳过接纳、容量、驻留字节和项数 |

打开时的完整校验、Next 全扫描不计入这些 Get 统计。文件流和操作系统也可能缓存字节，所以 block_reads 不是物理磁盘读取次数，不能据此直接声称吞吐量或尾延迟提升。

固定测试样本：256 条记录、每条 value 64 字节，查询最后一条。顺序查询比较 256 条；索引定位后比较 21 条，并读取一个候选块。该冷读还额外校验块内 21 条记录。另一个两记录一块的样本，先查第一条再查第二条，仅加载一次块，第二次命中缓存。

## 文件变化与 C++ 知识

| 文件 | 修改目的 |
| --- | --- |
| `include/kvstore/block_cache.h`、`src/block_cache.cpp`，新增 | 共享 LRU、字节预算、编号、淘汰、关闭清理和统计 |
| `include/kvstore/sstable.h`、`src/sstable.cpp` | 逻辑块索引、二分查找、完整块读取校验、缓存查询和逻辑游标 |
| `include/kvstore/store.h`、`src/store.cpp` | 创建 Store 级缓存、第五个参数、统计快照、统一 OpenSSTable 工厂 |
| `src/background_flush.cpp`、`src/background_compaction.cpp` | 新 reader 通过 OpenSSTable 接入同一缓存 |
| `src/compaction.cpp` | 顺序输入关闭索引及 Bloom，绕过缓存 |
| `tests/block_cache_test.cpp`、`tests/sstable_index_test.cpp`，新增 | 独立缓存和查询/发布接入测试 |
| `tests/sstable_bloom_test.cpp` | 对照时隔离块索引的影响 |
| CMake、README、GOAL、ROADMAP、本记录 | 构建和说明 |

`std::list` 是双向链表，节点移动后迭代器仍有效；map 保存它的迭代器，从而命中后不必遍历链表寻找节点。`splice` 把已有节点重新接到表头，类似维护 Java access-order LinkedHashMap 的访问顺序。

`shared_ptr<const string>` 让缓存与当前读取者共享不可修改的块字节。淘汰或关闭 reader 不会让已经取出的对象悬空；Get 返回的 SSTableEntry 仍拥有自己的 key/value 字符串。

`lower_bound` 返回首个满足边界条件的位置；比较器 `block.last_key < searched` 表示该块完全在目标 key 之前，可以跳过。索引构建 lambda 的 `[&]` 使它更新当前构造过程的计数和状态，没有启动新的线程。

没有新增磁盘格式、第三方依赖或 VS Code 配置。索引持久化、压缩、并行前台查询和异步预取都未实现。

## 运行与验证

项目根目录执行：

```sh
cmake --build build --target block_cache_test sstable_index_test
./build/block_cache_test
./build/sstable_index_test
```

新目标尚未配置时，先执行 README 中的配置命令，或先构建全部目标让 CMake 自动重新生成规则。

8 个缓存测试覆盖命中/未命中、LRU 提升、一次淘汰多项、替换计费、调用者保活、禁用/超大块/空块、编号隔离/关闭清理、并发访问以及与线性参考模型的 300 次混合操作对照。

12 个接入测试覆盖索引与顺序结果一致、边界/gap/cursor、同块复用/Bloom 跳过、跨块 LRU、容量 0/超大单记录、二进制/空值/删除标记、路径重用、读到损坏块不接纳、索引预算降级、Store 新表发布/旧缓存回收、缓存开关下并发读写与恢复、扫描和合并不污染缓存。

原有格式 fixture、强杀、真实部分写入、双 WAL、恢复与并发测试继续通过。普通与 AddressSanitizer/UndefinedBehaviorSanitizer 构建十三组 CTest 全通过，共 151 个测试函数；ThreadSanitizer 检查五组并发相关测试，共 48 个函数，通过且无数据竞争报告。没有真实断电实验或吞吐量/延迟测量。

第 10 步单机集成与故障验证尚未开始。本轮先讨论块边界、二分定位、缓存共享和容量计费。
