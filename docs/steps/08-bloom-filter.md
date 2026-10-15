# 第 8 步：SSTable 的 Bloom filter

本轮让 Get 在扫描一个 SSTable 前，先判断“这个文件一定没有该 key”。已有的刷盘、WAL、清单发布与合并协议继续使用。

先记住两种返回值：

```text
MayContain(key) == false → 一定没有，跳过这个 SSTable
MayContain(key) == true  → 可能有，仍需精确查询
```

Bloom filter 不保存 value，不代替查询，也不决定 key 的最新版本。

## 放在现有流程的哪里

```text
Get
 ├─ 查可写 MemTable
 ├─ 查冻结 MemTable
 └─ 从新到旧访问 SSTable
       ├─ Bloom 判定一定没有 → 继续下一张表
       └─ Bloom 判定可能有   → 扫描本表，查到值或删除标记即停止
```

Store 的查询循环不需要改动。判断放在 `SSTableReader::Get()` 中，它向上仍返回原有的三种状态：没有记录、普通值、删除标记。

## 本轮选择：打开文件时在内存中构建

目前 SSTableReader 打开文件时，本来就会完整检查所有记录的 CRC、顺序、条目数和尾部。因此在这个循环里，把每条记录的 key 加入 Bloom filter，无需为构建再额外扫描一遍文件。

```cpp
entry_count_ = ReadU32(header, 4);
if (use_bloom_filter) { bloom_.emplace(entry_count_); }

// Inside the existing validation loop:
if (bloom_) { bloom_->Add(entry.key); }
```

添加 key 发生在将 `entry.key` move 给其他变量之前，避免对已经被移走的字符串做过滤。

过滤器只存内存，不改变 KVS1 文件，也没有新增过滤器文件。已有 SSTable 直接打开即可使用；新刷盘和 compaction 输出在打开 reader 时同样自动构建。

代价是：每次重新打开都要重建。打开时仍完整扫描并校验文件，过滤器只减少后续点查询的扫描。当前 CLI 每条命令是一个新进程，会付出重新打开的成本；收益主要由长时间打开的 Store 和测试中的重复查询体现。没有测量总体延迟提升。

过滤器持久化尚未实现。第 9 步设计块索引与文件元数据时，可以再讨论是否持久化过滤器；本轮先把位数组、查询接入与正确性讲清楚。

## 位数组如何工作

BloomFilter 内部使用 `vector<uint8_t>`，每个字节包含 8 个 bit，开始都为 0。

加入 key 时，计算七个位置，把这些位置的 bit 设成 1。例如某个 key 对应位置 `3、7、15……`，就把这些位置标记起来。

查询同一个 key 时，使用相同算法计算相同的七个位置：

- 只要有一个位置为 0，它一定没有被加入过。
- 全部为 1，可能是这个 key 设置的，也可能由其他 key 的位置碰巧凑齐，所以只能回答“可能存在”。

不同 key 共用 bit，就产生假阳性。这里的“误判”只会增加一次精确查询，不会直接返回错误的值。

Add 只把 bit 从 0 改为 1，不清除；查询又使用完全相同的位置计算。因此所有已经加入的 key，其检查位一直为 1，不会因为加入其他 key 而变成“一定没有”。这就是无假阴性的依据，前提是完整加入所有记录的 key，且冻结后的过滤器不被修改或损坏。

不提供单独删除一个 key 的操作：清除共享 bit 可能影响其他 key。SSTable 本身不可变，删除表现为新表中的删除标记；合并生成新的文件后，按实际输出的 key 重建过滤器。

## 参数、哈希与资源限制

| 参数 | 本轮实现 |
| --- | --- |
| 每个预估 key | 10 bit，向上取整到完整字节 |
| 最小过滤器 | 64 bit，即 8 字节 |
| 每次 Add/MayContain | 检查 7 个位置 |
| 单 reader 位数组上限 | 8 MiB，约覆盖 671 万条记录的预估 |
| 超过上限 | 关闭该过滤器，MayContain 始终返回 true，回到精确扫描 |

10 bit/key 是一个常见空间配置；[LevelDB 官方文档](https://github.com/google/leveldb/blob/main/doc/index.md) 也用它举例说明内存与过滤效果的取舍。本项目的哈希实现、内存构建时机和测量结果以本项目代码及测试为准。

`Hashes()` 按 key 的所有原始字节计算两个 64 位数，再用以下形式生成七个探测位置：

```cpp
auto [position, step] = Hashes(key);
const auto bit = position % bit_count;
// Repeat for seven probes:
position += step;
```

Add 和 MayContain 都使用同一套计算。哈希不读取 value，不按 `\0` 截断，支持空 key 和包含任意字节的 key。固定算法保证样本结果可重复；不使用可能随实现变化的 `std::hash` 作为测试测量依据。

上限检查在 `expected_keys * 10` 之前进行，避免恶意文件头导致整数溢出或巨大分配。即使条目数巨大而禁用了过滤，完整文件校验仍然进行，坏文件仍拒绝打开。

每个 reader 都有自己的过滤器；上限不是整个 Store 的总内存预算，打开很多 SSTable 时会累加。compaction 的输入 reader 只做 Next 全扫描，显式关闭 Bloom，避免为不会用到的过滤器分配空间和计算哈希。合并后的前台查询 reader 正常构建过滤器。

## 删除标记为什么必须加入

假设旧表有 `name=Alice`，新表只有“删除 name”。如果新表过滤器漏掉删除标记，Get 就可能跳过新表，接着从旧表读到 Alice。

因此构建时对 Value 和 Tombstone 都执行 Add(key)。Bloom 表示“文件中有没有这个 key 的记录”，不是“有没有一个仍然存活的值”。

命中新表的删除标记后，Store 仍按原有逻辑立即返回不存在。只有安全的全量 compaction 已同时去掉旧值和最新删除标记时，新输出中才不再有这个 key，过滤器也不再主动加入它。

## Get 如何接入

```cpp
++read_stats_.get_calls;
if (bloom_ && !bloom_->MayContain(key)) {
    ++read_stats_.bloom_negatives;
    return std::nullopt;
}
++read_stats_.table_scans;
Rewind();
// Continue the original exact lookup.
```

负判断发生在 Rewind/Next 之前，因此不会为这次 Get 定位或解码文件记录。若“可能存在”，仍走原来的精确查询，误判不会制造一个 value。

reader 仍有共享游标：需要扫描的 Get 会 Rewind 并推进游标，Bloom 负判断则保持游标原样。Next 用于完整遍历，永远不会因为 Bloom 跳过任何记录。测试覆盖了这个行为。

可用 `SSTableReader(path, false)` 关闭过滤，进行结果对照或完整扫描。Store 默认开启。

## 统计量如何解读

`SSTableReader::read_stats()` 返回：

| 字段 | 含义 |
| --- | --- |
| get_calls | Get 调用次数 |
| bloom_negatives | 确定没有，直接跳过的次数 |
| table_scans | 进入原精确扫描的次数，含真实命中和误判 |
| entries_examined | 这些 Get 实际解码并检查的记录数 |

不计打开时的校验，也不计 Merge/Next 的全遍历。它们是逻辑工作量，不是系统调用数或物理磁盘读取次数：文件流和操作系统可能已经缓存了数据。Store 汇总统计只在测试 friend 中使用，本轮没有新增命令行统计命令。

固定样本结果：

| 样本 | 结果 |
| --- | --- |
| 加入 10,000 个 key，再检查这些 key | 无假阴性，位数组 12,500 字节 |
| 查询另外 20,000 个缺失 key | 158 次假阳性，样本误判率 0.79% |
| 一张 128 条记录的表，200 个排在末尾之后的缺失 key，开启 Bloom | 跳过 199 次扫描，解析 128 条记录 |
| 同一张表、同一批查询，关闭 Bloom | 扫描 200 次，解析 25,600 条记录 |

这是固定数据集的测量，不代表所有负载都具有相同误判率，也不是吞吐量、尾延迟或物理 I/O benchmark。

## C++ 写法

- `vector<uint8_t>` 是位数组的底层字节容器，类似 Java 的 byte[]；这里使用无符号字节处理高位和移位。
- `bit / 8` 定位字节，`bit % 8` 定位字节内的位。`1U << ...` 创建掩码，`|=` 设位，`&` 检查该位。
- `auto [position, step] = Hashes(key)` 是 C++17 结构化绑定，把返回的 pair 中两个数分别命名；可以理解成分别读取 `.first` 和 `.second`。
- `uint64_t` 的加乘溢出按无符号规则回绕，Add 和查询完全一致；右移时高位补零。它不是需要捕获的算术异常。
- Add 接收 string_view，但立即计算位置，不保存传入字符串的指针，临时字符串销毁后过滤器仍有效。
- Reader 的 `optional<BloomFilter>` 表示是否配置过滤器；配置后也可能因超出内存上限而禁用，禁用时始终保守扫描。

过滤器在 reader 发布前构建完毕，之后只读，不新增锁。Get 的计数器与 reader 游标一样，由 Store 原有状态锁保护；独立 SSTableReader 仍要求调用方串行访问。

## 文件改动与验证

| 文件 | 作用 |
| --- | --- |
| `include/kvstore/bloom_filter.h`、`src/bloom_filter.cpp`，新增 | 位数组、固定哈希、Add/MayContain 和容量降级 |
| `include/kvstore/sstable.h`、`src/sstable.cpp` | 打开时构建、Get 前置判断、关闭选项与逻辑计数器 |
| `src/compaction.cpp` | 输入全扫描 reader 关闭无用过滤器 |
| `tests/bloom_filter_test.cpp`，新增 | 7 个独立过滤器测试 |
| `tests/sstable_bloom_test.cpp`，新增 | 9 个 SSTable/Store 接入测试 |
| CMake、README、GOAL、ROADMAP | 构建目标、运行方式与进度 |

独立测试覆盖空集合、多种大小下的无漏判、空/二进制/最大长度 key、临时字符串、重复与超额加入、内存上限及整数极值、加入顺序和移动后的安全状态、固定样本误判率。

接入测试覆盖开启/关闭结果一致及扫描量对照、真实假阳性的精确回退、游标规则、删除标记与空表、重开重建、坏文件不被掩盖、Store 删除不复活、多 SSTable 跳过、部分/全量合并输出，以及三调用线程与后台刷盘/合并交错后的 36 个 key 恢复。

在项目根目录运行：

```sh
cmake --build build --target bloom_filter_test sstable_bloom_test
./build/bloom_filter_test
./build/sstable_bloom_test
```

若当前构建目录尚未配置新目标，先执行一次 README 中的配置命令，或构建全部目标让 CMake 自动重新生成规则。

普通与 AddressSanitizer/UndefinedBehaviorSanitizer 构建十一组 CTest 通过，共 131 个测试函数。ThreadSanitizer 运行后台合并、后台刷盘、SSTable Bloom 接入三组测试，共 28 个函数，通过且无数据竞争报告。没有进行真实断电或性能实验。

第 9 步块索引与 block cache 尚未开始。先讨论“为何没有漏判、为何仍需精确查询、为何删除标记必须加入”。
