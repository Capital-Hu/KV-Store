# 第 4 步：独立 SSTable 文件读写

> 路径说明：本文记录本步骤完成时的文件位置。后续目录已按模块整理，当前路径见[第 16a 步](16a-project-layout.md)；构建与运行命令以[项目 README](../../README.zh-CN.md)为准。

## 这一步做成了什么

SSTable 是 Sorted String Table：把按 key 排好序的数据写进文件，写好以后不再原地修改。本步提供独立的写文件和读文件接口，不改变 Store 的写入、恢复和 CLI。

```text
已排序、无重复 key 的 entries → WriteSSTable → 新的 .sst 文件
已有 .sst 文件 → SSTableReader 全文件校验 → Get(key) 顺序查找
```

这里的“不可变”是接口约定：写入接口拒绝覆盖已有路径，读取接口不修改文件。它不是操作系统层面的防篡改，外部程序仍可能改文件；使用期间不得这样做。

MemTable 自动刷成 SSTable、加载多个文件、持久化文件集合和回收 WAL 都属于第 5 步。当前 main 仍使用 WAL + MemTable，不会自动产生或查询 SSTable。

## 改了什么，为什么

| 文件 | 变化与原因 |
| --- | --- |
| `include/kvstore/sstable.h` | 声明数据条目、写文件函数、只读查询类 |
| `src/sstable.cpp` | 输入检查、新文件创建、二进制写出、完整校验和点查 |
| `tests/sstable_test.cpp` | 独立验证文件格式、三种查询结果和损坏处理 |
| `CMakeLists.txt` | 引擎加入 SSTable 实现，新增 `sstable_test` 目标和 CTest 登记 |
| `README.md`、`GOAL.md`、`ROADMAP.md` | 更新进度、运行方式及下一步边界 |

已有 MemTable、WAL codec、Store、main 和三组测试的源文件没有改动；VS Code 配置没有改动。

## 建议的阅读顺序

1. `sstable.h`：先理解一条数据和查询结果是什么。
2. `sstable_test.cpp` 的前四个测试：看调用示例和预期结果。
3. `sstable.cpp` 的 `WriteSSTable`、构造函数、`Get`：看主流程。
4. 再看匿名命名空间里的文件读写和解码辅助函数。

## 数据条目和三种查询结果

```cpp
enum class SSTableEntryType { Value, Tombstone };

struct SSTableEntry {
    SSTableEntryType type;
    std::string key;
    std::string value;
};
```

Value 保存一个值，包括空字符串；Tombstone 是删除标记，其 value 必须为空。

例如，旧文件里存在 `name=Alice`，新的文件里保存 `name` 的删除标记。未来多文件查询看到较新的删除标记，就应停止查找，避免继续从旧文件读出 Alice。本步只保存并返回标记，尚未实现多文件查询。

`Get` 返回 `std::optional<SSTableEntry>`：

| 返回结果 | 含义 |
| --- | --- |
| `std::nullopt` | 这个文件没有该 key，未来可能需要查其他文件 |
| 包含 `Value` 条目 | 这个文件保存了该 key 的值，value 可以为空 |
| 包含 `Tombstone` 条目 | 这个文件明确记录了该 key 被删除 |

这就是为什么 SSTable 查询不能直接沿用 MemTable 的 `optional<string>`：我们需要把“不在这个文件里”和“这个文件说它已经被删除”区分开。

## 如何使用

下面是接口示例，可在测试中看到对应用法；不是新的 CLI 命令。

```cpp
kvstore::WriteSSTable("build/example.sst", {
    {kvstore::SSTableEntryType::Value, "age", "20"},
    {kvstore::SSTableEntryType::Tombstone, "city", ""},
    {kvstore::SSTableEntryType::Value, "name", "Alice"},
});

kvstore::SSTableReader reader("build/example.sst");
const auto result = reader.Get("name");
if (result.has_value() && result->type == kvstore::SSTableEntryType::Value) {
    // result->value is "Alice".
}
```

调用者必须提供按 `std::string` 比较规则严格递增的 key，不进行语言排序或大小写转换；字节内容可以包含 `\0`。重复 key、乱序、非法类型、带 value 的删除标记和超限数据都会在创建文件之前被拒绝。

每条 key + value 的总长度不超过 1 MiB，与 WAL 一致。表内记录数用 32 位无符号整数表示，最多 `2^32 - 1` 条；实际写入仍受内存、磁盘等资源限制。计数包括删除标记。

路径不能为空或包含 `\0`，父目录必须存在。已有路径不能覆盖，所以重复运行示例时，须使用新的输出路径或自行处理之前的示例文件。

## 文件长什么样

最小格式为：

```text
[KVS1:4 bytes][entry_count:4 bytes][entry 1][entry 2]...[entry N]
```

前 8 字节是 SSTable 文件头：

| 零起始偏移 | 内容 |
| --- | --- |
| 0..3 | `KVS1`，本步 SSTable 格式与版本标识 |
| 4..7 | 条目数量，u32，小端序；数量 2 编码为 `02 00 00 00` |
| 8 起 | 按 key 严格递增的条目 |

每个条目复用第 2 步的 WAL 记录编码：

```text
KVW1 | type:u8 | key_size:u32 | value_size:u32 | key | value | crc32:u32
```

SSTable 的 Value 在磁盘条目中编码成 Put，Tombstone 编码成 Delete。转换只发生在实现内部；公共接口用 SSTable 自己的条目类型表达含义。

这不是把 WAL 文件改名成 SSTable。WAL 按操作发生顺序追加，同一个 key 可以出现多次；SSTable 按 key 排序，一个文件里每个 key 只出现一次，创建后不再追加。复用的是长度编码、校验和单条记录格式，不是日志回放语义。

本步这样做是为了少引入一套编解码器。以后引入磁盘块、索引或 Bloom filter 时，需要相应演进文件格式；当前不承诺跨格式版本兼容。

空表也是合法文件：8 字节文件头，记录数为 0。零字节文件不是合法 SSTable。

为什么需要记录数量？假如最后一整条记录都丢了，剩下的记录可能各自 CRC 正确。计数能让读取方发现“头部说有 3 条，实际只有 2 条”。读取完指定数量后，还会确认文件结束，拒绝多余记录和尾部垃圾。文件头本身没有另加 CRC；格式标识、数量与实际完整条目的匹配负责检测这部分错误，条目各自有 CRC。这不是对恶意篡改的认证。

## 写入和读取怎么工作

`WriteSSTable`：

1. 验证路径、数量、所有条目以及排序，避免无效输入留下半个文件。
2. 用 `O_CREAT | O_EXCL` 创建新文件，已有路径直接失败，不先检查再覆盖。
3. 写出文件头，然后逐条编码并写出。底层处理短写和 `EINTR`。
4. 显式关闭文件并检查错误；异常路径由 RAII 关闭资源。

`SSTableReader` 构造函数：

1. 打开已有普通文件，检查 8 字节文件头。
2. 按数量逐条读取。先读取 13 字节条目头，用已有 codec 验证类型和长度上限，再分配并读取剩余内容，检查 CRC。
3. 检查 key 严格递增，不依赖写入方一定用了我们的函数。
4. 确认没有缺少记录，也没有多余尾部。

SSTable 中任何不完整或确认损坏的内容都会使打开失败；读取方不截尾、不修改文件。这里没有 WAL 那样的显式尾部恢复模式。

`Get`：

1. 清除输入流的 EOF 等状态，把读取位置移回文件头之后。
2. 从第一条开始逐条解码，匹配 key 则返回条目。
3. 如果当前 key 已经大于目标 key，利用排序提前停止；否则继续到表末尾。

打开和最坏情况下单次查询都需要扫描整个文件。读取期间只保留当前条目等少量数据，空间随最大条目大小变化，不会把完整表读进 map。写入接口接收的 vector 则由调用者提前放在内存中；它还不是流式 builder。

当前优先让行为直观、可验证。块读取和查询加速留到后续步骤，不能把这版顺序扫描当成最终性能实现。

## 这一步的持久化边界

本步 writer 完成文件写入和关闭，没有调用 `fsync` / `F_FULLFSYNC`，没有同步父目录，也没有原子发布文件集合。成功返回可以用于重新打开读取，不代表已经提供断电安全的 Store 刷盘承诺。

创建后发生 I/O 或内存分配失败，可能留下不完整文件，不会自动删除。该文件不能因为扩展名是 `.sst` 就被当成有效数据文件；它可能被校验拒绝，也可能碰巧完整但调用方没有得到成功返回。

第 5 步需要把“写临时/未发布文件、同步、发布文件集合、处理孤立文件、回收旧 WAL”组成正确流程。当前 WAL 和 Store 的持久化保证没有变化，也没有删除任何 WAL。

## 本步遇到的 C++ 写法

- `std::vector<SSTableEntry>`：类似 Java 的 `ArrayList<Entry>`，这里直接存放条目对象，不是默认存放对象引用。
- `const std::vector<SSTableEntry>&`：借用调用者的列表进行只读访问，不复制整个列表；函数也不会长期保存这份引用。
- `static_cast<uint32_t>(entries.size())`：显式转换整数类型。先检查数量上限，再转换，避免截掉高位。
- `std::ifstream`：拥有文件资源的输入流。成员对象会自动析构，因此 Reader 不需要手写析构函数；构造中途抛异常时，已构造的流成员也会清理。
- `input_.clear()` 清除流的错误/EOF 状态，不是删除文件内容；`seekg(8)` 移动读取位置，不是改变文件内容。
- `Get` 没加 `const`：它不改磁盘数据，但会改变成员输入流的读取位置和状态。不能仅凭“不修改 key/value”就判断所有成员都保持不变。
- `std::move(record.key)`：允许把即将不用的字符串资源交给返回条目，避免再复制一次大字符串。它不移动磁盘文件，也不是 Java 对象引用赋值；被移动的对象仍可析构，但不应继续假设它保存原来的内容。
- `NewFile` 是实现内部的资源管理类；它不能复制，避免两个对象关闭同一文件描述符。它负责创建和写字节，公共函数负责条目规则和格式。

## 如何运行

在项目根目录执行：

```sh
cmake --build build --target sstable_test
./build/sstable_test
```

成功输出：

```text
All 12 SSTable tests passed.
```

测试创建独立临时文件并自动清理，不使用 `build/demo.wal`。现有 main 的运行方式不变。

## 12 个测试分别验证什么

| 测试 | 实现方式与检查内容 |
| --- | --- |
| SortedLookupAndRepeatedReads | 写入 b/d/z，反复以不同顺序查询，检查首中尾和前/中/后的缺失 key |
| EmptyTable | 写出零条记录的文件，检查 8 字节文件头、计数和查询为空 |
| EmptyAndBinaryStrings | 写入空 key/value、含 `\0` 和高位字节的内容，检查完整往返 |
| TombstoneIsNotMissingOrEmpty | 同表保存删除标记和空值，检查它们与缺失 key 的三种结果不同 |
| UnsortedAndDuplicateInput | 输入倒序或重复 key，检查拒绝且不创建文件 |
| InvalidEntriesLeaveNoFile | 输入非法类型、带值的删除标记、超限 key/value，包括较后位置的坏条目，检查预先拒绝 |
| MaximumPayload | 连续写入两条恰好达到 1 MiB payload 上限的记录及一条小记录，检查读回 |
| GoldenFile | 与独立固定字节样本比较写出结果，再单独读入样本，避免仅靠自身编码/解码互相证明 |
| EveryTruncatedPrefix | 遍历合法文件的所有更短前缀，包括整条记录边界，检查拒绝且文件不变 |
| CorruptionAndTrailingBytes | 逐字节翻转一个 bit，追加垃圾/多余完整记录，制造超大长度，检查打开失败且不自动截尾 |
| InvalidOrderingOnDisk | 手工拼接 CRC 正确但乱序或重复的记录，检查读取方独立验证排序 |
| ExistingFileAndInvalidPaths | 检查不能覆盖已有文件、无效路径和目录输入会报错，原数据保留 |

## 验证结果与下一步

本机普通构建四组 CTest 全部通过，共 44 个测试函数：8 个 MemTable、12 个 WAL、12 个 Store、12 个 SSTable。启用 `-Wall -Wextra -Wpedantic`、AddressSanitizer 和 UndefinedBehaviorSanitizer 的独立构建也通过四组测试，无编译警告或检查错误报告。

本步未测试真实断电，也未穷举文件写入错误。当前未接入刷盘、文件集合、WAL 回收、后台线程、Bloom filter、块缓存或复制协议。

完成讨论并收到明确 OK 后，才开始第 5 步；届时可先拆分删除标记在 MemTable 中如何保存、刷盘接口和文件发布流程。
