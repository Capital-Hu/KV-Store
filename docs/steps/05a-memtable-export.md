# 第 5a 步：MemTable 删除标记和有序导出

> 路径说明：本文记录本步骤完成时的文件位置。后续目录已按模块整理，当前路径见[第 16a 步](16a-project-layout.md)；构建与运行命令以[项目 README](../../README.zh-CN.md)为准。

## 这轮的范围

第 5 步需要把 MemTable、SSTable、文件集合和 WAL 生命周期接起来。为保持每轮可以理解，本轮先完成前置条件：MemTable 保留删除信息，并能导出一份可转换成 SSTable 的有序数据。

```text
Put/Delete → MemTable 保留每个 key 的最新状态
                  ↓ Snapshot()
           有序、独立的数据副本
                  ↓ 测试中转换为 SSTableEntry
           WriteSSTable → 读回验证
```

这条转换路径目前只在集成测试里演示。Store 没有增加 Flush 或多文件查询，main 命令和 WAL 格式不变；没有回收任何日志。

## 为什么先改删除

之前 MemTable 是：

```cpp
std::map<std::string, std::string> entries_;
```

`Delete(key)` 使用 erase，把 key 从 map 中移除。这在只有 MemTable 的时候能满足查询，但无法表达“这里发生过一次删除，请不要继续从旧 SSTable 读取这个 key”。

现在改为：

```cpp
std::map<std::string, std::optional<std::string>> entries_;
```

| map 的状态 | 含义 |
| --- | --- |
| 没有这个 key | 这张 MemTable 没有它的状态，未来可以查更旧的文件 |
| key 对应一个 string，包括空 string | 有一个当前值 |
| key 对应 nullopt | 删除标记，未来查询应停止，不能返回旧文件的值 |

`Put` 用新值覆盖已有值或删除标记。`Delete` 写入 nullopt，不再 erase；删除当前不存在的 key 也保留标记，因为将来旧文件里可能存在它。

Delete 的 bool 仍表示“本次是否删除了 MemTable 中一个可见值”：已有值返回 true，没有值或已经是删除标记返回 false。返回 false 不表示什么都没记录，删除标记依然存在。未来 Store 接入磁盘层后，需要由 Store 根据完整查询结果决定其 Delete 返回值。

## Get 和 Lookup 为什么分开

现有 `Get` 仍返回 `optional<string>`，用户看到的行为不变：有值则返回值，不存在或已删除都返回 nullopt。

新增 `Lookup` 用于存储引擎内部，返回 `optional<MemTableEntry>`：

```cpp
struct MemTableEntry {
    std::string key;
    std::optional<std::string> value;
};
```

这里有两层不同的判断：

```cpp
const auto result = table.Lookup("name");
if (!result.has_value()) {
    // No entry in this MemTable; a future Store may search older files.
} else if (!result->value.has_value()) {
    // An entry exists, but its value is a tombstone. Stop searching.
} else {
    // A visible string value exists, including an empty string.
}
```

外层 optional 表示“有没有条目”；条目里面的 optional 表示“这个条目有没有值”。结果是独立副本，不借用 map 内部的字符串。

当前 Store 仍只需要 Get：它没有磁盘层可以继续查询。现有启动回放调用 MemTable::Delete 时，现在也会在内存保留标记；WAL 字节和回放顺序没有变化。

## Snapshot 和 Clear 做什么

`Snapshot()` 返回 `vector<MemTableEntry>`，遍历 map，把每个 key 的最新状态复制出来，包括删除标记。map 本来就有序，所以不需要额外排序，也不会导出同一个 key 的多个历史版本。

```text
操作：Put(z, old) → Put(a, "") → Delete(m) → Put(z, latest)

导出：
a → 空字符串值
m → 删除标记
z → latest
```

导出不会清空原表，也不会写文件。后续更新或清空 MemTable，不会改变已经返回的 vector；调用方修改 vector，也不会反过来修改表。

这个 Snapshot 只是“当前内容的复制导出”，没有事务隔离、MVCC 或并发一致性保证。MemTable 仍按单线程使用。

新增 `Clear()` 用于明确清空值和删除标记。当前 Store 不调用它；未来只有在数据安全交给 SSTable 并发布文件集合后，才能清空对应内存状态。测试里的 Clear 只操作独立内存表，不删除 WAL 或 SSTable。

## 为什么内存条目不用 SSTableEntry

MemTable 用 optional 表达“值/删除”，SSTable 文件接口用 Value/Tombstone 枚举表达同样两种状态。测试中的转换是：

```cpp
std::vector<kvstore::SSTableEntry> entries;
for (const auto& entry : table.Snapshot()) {
    const auto type = entry.value.has_value()
                          ? kvstore::SSTableEntryType::Value
                          : kvstore::SSTableEntryType::Tombstone;
    entries.push_back({type, entry.key, entry.value.value_or("")});
}
kvstore::WriteSSTable(path, entries);
```

这样 MemTable 仍然只管理内存，不依赖文件读取类或 SSTable 的编码规则。转换最终会放到协调刷盘的层；本轮先用已有 writer 验证接口能接起来。

`value_or("")` 在有值时返回值，没有值时返回空字符串。删除信息由 type 保留，因此不会把删除标记误当成一个空值。

## 改了哪些文件

| 文件 | 修改 |
| --- | --- |
| `include/kvstore/memtable.h` | map 的 value 改为 optional，新增 MemTableEntry、Lookup、Snapshot、Clear |
| `src/memtable.cpp` | 删除保留标记，实现查询、排序导出和清空 |
| `tests/memtable_test.cpp` | 保留原来的 8 个测试，新增 4 个测试 |
| `tests/sstable_test.cpp` | 保留原来的 12 个测试，新增内存导出到 SSTable 的验证 |
| `README.md`、`ROADMAP.md`、`GOAL.md` | 更新阶段进度、运行命令和范围 |

没有修改 Store、main、WAL codec、SSTable 实现、CMake 或 VS Code 配置。Store 重新编译是因为它包含了改变后的 MemTable 头文件，并不是给 Store 增加了文件管理代码。

## 新测试验证什么

| 新测试 | 检查内容 |
| --- | --- |
| LookupStatesAndOverwrite | 缺失、空值、删除标记三种状态；删除缺失 key；重新写入；重复删除 |
| SortedSnapshotIncludesTombstones | 导出有序且无重复，只保留最新值，包含两种来源的删除标记；导出不清表 |
| SnapshotOwnsItsData | 含 `\0` 的字符串完整复制；更新原表不影响导出，修改导出不影响原表 |
| ClearRemovesAllEntries | 清空值和标记；先前导出副本仍有效；清空后可继续写入 |
| MemTableSnapshotToSSTable | 写入、覆盖、删除后导出，转换并写成 SSTable，再读回最新值、空值、删除标记和缺失 key |

## 本步的 C++ 写法

- map 中每个元素是一个 pair：`entry.first` 是 key，`entry.second` 是 optional value。`it->second` 先通过迭代器访问这个 pair，再取它的 second 成员。
- `entries_[key] = std::nullopt` 是把该 key 的状态设为删除标记，不是移除 map 节点。
- `reserve(entries_.size())` 为 vector 提前预留空间，减少扩容；它不会增加元素数量。随后 push_back 才加入元素。
- `const auto& entry` 用只读引用遍历，避免每次迭代复制 map 条目；push_back 时则明确复制到独立的返回 vector。
- 返回的 vector 管理自己的元素，函数结束后仍有效；内部局部变量不会变成悬空引用。

## 运行与验证

在项目目录运行本轮相关测试：

```sh
cmake --build build --target memtable_test sstable_test
./build/memtable_test
./build/sstable_test
```

成功分别输出 `All 12 MemTable tests passed.` 和 `All 13 SSTable tests passed.`。main 的运行方式不变。

本机普通构建四组 CTest 全部通过，共 49 个测试函数：12 个 MemTable、12 个 WAL、12 个 Store、13 个 SSTable。带编译警告、AddressSanitizer 和 UndefinedBehaviorSanitizer 的独立构建也通过四组测试，无编译警告或检查错误报告。

当前导出会复制整张表，转换为 SSTableEntry 时也会有额外副本；还不是流式或后台刷盘。删除标记会占内存，尚未设置自动刷盘阈值。当前仍从 WAL 恢复整个 MemTable，WAL 不会轮换或回收。

## 后续顺序

第 5a 步完成不等于第 5 步完成。下一小步先设计并实现文件集合记录，明确哪些 SSTable 有效、它们的新旧顺序、恢复需要哪个 WAL；后续再接 Store 刷盘、加载、日志轮换和回收，并验证中断恢复。每轮仍先讨论，明确 OK 后继续。
