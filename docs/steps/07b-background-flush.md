# 第 7b 步：后台刷盘、冻结表与两个 WAL 的恢复

第 7 步至此完成：7a 后台合并，7b 后台刷盘。先掌握本轮的三件事即可：

1. 旧 MemTable 冻结后不再修改，交给后台写 SSTable。
2. 新写入使用新的 MemTable 和 WAL，查询优先读取新状态。
3. 冻结表的 WAL 必须等 SSTable 正式发布后才能删除。

## 原来与现在

7a 的 Flush 从写 SSTable 到发布、回收都占着状态锁。现在拆成三个阶段：

```text
前台，持锁：同步新 WAL → 发布双 WAL 清单 → 冻结旧 MemTable，切换可写表
后台，无锁：读取冻结表 → 生成 SSTable → 同步并校验新文件
后台，持锁：发布新清单 → 接入 reader，移除冻结表 → 回收旧 WAL
```

生成 SSTable 的大批量 I/O 不再占状态锁；创建新 WAL、发布清单、回收文件仍持锁。没有实现无锁读写，也没有测量性能提升。

每个 Store 有两个后台线程：一个刷盘，一个 compaction。这样合并耗时较长时，刷盘仍能推进；7a 中“合并暂停期间前台 Flush 可以完成”的行为也继续成立。它们发布 manifest 时仍共用 `mutex_`，不会同时写同一个 `.manifest.tmp`。

## 先看一次完整例子

开始时只有可写表 A，对应 WAL 0：

```text
可写 MemTable A  ← 新写入
WAL 0           ← 新日志
```

请求刷盘后，预留 SSTable 1，创建并同步 WAL 2，再正式发布：

```text
清单：SSTable []，冻结 WAL 0，可写 WAL 2，next 3

冻结 MemTable A  → 后台准备写 SSTable 1
可写 MemTable B  ← 后续新写入
WAL 0           → 暂时保留，不再追加
WAL 2           ← 后续新日志
```

后台写完、同步并校验 SSTable 1 后，再发布：

```text
清单：SSTable [1]，无冻结 WAL，可写 WAL 2，next 3
```

此后移除冻结表 A，删除 WAL 0。可写表 B 不清空，WAL 2 不回收。

两次发布各有目的：第一次保证新写入所在的 WAL 能被重启发现；第二次确认冻结表已由 SSTable 接替，旧 WAL 才能回收。

## 查询顺序

```text
可写 MemTable → 冻结 MemTable → SSTable，从新到旧
```

例如冻结表中 `name=Alice`，新表写入 `name=Bob`，Get 返回 Bob。新表删除 name，Get 返回不存在，不能再回到冻结表取 Alice。

每层的 Lookup 都区分“没有此 key”和“有删除标记”。只有没有此 key 才继续查下一层；删除标记必须立即停止查询。冻结表尚未发布为 SSTable 时，它本身就是有效的查询来源。

## 接口如何使用

| 接口 | 本轮后的行为 |
| --- | --- |
| `RequestFlush()` | 冻结当前表、切换 WAL 并安排后台刷盘；不等待该 SSTable 完成 |
| `Flush()` | 同样安排后台刷盘，然后等待本次需要刷出的数据正式发布并回收旧 WAL |
| `WaitForBackgroundWork()` | 先等调用时已有的刷盘请求，再捕获并等待合并请求；包含这些刷盘触发的自动合并 |
| `Compact()` / `RequestCompaction()` | 继续合并选取时已有的 SSTable，不主动冻结 MemTable |
| `memtable_bytes()` | 当前可写表最新条目的编码大小 |
| `frozen_memtable_bytes()` | 当前冻结表的编码大小，无冻结表返回 0 |

`Flush()` 仍保持调用方看来是同步的，因此 CLI 的 `flush` 命令不变。空可写表不会新建文件，但如果已有冻结表，Flush 仍等待它完成。

在长时间运行的程序里，可以这样使用：

```cpp
store.Put("name", "Alice");
store.RequestFlush();
store.Put("name", "Bob");
auto value = store.Get("name");
store.WaitForBackgroundWork();
```

value 是 Bob；它可能在新的可写表中。后台刷盘不会清空这个新表。

默认 4 MiB 自动阈值保持不变，但触发的是冻结和后台任务，而非在 Put 中写完整个 SSTable。成功 Put/Delete 仍先同步对应 WAL，持久化承诺没有改成“只写内存”。

## 为什么只允许一张冻结表

如果后台跟不上，而前台不断冻结新表，内存和待恢复 WAL 都会越积越多。本轮只设置一个冻结槽：最多一张可写表、一张冻结表，清单最多引用两个 WAL。

第二次 RequestFlush/Flush 需要冻结时，如果槽还满着，就通过条件变量等待，等待期间释放状态锁。Get 仍能访问可写表和冻结表，后台也能拿锁完成发布。

自动刷盘开启时，可写表达到阈值后，后续写入在追加 WAL 前会等待可用空间，或先帮助冻结已有的满表；不会让其他写线程无限追加到一张正在等待冻结的满表。这叫背压：写入过快时，让写入方等待后台。

阈值统计编码大小，不是精确堆内存。单条记录可令大小越过阈值，map 节点、后台 Snapshot 副本、编码缓冲也占内存，因此不声称总内存严格等于两倍阈值。第三个构造参数设为 0 会关闭自动阈值，活跃表的大小由调用方手动刷盘控制；冻结槽数量仍限制为 1。

覆盖少数 key 时，MemTable 大小可能一直很小，而 WAL 历史继续增长。本轮没有新增 WAL 文件大小阈值，仍可以手动请求刷盘。

## manifest 与恢复

`Manifest` 新增：

```cpp
std::optional<std::uint64_t> frozen_wal_id;
```

没有冻结表时，继续编码为原来的 KVM1；有冻结表时使用 KVM2，多存一个旧 WAL 编号：

```text
KVM1 | active WAL:u64 | next:u64 | SST count:u32 | SST IDs:u64[count] | CRC32
KVM2 | active WAL:u64 | next:u64 | SST count:u32 | frozen WAL:u64 | SST IDs:u64[count] | CRC32
```

字段均沿用小端序。KVM1 最小 28 字节，KVM2 最小 36 字节。新版本可以读取已有清单；旧版本无法读取包含冻结 WAL 的 KVM2 清单。WAL 和 SSTable 的文件格式不变。

冻结 WAL 必须比当前 WAL 的编号小，不能与 SSTable 编号冲突。`optional` 中有值 0 表示原始基础路径的 WAL；它与没有值不同。`if (frozen_wal_id)` 判断的是有没有值，0 也算有值。

启动流程：

1. 读取清单，打开并校验全部有效 SSTable。
2. 如果有冻结 WAL，严格回放到冻结 MemTable。缺失、空文件、CRC 损坏或不完整尾部均拒绝打开。
3. 打开当前 WAL，回放到可写 MemTable；只有这里允许显式 `recover-tail` 截断不完整尾部。
4. 全部有效文件验证成功后，才清理未引用文件。两个有效 WAL 都必须保留。
5. 启动后台线程。如果恢复出了冻结表，自动继续将它刷出。

两代 WAL 回放到不同的表，由查询顺序保证新状态覆盖旧状态。恢复时会为冻结表重新预留输出编号，原先未正式发布的输出作为孤立文件清理，因此编号可能跳跃。

## 两个后台任务如何交接

刷盘和 compaction 都必须在发布时复制最新 manifest。

例如刷盘预留了输出 5、切换到 WAL 6；它生成文件期间，compaction 将旧 SSTable `[1, 3]` 合并成 7：

```text
刷盘尚未发布：SSTable [7]，冻结 WAL 4，可写 WAL 6
刷盘发布之后：SSTable [7, 5]，无冻结 WAL，可写 WAL 6
```

5 的内容更新，虽然编号比 7 小，也必须放到列表末尾。文件编号不决定查询新旧顺序，清单中的顺序才决定。

反过来，刷盘先追加新表时，compaction 仍按 7a 的规则只替换自己的旧前缀，保留新表。冻结期间的删除标记留在冻结表及其 SSTable 中；compaction 只能清理它覆盖的全部更旧磁盘状态。

## 锁与退出

前台仍共用一把 `mutex_`，Get 还要保护 reader 的文件游标。Put/Delete 改用 `unique_lock`，因为背压等待需要临时释放锁；等待结束后重新持锁，才继续追加 WAL。

冻结和正式发布都持锁；后台遍历冻结表、写出和校验新 SSTable 不持锁。冻结表不会再被修改，所以后台 Snapshot 与前台 Lookup 可以同时读取它。

析构时通知两个线程收尾，并先等待刷盘线程，再等待合并线程。合并线程只有确认刷盘线程已经退出、合并请求也处理完才退出，避免漏掉最后一次刷盘触发的自动合并。join 期间不持状态锁。

已经冻结的表会刷完；没有请求刷盘的可写表不在退出时额外冻结，仍由当前 WAL 恢复。所有调用方必须结束访问后再销毁 Store。

后台失败沿用 7a 的保守策略：保存异常、唤醒等待者、停止后续操作，关闭后重开。析构不会抛出后台错误；需要确认成功时，先调用等待接口。

## 本次新增的 C++ 写法

`frozen_` 和刷盘线程的局部变量共同使用 `std::shared_ptr<FrozenTable>`。它们指向同一张冻结表，不会复制 map；最后一个持有者释放时才销毁它，避免 Store 切换查询来源后，后台仍使用的对象提前消失。Java 的对象引用通常依靠 GC 保活；这里通过引用计数管理生命周期。

shared_ptr 本身不会让对象内容自动线程安全。我们仍在锁内修改 Store 的 `frozen_`，并保证冻结表内容永远只读。

`MemTable::Swap()` 交换两张 map 的所有权，把原可写表交给冻结对象，新可写表变成空表。它不逐条复制数据，也不在清单已经发布后的交接阶段分配 map 节点。后台使用的 Snapshot 才是独立副本，并在锁外生成。

## 文件改动与阅读顺序

| 文件 | 作用 |
| --- | --- |
| `src/background_flush.cpp`，新增 | 先看 FreezeLocked，再看 RunFlush；分别实现冻结交接与后台发布，FlushLoop 负责等待/完成通知 |
| `include/kvstore/store.h` | 新接口、FrozenTable、刷盘线程与计数器、冻结测试阶段 |
| `src/store.cpp` | 查询冻结表、双 WAL 回放、保留有效 WAL、统一清单发布与线程生命周期 |
| `include/kvstore/manifest.h`、`src/manifest.cpp` | 可选冻结 WAL、KVM2 编解码与校验 |
| `include/kvstore/memtable.h`、`src/memtable.cpp` | 增加不复制条目的 Swap |
| `src/background_compaction.cpp` | 等待接口包含刷盘，退出前等待刷盘线程收尾 |
| `tests/background_flush_test.cpp`，新增 | 暂停后台后的查询、写入、背压、双任务交接、异常和退出 |
| `tests/store_flush_test.cpp`、`tests/manifest_test.cpp` | 更新协议边界，增加冻结、双 WAL 和真实部分写入恢复验证 |
| `CMakeLists.txt`、README、GOAL、ROADMAP | 源文件与测试目标、运行方式、完成状态 |

没有新增 VS Code 配置或第三方依赖。已有 Notify 检查点继续只用于测试；冻结与后台刷盘现在分别使用 FreezeStage 和 FlushStage，避免混淆两次发布。

## 如何运行与验证

在项目根目录：

```sh
cmake --build build --target background_flush_test
./build/background_flush_test
```

成功输出 `All 10 background flush tests passed.`，十个测试函数覆盖：

1. 冻结表覆盖旧 SSTable、可写表再覆盖冻结表；删除标记、空值、二进制与重开。
2. 可写表为空时，Flush 仍等待已有冻结表。
3. 单冻结槽背压，不创建第三个有效 WAL。
4. 自动阈值下限制满表继续增长，等待期间 Get 能执行。
5. 等待刷盘以及刷盘触发的自动合并。
6. compaction 先发布、冻结表后发布时，保持最新清单与正确新旧顺序。
7. 后台刷盘失败传播、停止操作、重开恢复。
8. 五个冻结边界的异常与恢复。
9. 退出等待刷盘以及随后触发的自动合并，活跃数据仍由 WAL 恢复。
10. 三个调用方交错自动刷盘、手动请求、合并，48 个 key 重开后符合预期。

原刷盘测试增至 21 个函数，包括五个冻结边界、七个后台刷盘边界的强杀，两个 WAL 的严格校验和当前 WAL 的显式尾部处理，以及向新 WAL 写入后再强杀的七个刷盘边界。真实文件大小限制分别覆盖冻结清单、SSTable 和刷盘清单的部分写入。manifest 测试增至 6 个函数，新增 KVM2 往返、截断、损坏、编号冲突验证。

普通构建与 AddressSanitizer/UndefinedBehaviorSanitizer 构建九组 CTest 通过，共 115 个测试函数。ThreadSanitizer 单独运行两组后台测试，共 19 个函数，通过且无数据竞争报告。没有真实断电实验或性能测量。

下一步是第 8 步 Bloom filter，本轮不开始。先讨论冻结交接、两次 manifest 发布及查询顺序。
