# 第 5 步：Store 刷盘、文件集合和 WAL 回收

## 本步完成的结构

第 5a 步让 MemTable 保留删除标记并导出有序数据。本轮把剩余流程接入 Store：手动/自动刷盘、持久化有效文件集合、多文件查询、启动恢复、WAL 轮换和回收。暂不进行 compaction 或后台刷盘。

```text
写入：Put/Delete → 同步当前 WAL → 更新 MemTable → 检查刷盘阈值
读取：MemTable → 从新到旧的 SSTable；找到值或删除标记就停止
启动：加锁 → 读取 manifest → 打开清单中的 SSTable → 回放当前 WAL → 清理孤立文件
刷盘：写 SSTable → 同步新文件 → 发布新 manifest → 切换内存状态 → 删除旧 WAL
```

Store 仍按单线程使用，Flush 在调用线程中完成。用户成功收到 Put/Delete 返回前，WAL 已同步；如果该次操作触发自动 Flush，还会等待刷盘完成。

## 路径现在代表一组文件

仍然使用 `Store("build/demo.wal")` 或 CLI 的 `build/demo.wal` 参数。它现在是整个 Store 的基础路径，不保证始终存在一个同名 WAL 文件。

| 路径示例 | 用途 |
| --- | --- |
| `demo.wal` | 编号 0 的初始 WAL；兼容之前留下的 WAL 文件 |
| `demo.wal.lock` | 不随 WAL 轮换删除的锁文件，防止两个 Store 同时使用这一组文件 |
| `demo.wal.manifest` | 当前有效文件清单 |
| `demo.wal.sst.1` | 第一次刷盘生成的 SSTable |
| `demo.wal.wal.2` | 第一次刷盘后使用的新 WAL |
| `demo.wal.manifest.tmp` | 尚未发布的临时清单，正常完成后被 rename 替换正式清单 |

打开时会把相对基础路径固定为绝对路径；之后程序切换工作目录也不会改变文件位置。

第一次刷盘后，原始 `demo.wal` 会被回收，但继续传相同基础路径即可打开数据。不要把基础路径文件的消失当成整个 Store 被删除，也不能只复制一个 WAL 就当成完整备份。

该基础路径的 `.lock`、`.manifest`、`.manifest.tmp`、规范编号的 `.sst.N` / `.wal.N` 属于 Store 保留的命名空间。启动会清理其中未被清单引用的编号文件；不要把其他数据放入这些路径。无关名称不会被清理。持锁期间不得从外部修改、删除或替换这些文件。

## manifest 保存什么

`Manifest` 是内存里的清单对象，磁盘文件编码如下：

```text
KVM1 | wal_id:u64 | next_file_id:u64 | count:u32 | sstable_ids:u64[count] | crc32:u32
 4          8              8            4             每个 8 字节          4
```

整数是小端序。清单中的 SSTable 顺序是从旧到新，读取时反过来查询；不能直接按目录遍历顺序决定优先级。所有数据文件共用文件编号空间，`next_file_id` 防止重复使用有效文件名。

例如第一次刷盘后：

```text
wal_id = 2
next_file_id = 3
sstable_ids = [1]
```

下一次刷盘生成 SSTable 3、WAL 4，清单变为 `[1, 3]`。SSTable 1 仍然有效，直到后续 compaction 替换它；本步只回收旧 WAL，不合并 SSTable。

清单检查版本、长度、数量、CRC、编号范围、重复编号和 WAL/SSTable 编号冲突，最多允许 100000 个 SSTable ID；这只是分配上限，不代表当前系统能同时打开这么多文件。后续 compaction 需要控制文件数量。

## 为什么不能直接“写 SSTable，然后清空 WAL”

如果清空 WAL 后，SSTable 尚未真正同步，崩溃就可能同时失去两份恢复依据。即使 SSTable 已写好，还需要让重启知道它是有效文件、以及旧 WAL 是否应该继续回放。

本步用替换 manifest 作为文件集合的发布点：

| 阶段 | 做什么 | 中途退出后如何恢复 |
| --- | --- | --- |
| 1 | 从 MemTable 导出有序条目，创建并同步新 SSTable | 旧清单仍有效，旧 WAL 仍保留全部待刷盘操作 |
| 2 | 创建并同步空的新 WAL，同步父目录，保证两个新文件名已持久化 | 仍使用旧清单；新文件是未发布文件 |
| 3 | 写临时清单，校验数据在内存中已编码合法，同步临时文件 | 正式清单仍旧，不能提前删除旧 WAL |
| 4 | rename 临时清单替换正式清单，再同步父目录 | 新清单成为已同步的恢复依据，引用新 SSTable 和新 WAL |
| 5 | 切换 Store 中的 reader、WAL 和清单，清空已刷盘 MemTable | 数据可以通过新清单恢复 |
| 6 | 删除旧 WAL，再同步父目录 | 不再需要旧日志；清理中断时，下次启动可重复回收 |

文件内容同步在 macOS 上使用 fsync 和 F_FULLFSYNC，目录项也同步。rename 提供名称替换的原子可见性；它本身不能代替持久化同步。参考 [Apple rename 手册](https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man2/rename.2.html) 和 [Apple fsync 手册](https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man2/fsync.2.html)。保证依赖文件系统和设备兑现同步及原子替换语义；本步没有真实断电验证。

刷盘发生写入、同步或发布阶段错误，Store 会停止后续读写，要求关闭重开。发布前后的恢复来源不同，不能一律通过“删除刚写的文件”回滚。一条已同步进 WAL 的操作，即使调用因后续自动刷盘失败而没有返回成功，重启仍可能恢复出来。

## 启动和孤立文件

启动先获取稳定锁，再读取 manifest；清单不存在且没有编号数据文件时，可以从之前的初始 WAL 引导，先持久化初始清单，再接受操作。

若清单存在，其引用的 SSTable 和 WAL 必须存在并通过检查，不能偷偷创建空 WAL 代替丢失的数据。若清单缺失但存在编号数据文件，则拒绝打开，不扫描目录猜测数据集合。

完成有效 SSTable 的完整校验和当前 WAL 回放后，才清理未列入清单的规范编号文件、临时清单和已退休的初始 WAL。这覆盖刷盘未发布或已发布但尚未回收旧文件两种中断状态。若有效文件损坏，打开失败且不进行孤立文件清理，保留现场。

`recover-tail` 仍只作用于清单指定的当前 WAL；不修复 manifest 或 SSTable。CRC 损坏仍拒绝打开，不自动跳过。旧 WAL 内容即使还留在目录中，也不会在新清单生效后重新回放。

## 刷盘什么时候触发

- 手动调用 `Store::Flush()` 或 CLI 的 `flush`。
- 默认在一次成功写入 WAL、更新 MemTable 后，待刷盘条目的估算大小达到 4 MiB，立即同步刷盘。

这里的大小是每个最新条目的 `17 + key长度 + value长度` 之和，删除标记没有 value；不统计 map 节点和分配器开销。覆盖同一个 key 会替换它的大小，而不是累积历史操作大小。

构造参数的第三项可以改变阈值，0 表示只手动刷盘：

```cpp
kvstore::Store store(path, kvstore::WalRecoveryMode::Strict, 0);
```

空 MemTable 的 Flush 不创建新文件；只有删除标记的 MemTable 仍需要刷盘。退出时不自动 Flush，未刷盘数据由 WAL 恢复。启动不会仅因回放达到阈值就主动刷盘，下一次写入会检查阈值。

重复覆盖少量 key 时，内存可能很小而 WAL 历史很长；当前阈值控制待刷盘数据大小，不单独限制 WAL 大小，可以手动 Flush 回收历史。

## 改动和阅读顺序

| 文件 | 职责 |
| --- | --- |
| `store.h` / `store.cpp` | 协调读写、刷盘、清单发布、WAL 轮换和恢复 |
| `manifest.h` / `manifest.cpp` | 描述和编解码有效文件集合，不负责执行发布 |
| `src/file_io.h` / `src/file_io.cpp` | 文件描述符临时所有权、短读写、同步、锁和删除 |
| `src/coding.h` | 从已有 codec 提取共享小端整数和 CRC，新增 u64 支持 |
| `wal_record.cpp` / `sstable.cpp` | 使用共享编码函数，原来的磁盘格式不变 |
| `main.cpp` | 新增 flush 命令，其余命令继续接受相同基础路径 |
| `manifest_test.cpp` / `store_flush_test.cpp` | 清单校验、集成和中断测试 |
| `CMakeLists.txt` | 新增两个实现源文件和两个测试目标 |

没有修改 MemTable 的实现或 VS Code 配置。建议先看 Store::Get、Flush、构造函数，再看 PublishManifest 和 CleanupObsoleteFiles，最后看底层辅助文件。不要一开始逐行读全部系统调用。

## 必要的 C++ 写法

- `unique_ptr<SSTableReader>` 独占拥有一个 reader；Store 销毁时自动关闭对应输入流。Reader 不可复制/移动，vector 保存的是可以移动的 unique_ptr。
- `std::move(next)` 把准备好的清单交给 Store，`std::move(reader)` 转移 reader 的所有权，不复制文件。发布前预留 vector 空间，减少发布后再分配失败的机会。
- `rbegin/rend` 从 vector 尾部向前迭代，恰好按新到旧查询。
- `OwnedFd` 为刷盘中途的临时描述符提供 RAII；Release 把所有权交给 Store，析构时不再关闭已交出的描述符。
- `friend StoreTestAccess` 只让测试设置私有阶段回调，用于确定性强杀/抛异常。正常使用没有回调，也没有环境变量触发的故障入口。

## 运行和观察

在项目目录：

```sh
cmake --build build --target kvstore
./build/kvstore build/step5-demo.wal put name Alice
./build/kvstore build/step5-demo.wal flush
./build/kvstore build/step5-demo.wal get name
```

首次使用该基础路径，依次输出 `OK`、`OK (1 SSTables)`、`Alice`。flush 后没有 pending 数据，Get 的值来自 SSTable。已有示例数据时 SSTable 数量可能不同。

```sh
./build/kvstore build/step5-demo.wal delete name
./build/kvstore build/step5-demo.wal flush
./build/kvstore build/step5-demo.wal get name
```

最后输出 `<not found>`，删除标记阻止旧值复活。可以用 `ls build/step5-demo.wal*` 观察清单、锁、SSTable 和当前 WAL；数据文件会保留在 build 下。

本步新增测试：

```sh
cmake --build build --target manifest_test store_flush_test
./build/manifest_test
./build/store_flush_test
```

分别输出 `All 5 manifest tests passed.` 和 `All 17 Store flush tests passed.`。

## 验证范围

清单的 5 个测试覆盖初始/64 位编号/顺序往返、独立 Python 字节样本、全部截断前缀与单 bit 损坏、CRC 正确但编号非法、超大数量与尾部字节。

Store 刷盘的 17 个测试函数覆盖：手动刷盘和重开、多文件覆盖和删除、删除后重插、空值和二进制、空刷盘与纯删除表、默认/自定义/关闭阈值、大小记账、孤立文件清理、有效文件缺失或损坏、轮换 WAL 的尾部恢复、稳定锁、旧 WAL 引导、相对路径打开后切换工作目录。

其中故障测试进一步执行：

- 第一次和后续刷盘，在 8 个协议边界分别 SIGKILL，共 16 次确定性强杀。检查可见清单选择、旧 WAL 的保留/回收时机、恢复后的更新/删除及继续刷盘。
- 在同样 8 个边界抛异常，检查当前 Store 停止服务，重开后恢复正确。
- 子进程设置文件大小上限，分别制造真实的半个 SSTable 和半个临时 manifest，检查正式清单及旧 WAL 保留，再重开清理并重试。

普通构建与带编译警告、AddressSanitizer、UndefinedBehaviorSanitizer 的构建均通过六组 CTest，共 71 个测试函数。另用独立 CLI 进程执行手动示例和 90 次固定种子的混合写入/删除，穿插 flush 与查询，对照内存模型并验证只剩当前 WAL。

这不是实际断电、内核崩溃或穷举所有 I/O 错误；注入覆盖已列出的协议边界。当前仍顺序查询每个 SSTable，并保持每个有效文件的 reader；文件数量和启动校验成本会增长，下一步 compaction 处理合并与旧文件回收。后台线程、索引/Bloom/cache、Raft 和事务均未在本步实现。

第 5 步完成后停下来讨论，明确 OK 后才开始第 6 步同步 compaction。
