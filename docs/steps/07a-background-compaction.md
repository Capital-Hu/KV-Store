# 第 7a 步：后台 compaction 与并发访问

本记录描述 7a 交付时的实现；当前已完成后台刷盘，变化见 [第 7b 步](07b-background-flush.md)。

本轮只完成第 7 步的前半部分：后台合并 SSTable。刷盘仍然同步，后台刷盘留到 7b；它还需要冻结的 MemTable 和多个待恢复 WAL，适合单独理解。

## 原来与现在

第 6 步的 `Compact()` 在调用线程里读取全部输入、写出结果、发布清单、删除旧文件。Store 只支持单线程使用。

现在每个 Store 有一个后台线程。调用方可以请求它工作，然后继续读写；多个调用线程也可以访问同一个 Store。前台操作用一把互斥锁串行执行，后台最耗时的合并过程不持有这把锁。

这不表示所有操作都同时进行：前台 Get 与 Put 之间仍互相等待，Flush 仍同步占锁，后台发布和回收文件时也占锁。改进的是合并扫描、输出和输出校验期间，前台可以继续工作。尚未优化锁竞争或提供性能结论。

## 新接口与触发方式

| 接口 | 调用方什么时候返回 |
| --- | --- |
| `RequestCompaction()` | 请求排队后返回，不等合并完成 |
| `WaitForBackgroundWork()` | 本次调用之前已经排队的请求完成，或发生故障 |
| `Compact()` | 为本次调用排队一个请求，并等待它完成 |
| `Flush()` | 同步刷盘完成；可能顺带排队后台合并，不等待该合并 |

`Compact()` 保持原有“返回时合并已完成”的用法，因此 CLI 的 `compact` 命令不需要改变。

构造函数增加第四个可选参数：

```cpp
kvstore::Store store(path, kvstore::WalRecoveryMode::Strict,
                     4 * 1024 * 1024, 4);
```

第三个参数仍是自动刷盘阈值。第四个参数是自动合并的 SSTable 数量阈值：每次成功 Flush 后，如果文件数达到该值，就排队一个请求；默认 4，设为 0 关闭自动合并，手动请求仍然可用。打开 Store 时不会仅因为已有文件多就自动请求。

长时间运行的程序可以这样使用：

```cpp
store.RequestCompaction();
store.Put("name", "Bob");
auto value = store.Get("name");
store.WaitForBackgroundWork();
```

这里不保证 Put 与后台恰好同时执行，只保证它们允许交错且结果正确。测试会主动暂停后台，确定验证这个交错。

## 一把锁保护什么

`mutex_` 保护 MemTable、WAL 写入与轮换、manifest、前台 SSTable reader 集合，以及后台请求/完成状态。

写操作必须把“写入并同步 WAL → 更新 MemTable → 必要时 Flush”作为一段受锁保护的操作，避免两个写线程的 WAL 顺序和内存结果不一致。Delete 查询旧状态与写删除标记也在同一把锁里完成。

Get 也持锁：SSTableReader 有可变的文件游标，同时读取同一个 reader 会互相干扰；发布时还会关闭和回收旧 reader。持锁查询保证切换发生时，没有前台查询仍使用旧 reader。后台合并自己打开独立 reader，不使用这些前台游标。

`GetLocked()`、`FlushLocked()` 是已经持锁时调用的内部版本。例如 Delete 调用 `GetLocked()`，避免再次调用会加锁的公开 Get。普通 `std::mutex` 不支持同一个线程重复加锁。

## 后台工作的三个阶段

```text
短暂持锁：选中当前全部 SSTable，预留输出文件编号
    ↓ 解锁
不持锁：独立 reader 合并 → 写出 SSTable → 同步 → 打开并校验输出
    ↓ 加锁
持锁：基于最新 manifest 发布 → 切换 reader → 回收输入文件
```

目前只有一个后台线程，因此不会有两个合并任务同时删除对方的输入。前台 Flush 只追加 SSTable，不会删除正在被合并的 SSTable。

### 为什么要预留编号

后台输出和前台 Flush 使用同一个文件编号分配器。后台在释放锁之前先增加 `next_file_id`，前台才能跳过它已经选中的输出编号。

预留本身只修改内存；不额外发布清单。如果进程此时退出且没有任何后续发布，启动会删除未被正式清单引用的输出，之后可以重新分配这个编号。如果前台已经 Flush，那个 Flush 会把增加后的计数一并发布。编号出现空缺是允许的。

### 发布时为什么要重新读取 manifest

假设后台开始时：

```text
SSTable [1, 3]，WAL 4，next 5
```

后台选中 `[1, 3]`，预留输出 5；前台在它合并时 Flush，分配 SSTable 6 和 WAL 7：

```text
当前 SSTable [1, 3, 6]，WAL 7，next 8
```

后台发布的正确结果是：

```text
SSTable [5, 6]，WAL 7，next 8
```

5 替代原来的 `[1, 3]`，6 比这些输入更新，必须继续保留。后台只替换选中输入构成的最旧前缀，其余文件顺序不变。它复制当前 manifest 再替换前缀，因此也保留前台刚切换的 WAL 和编号计数。

如果直接用合并开始时的旧 manifest 发布，就会丢掉 6、错误切回 WAL 4；WAL 4 可能已经回收，导致重启失败。测试专门覆盖这个边界。

### 为什么仍能清理输入中的删除标记

选中的前缀包含合并开始时全部磁盘历史，输入之外不会出现更旧的 SSTable。后来追加的 SSTable、MemTable 和 WAL 都更新。因此输入中最新状态为删除的 key，其更旧磁盘值也一起消失，输出无需保留该删除标记。

这不是任意部分合并都能使用的规则；独立 `MergeSSTables()` 默认仍保留删除标记。当前也没有历史快照或事务读。

## 如何等待、处理错误与退出

后台没有循环 sleep 检查工作，而是使用条件变量。

`requested_` 表示已经排队的请求编号，`completed_` 表示已完成的编号。后台醒来时捕获当时最新的请求编号，一次合并可以完成之前累积的多个请求。合并期间到达的新请求会触发下一轮，避免漏掉新刷出的文件。

`WaitForBackgroundWork()` 捕获调用时的请求编号，等待完成编号达到它。其他线程在此之后追加的请求不延长这次等待。`Compact()` 则等待自己排队的那一个请求。没有 SSTable 时，任务直接完成，不创建文件。

后台异常通过 `std::exception_ptr` 保存；等待接口重新抛出异常。之后的读写、刷盘和合并请求也报错，要求关闭后重新打开。发布阶段出错时，必须在释放状态锁前标记失败，防止磁盘清单已经替换、前台却继续操作旧的内存状态。

析构时先通知线程停止接收后续工作，已经排队的工作仍完成；随后 `join()` 等待后台退出，最后才关闭 WAL 和文件锁。join 期间不能持有状态锁，否则后台无法拿到它完成收尾。析构不会抛出后台错误；需要确认成功时，在析构前调用等待接口。

所有调用方线程必须先结束对 Store 的访问，再销毁 Store。线程安全并不意味着可以一边释放对象、一边调用它的方法。退出也不自动 Flush，剩余 MemTable 仍由 WAL 恢复。

## 本次涉及的 C++ 写法

| 写法 | 在这里的含义 | Java 类比 |
| --- | --- | --- |
| `std::thread(&Store::BackgroundLoop, this)` | 新线程在当前 Store 上执行成员函数；`this` 指向该对象 | 创建并启动执行某个任务的 Thread |
| `std::lock_guard<std::mutex> lock(mutex_)` | 构造时加锁，离开作用域自动解锁，抛异常也解锁 | synchronized 代码块，或 try/finally unlock |
| `std::unique_lock<std::mutex>` | 可暂时解锁再加锁，供条件变量等待使用 | ReentrantLock 搭配 Condition |
| `condition.wait(lock, predicate)` | 条件不成立就释放锁并休眠，醒来重新拿锁并检查 | while 循环检查条件再 await |
| `[this, target] { ... }` | lambda 捕获 this 指针和 target 的值，作为等待条件 | 使用外部变量的 lambda；C++ 需要明确捕获方式 |
| `std::exception_ptr` | 保存跨线程的异常，再在等待线程抛出 | Future.get 等接口传递后台异常的用途 |
| `worker_.join()` | 等后台线程结束，保证它不再使用 Store 的成员 | Thread.join |

`condition_variable` 可以无通知地醒来，因此必须用谓词反复检查。等待会释放关联的锁，返回前重新获取锁，见 [C++ 标准草案](https://eel.is/c++draft/thread.condition.condvar)。`std::thread` 在销毁前必须完成 join 或 detach；这里使用 join 保证后台不会访问已经销毁的 Store，见 [线程成员规则](https://eel.is/c++draft/thread.thread.member)。

## 文件变化

| 文件 | 修改原因 |
| --- | --- |
| `include/kvstore/store.h` | 声明异步/等待接口、线程、锁、条件变量和请求状态 |
| `src/store.cpp` | 前台入口加锁，拆出已持锁的辅助方法，Flush 排队合并，构造启动/析构等待线程 |
| `src/background_compaction.cpp`，新增 | 单独放调度、等待和后台合并交接逻辑，仍是 Store 成员函数实现 |
| `CMakeLists.txt` | 链接线程支持、加入新源文件和测试目标 |
| `tests/background_test.cpp`，新增 | 验证可控交错、并发访问、请求与退出规则 |
| `tests/compaction_test.cpp` | hook 设置加锁，调整后台强杀测试，并增加前台 Flush 后的恢复验证 |
| README、GOAL、ROADMAP、本记录 | 更新进度、运行方式及解释 |

新 `.cpp` 使用现有 `store.h`，没有增加对外类型。`Threads::Threads` 让 CMake 根据平台决定线程编译/链接参数，见 [CMake 官方说明](https://cmake.org/cmake/help/latest/module/FindThreads.html)。直接编译 Store 需要包含新 `.cpp` 并带上 `-pthread`，README 已更新。

磁盘格式不变，已有 Store 可以直接打开。本轮没有修改 VS Code 配置，也没有引入第三方线程库。

## 如何运行与验证

在项目根目录编译运行本次测试：

```sh
cmake --build build --target background_test
./build/background_test
```

成功输出 `All 9 background tests passed.`，覆盖：

1. 后台暂停时前台 Get/Put/Delete 可执行，pending MemTable/WAL 保留且重启恢复。
2. 后台暂停时两次 Flush，输出编号不冲突，新 SSTable 顺序、当前 WAL 和编号计数保留。
3. 合并过程中追加请求，后续一轮确实合并新文件。
4. 关闭自动合并、阈值 2、默认值对应的阈值 4。
5. Put 触发自动 Flush 后再触发后台合并。
6. 输出、清单替换和文件回收三个阶段的后台异常传播及重开。
7. 三个并发调用方进行读写、删除、Flush 和 Compact，最终 60 个 key 重开后符合预期。
8. 析构等待正在执行的任务及排队的下一轮任务。
9. 空请求、无工作等待、空闲线程退出与 pending WAL 恢复。

compaction 测试增加第 16 个函数：在后台暂停时，前台 Flush 并再次写入新 WAL；然后在七个合并边界分别强杀子进程，确认前台已确认的数据及当前 WAL 不会丢失。

验证结果：普通构建与 AddressSanitizer/UndefinedBehaviorSanitizer 构建八组 CTest 全部通过，共 100 个测试函数。单独用 ThreadSanitizer 运行九个后台测试，通过且无数据竞争报告；带 fork 的故障测试不放入这项线程检查。没有进行真实断电实验或性能测量。

第 7b 步仍未开始：将 MemTable 刷盘放到后台，并设计冻结表、读路径、多 WAL 恢复和资源上限。先讨论本次的锁、交接和线程生命周期，再继续。
