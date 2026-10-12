# 第 3 步：WAL 文件写入与启动回放

## 这一步做成了什么

第 2 步只能把操作转换成字节。现在增加 Store，把编解码器、真实 WAL 文件和 MemTable 连接起来。

```text
Put/Delete → 编码 → 追加 WAL → 同步文件 → 更新 MemTable → 返回成功
Get        → 读取 MemTable
启动       → 打开并锁定 WAL → 顺序解码并回放 → 同步文件及父目录 → 接受操作
```

先同步日志，才能在进程突然退出、内存丢失后恢复已成功返回的写入。每次操作单独同步，当前优先保证顺序清晰；批量同步与吞吐量优化留到后面讨论。

## 改了什么，为什么

| 文件 | 变化与原因 |
| --- | --- |
| `include/kvstore/store.h` | 新增持久化接口、恢复模式、文件所有权规则 |
| `src/store.cpp` | 新增文件打开/关闭、锁、同步、回放和失败处理 |
| `tests/store_test.cpp` | 新增 12 个测试，包含子进程强杀和部分写入故障 |
| `src/main.cpp` | 从固定内存演示改成 put/get/delete 命令，方便跨进程验证持久化 |
| `CMakeLists.txt` | 库加入 Store 实现，登记 `store_test` 目标与测试超时 |
| `README.md`、`GOAL.md`、`ROADMAP.md` | 更新用法、验收和本轮状态 |

已有的 MemTable 和 WAL 编解码实现没有改动，`.vscode` 没有变化。源代码注释保持英文，文档保持中文。

## 建议的阅读顺序

先看 Store 的 Put/Get/Delete，理解写入顺序；再看构造函数和 Replay；最后看系统调用辅助函数与故障测试。

Put 主要是：

```cpp
const auto bytes = EncodeWalRecord({WalRecordType::Put, key, value});
AppendAndSync(bytes);
table_.Put(key, value);
```

编码错误，例如长度超限，发生在写文件前，Store 仍可继续使用。

追加、同步或更新内存失败时，Store 被标记为不可用，后续读写都会报错，要求关闭后重新打开。原因是日志里可能已留下部分记录，或者完整记录已写入而内存还没更新。不能继续把缓存中的数据当成可信状态。

一次操作没有成功返回，不代表它一定不存在于 WAL。重开时可能恢复出这个操作；这是故障后的不确定结果，不能把失败承诺为“完全没有执行”。

Delete 同样记录到 WAL，包括删除不存在的 key；成功同步后再调用 MemTable::Delete，返回是否实际移除了内存中的条目。Store 的 key + value 遵循已有 1 MiB 限制。

## 文件如何写入与同步

使用文件描述符 `int fd_` 表示当前打开的 WAL。它是操作系统分配的资源编号，不是文件内容或内存指针。

`open` 通过 `O_APPEND` 让每次写入发生在末尾。`write` 不保证一次写完所有字节，因此 AppendAndSync 用循环累计已写长度；被信号中断时重试，其他错误上报。没有进展的写入也视为失败。

所有字节写完后调用 `fsync`。macOS 上进一步调用 `fcntl(..., F_FULLFSYNC)`，请求刷新设备写缓存；这是 Apple 为更严格存储顺序提供的接口，参考 [Apple fsync 手册](https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man2/fsync.2.html)。打开时也同步 WAL 的父目录，覆盖新文件目录项的同步需求。

成功返回表示这些同步调用和内存更新都成功。实际保证仍依赖文件系统和存储设备兑现同步请求。本轮测试验证进程终止，不等于完成了操作系统崩溃或真实断电测试。

构造函数使用 `flock(LOCK_EX | LOCK_NB)` 获取独占、非阻塞的文件锁；第二个 Store 打开同一 WAL 会失败。它是协作式锁，不能阻止外部程序直接修改文件，参考 [Apple flock 手册](https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man2/flock.2.html)。当前 Store 不支持多线程共享，也不应在打开期间从外部替换或编辑 WAL。

## 启动时怎么恢复

Replay 每次读最多 64 KiB，追加到 pending 缓冲区，避免一次把整个 WAL 放进内存。

1. 解码 pending 中的第一条记录。
2. 完整则按操作类型更新 MemTable，累计有效字节数，再检查下一条。
3. 数据不足则保留尚未处理的字节，继续读取文件。
4. 已确认损坏立即报错，保留原文件。
5. 文件结束时，按恢复模式处理剩余字节。

已处理的字节从 pending 中移除；单条记录大小有上限，因此缓冲区大小受记录上限和读取块大小约束。结果的 key/value 是独立字符串副本，移除 pending 不影响 MemTable。

回放顺序决定最终状态。例如 `Put(name, Alice) → Put(name, Bob) → Delete(name)` 恢复后 name 不存在。回放只操作 MemTable，不调用 Store::Put/Delete，避免把回放过的操作重新写进 WAL。

## 尾部与损坏处理规则

默认 `WalRecoveryMode::Strict`：只要文件末尾仍有不能组成完整记录的字节，就报错；已经存在的 WAL 内容不会修改。空文件可以正常打开。

显式选择 `TruncateIncompleteTail`：保留完整、合法记录的前缀，使用 ftruncate 去掉剩余尾部，随后同步文件。`truncated_tail_bytes()` 报告截去多少字节。继续追加前必须先截尾，否则新的完整记录会接在半条记录后面。

不完整可能来自中断的追加，也可能来自损坏的长度；目前的格式无法完全区分。该模式表示调用者明确接受丢弃从不完整记录起的剩余内容，不是自动判断哪些数据一定可以丢弃。

两种模式都拒绝已确认的格式或 CRC 损坏，不自动跳过、不尝试扫描后面的记录。

## 新的 C++ 与系统接口写法

- `explicit Store(...)`：构造函数创建并恢复一个 Store；explicit 阻止字符串被隐式转换成 Store。
- `~Store()`：析构函数，离开作用域时关闭文件，释放锁。这里体现 RAII：对象生命周期管理资源，类似 Java try-with-resources 的使用目标，但 C++ 有确定的作用域析构。
- `Store(const Store&) = delete` 等声明：禁止复制/移动 Store，避免两个对象都认为自己拥有同一个文件描述符。这里的 `= delete` 是禁止某个函数，和删除 key 的 Delete 方法无关。
- 构造函数失败时不会调用 Store 自己的析构函数，因此 catch 中显式关闭已经打开的 fd；已完成构造的成员仍会自动清理。
- `::open`、`::read`、`::write`：开头的 `::` 表示调用全局命名空间里的系统函数。
- `errno` 保存系统调用失败原因；`EINTR` 表示被信号中断。`std::system_error` 将错误码和上下文作为异常传出。
- `catch (...)` 捕获任何异常，执行资源清理或标记状态；里面单独的 `throw;` 继续抛出原异常。
- `#ifdef __APPLE__`：只在编译 macOS 版本时加入 F_FULLFSYNC 调用。当前文件接口面向 macOS/Linux，只有 macOS 已实测。
- `off_t` 表示文件位置，`ssize_t` 一类的系统调用返回值可以为 -1；确认非负后才转换成表示长度的无符号类型。
- main 的 `argc` 是参数个数，`argv` 是参数字符串数组。相比 Java main(String[] args)，这里的 argv[0] 还包含程序名，所以 WAL 路径是 argv[1]，操作名是 argv[2]。

## 自己运行

在项目目录构建程序：

```sh
cmake --build build --target kvstore
```

分别启动两个进程：

```sh
./build/kvstore build/demo.wal put name Alice
./build/kvstore build/demo.wal get name
```

先输出 OK，再输出 Alice。第二次启动没有前一次进程的内存，读取结果来自 WAL 回放。

测试用：

```sh
cmake --build build --target store_test
./build/store_test
```

成功输出 `All 12 Store tests passed.`。CLI 的 `recover-tail` 用法与退出码见 README；Get 未找到会输出 `<not found>` 并返回 1。

## 验证及边界

使用本机 CMake 4.4.4 和 Apple Clang 21 构建，三组测试共 32 个函数通过。新增测试覆盖正常重开和覆盖/删除顺序、空字符串/二进制、跨读取块的大记录、参数错误不写日志、不完整尾部的两种处理、已确认损坏不改文件、独占锁及释放、非法路径、SIGKILL 后恢复，以及通过子进程文件大小限制制造的真实部分写入失败。

SIGKILL 测试在子进程成功完成写入和删除后，通过管道通知父进程；父进程强杀仍打开 Store 的子进程，再重新打开并检查结果。测试没有依赖正常析构来刷日志。

部分写入故障测试只限制子进程的文件大小，使系统实际写入四字节后失败，检查 Store 拒绝继续读写，再验证显式截尾。测试未穷举所有系统 I/O 错误，也未模拟同步调用本身失败。

额外启用编译警告、AddressSanitizer 和 UndefinedBehaviorSanitizer 的独立构建也通过三组测试，无警告或检查错误报告。CLI 的跨进程读写、删除、严格模式保留文件、显式截尾恢复和退出码另行验证通过。

当前没有日志轮换、SSTable、后台线程或网络复制，启动时间会随着 WAL 增大而增加。下一步讨论 SSTable 的最小格式和点查，收到用户明确 OK 后再实现。
