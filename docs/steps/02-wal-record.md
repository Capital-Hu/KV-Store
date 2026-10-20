# 第 2 步：把操作变成可以保存的字节

> 路径说明：本文记录本步骤完成时的文件位置。后续目录已按模块整理，当前路径见[第 16a 步](16a-project-layout.md)；构建与运行命令以[项目 README](../../README.zh-CN.md)为准。

## 先理解这一步的目的

MemTable 只保存当前数据，进程退出后数据就丢失。WAL（write-ahead log，预写日志）准备通过记录每次修改，帮助后续重启时重新执行这些操作。

但写文件前，需要先约定“一次操作在文件中长什么样”。本步只实现这个字节格式和转换过程：

```text
Put("name", "Alice") → WalRecord → EncodeWalRecord → 一段字节
一段字节 → DecodeWalRecord → WalRecord 或失败状态
```

本轮没有写文件，也没有把 WAL 接到 MemTable。真正的“先写日志、再修改内存”和重启回放属于第 3 步。

## 改了什么，影响哪里

| 文件 | 变化与原因 |
| --- | --- |
| `include/kvstore/wal_record.h` | 新增记录、结果类型及两个编解码接口，定义调用约定 |
| `src/wal_record.cpp` | 新增明确的字节编码、边界检查和 CRC32 校验 |
| `tests/wal_record_test.cpp` | 新增 12 个测试函数，验证格式与异常输入 |
| `CMakeLists.txt` | 把实现加入现有库，登记 `wal_record_test` 程序和 CTest 测试 |
| `README.md`、`GOAL.md`、`ROADMAP.md` | 补运行方式、验证结果和步骤状态 |

MemTable、演示入口、编辑器配置的内容没有变化。现有演示程序继续展示内存新增、覆盖和删除。

## 记录格式

每条记录依次包含：

| 字段 | 字节数 | 为什么需要 |
| --- | --- | --- |
| `KVW1` | 4 | 标识这是我们的 WAL 第 1 版记录 |
| 操作类型 | 1 | `1` 是 Put，`2` 是 Delete，区分删除与写入空值 |
| key 长度 | 4 | 知道接下来多少字节属于 key |
| value 长度 | 4 | 知道接下来多少字节属于 value |
| key 内容 | 可变 | 原始字节 |
| value 内容 | 可变 | 原始字节；Delete 必须为空 |
| CRC32 | 4 | 检查前面所有字段与内容的校验值 |

固定开销是 17 字节。Put("k", "v") 一共 19 字节，测试中有一份独立计算的预期字节样本，避免编码和解码同时写错却仍然能互相转换。

两个长度和 CRC32 都用 32 位小端格式：低位字节在前。例如长度 1 写成十六进制 `01 00 00 00`。格式显式指定字节顺序，不依赖当前机器如何排列整数。

不直接把 C++ struct 的内存写出去，因为对象可能包含填充，而且 string 对象的内存不是它所表示的完整字节内容。编码器逐个写入字段。

这里的长度是字节数，不是人类看到的字符数。`std::string` 可保存 `\0` 等二进制字节，所以不能依靠分隔符或 C 风格字符串终止符确定边界。

key + value 限制为最多 1 MiB，先检查长度再创建解码结果，避免损坏的长度字段让程序申请极大内存。当前 MemTable 尚未限制大小，接入 WAL 时需要统一写入接口的大小约定。

CRC32 使用 IEEE CRC-32 的算法，覆盖整个头部和 key/value；它检测常见字节损坏，但不是绝对无碰撞。算法参考 [RFC 1952 第 8 节](https://www.rfc-editor.org/rfc/rfc1952.html#section-8)，我们没有引入压缩或第三方运行库依赖。

## 如何使用和判断结果

```cpp
kvstore::WalRecord record{kvstore::WalRecordType::Put, "name", "Alice"};
std::string bytes = kvstore::EncodeWalRecord(record);
kvstore::WalDecodeResult result = kvstore::DecodeWalRecord(bytes);
```

编码时，未知操作类型、携带 value 的 Delete 会抛出 `std::invalid_argument`；超过大小限制会抛出 `std::length_error`。这些表示调用者提供的操作不符合约定。

解码面对的是外部字节，用返回状态表达数据问题：

| 状态 | 含义 | 返回内容 |
| --- | --- | --- |
| `Ok` | 第一条记录完整、合法、校验通过 | 记录及它占用的字节数 |
| `Incomplete` | 输入还不够组成第一条完整记录 | 无记录，消耗字节数为 0 |
| `Corrupt` | 已能确认标识、类型、长度规则或校验错误 | 无记录，消耗字节数为 0 |

一次只解码第一条记录。后面有更多记录时，用 `bytes_consumed` 跳过第一条，再解码剩余部分。这为后续顺序回放准备接口。

`Incomplete` 只说明字节不足，不能证明一定是一次正常的尾部截断：损坏的长度字段也可能让它表现成数据不足。空输入同样返回 Incomplete，是否已经正常读到文件末尾由后续文件读取层判断。第 3 步需要明确整个文件的恢复策略，不能在此步直接宣称崩溃恢复已完成。

## 这次的新 C++ 写法

建议先看 `wal_record.h` 的数据结构与接口，再看 `.cpp` 中的 Encode/Decode；字节辅助函数和 CRC 的细节可以最后读。

- `struct WalRecord`：把类型、key、value 放在一个对象中；C++ struct 的成员默认是 public，与 class 默认 private 不同。这里是简单数据对象，没有额外方法。
- `enum class WalRecordType`：限定操作名称，并提供独立类型；使用时写 `WalRecordType::Put`。后面的 `: std::uint8_t` 指定底层整数为 8 位。
- `std::uint32_t`：固定宽度的 32 位无符号整数，适合定义字节格式。Java 的 `int` 也是 32 位，但有符号；C++ 普通 `int` 的宽度不用于定义我们的文件格式。
- `static_cast<...>`：显式类型转换，例如把已检查的长度转成 32 位整数，或把操作类型转成一个字节。
- `std::string_view`：只读地查看已有字节，不复制也不拥有它们。没有 Java 对象引用那样的垃圾回收生命周期保证，因此使用期间原字节必须仍然有效。解码结果里的 key/value 是独立 string 副本，不依赖原缓冲区继续存在。
- `constexpr`：这里声明编译时可确定的常量，例如 1 MiB 大小上限。
- `.cpp` 内没有名字的 `namespace { ... }`：把辅助函数限制在这个源文件中，避免作为项目公开接口。
- `>>`、`<<`、`&`、`|`、`^`：这里用于整数的移位和位运算。AppendU32 把整数拆成字节，ReadU32 将字节拼回整数。CRC 使用无符号数右移，左侧补零；不要把这里的 `<<` 和输出流的重载混为一谈。

## 怎么运行与验证

在项目目录运行这两行：

```sh
cmake --build build --target wal_record_test
./build/wal_record_test
```

成功输出 `All 12 WAL record tests passed.`。现有构建目录会检测 CMakeLists 的变化并重新生成规则，不需要为这个新增目标另外手写编译路径。

本轮使用 CMake 4.4.4 和 Apple Clang 21，普通构建成功，原有 MemTable 测试及 WAL 测试均通过。额外启用 `-Wall -Wextra -Wpedantic`、AddressSanitizer、UndefinedBehaviorSanitizer 的独立构建也通过，无警告或检查错误报告。

测试覆盖 Put/Delete 往返、空 key/value、二进制字节、独立字节样本、连续记录、所有有效前缀的截断、key/value 与校验字段逐字节翻转一位、非法头部、大小边界、非法编码输入，以及解码结果拥有独立数据。

CTest 中登记的是两个测试程序；程序内分别运行 8 和 12 个测试函数，所以 CTest 显示 2 个测试通过。

## 下一步讨论

第 3 步需要选择文件写入接口、定义何时同步落盘、设计启动回放，以及处理最后一条不完整记录和已确认损坏的记录。用户理解本步并明确 OK 后再开始。
