# 第 1 步：内存中的 Put/Get/Delete

> 路径说明：本文记录本步骤完成时的文件位置。后续目录已按模块整理，当前路径见[第 16a 步](16a-project-layout.md)；构建与运行命令以[项目 README](../../README.zh-CN.md)为准。

本步由用户明确同意后开始。范围是单线程内存读写，完成后停下来讨论；WAL 仍是下一步。

## 改了哪些文件，为什么这样拆

| 文件 | 本次变化 | 原因 |
| --- | --- | --- |
| `include/kvstore/memtable.h` | 新增 MemTable 类声明 | 告诉调用者有哪些操作、参数和返回类型 |
| `src/memtable.cpp` | 实现三个操作 | 把存储行为放在独立模块里 |
| `src/main.cpp` | 改成新增、覆盖、删除的演示 | 展示真实调用流程，入口不用承担存储实现 |
| `tests/memtable_test.cpp` | 新增 8 个测试函数 | 验证正常行为与边界情况 |
| `CMakeLists.txt` | 新增存储库和测试目标 | 演示程序与测试复用同一份实现 |
| `README.md`、`GOAL.md`、`ROADMAP.md` | 更新运行方式与完成状态 | 保持入口文档和实际代码一致 |

当前结构：

```text
main.cpp ──────────┐
                  ├── 使用 MemTable 接口 → memtable.cpp → std::map
memtable_test.cpp ─┘
```

两者会分别构建成演示程序和测试程序，不会把两个 `main()` 链接进同一个程序。

## 接口约定

| 操作 | 行为 | 返回 |
| --- | --- | --- |
| `Put(key, value)` | 新增 key；已有 key 则覆盖 | `void` |
| `Get(key)` | 查找 key，读取不改变表 | 有值的 `optional<string>`，或 `nullopt` |
| `Delete(key)` | 移除已有 key；不存在也允许调用 | 实际移除返回 `true`，否则 `false` |

key/value 都用 `std::string`，都允许为空。空字符串值是有效数据，不能用它表示“没找到”。重复删除后数据状态不变，但第二次返回 `false`。

Get 返回字符串的副本。调用者拿到结果后，即使表中的值被覆盖或删除，这份结果仍然有效。代价是复制字符串；当前优先选择容易理解、生命周期明确的接口。

## 一次读写怎么走

```text
table.Put("name", "Alice") → entries_ 保存 name → Alice
table.Get("name")         → find 找到条目 → 复制 Alice 到返回结果
table.Put("name", "Bob")   → 同一个 key 的值改为 Bob
table.Delete("name")      → erase 移除条目
table.Get("name")         → find 没找到 → 返回 nullopt
```

底层使用 `std::map<string, string>`，可以类比 Java 的 `TreeMap<String, String>`：它按 key 排序，查找、插入和删除具有 O(log n) 复杂度。后续写 SSTable 时需要有序数据，所以先选择它；当前没有对外提供遍历接口。

`Put` 中的 `entries_[key] = value` 会在 key 不存在时插入，存在时覆盖。`Get` 使用 `find`，因为用 `[]` 读取一个不存在的 key 会插入空值，破坏“读取不修改数据”的约定。

当前删除是从内存 map 中真正移除条目。接入磁盘旧数据后，删除必须改成保存删除标记，否则旧值可能重新被读到；这项改动在多层数据读取前讨论并实现。

## 这一轮需要理解的 C++ 写法

### 头文件与源文件

`.h` 放类的声明，`.cpp` 放函数实现。C++ 调用者编译时需要看到声明，链接时需要找到实现。Java 通常在同一个 `.java` 类文件里写声明和方法体。

`#pragma once` 防止同一个头文件在一次源文件编译中被重复包含。

`namespace kvstore` 把项目里的名字归在一起，调用时写 `kvstore::MemTable`。它有类似 Java 包的名字隔离作用，但机制不同。

`public:` 和 `private:` 分别控制其后成员的访问权限，直到下一个访问标签。Java 通常在每个成员前写修饰符。C++ 类定义的最后还有一个分号：`};`。

### 对象与参数

```cpp
kvstore::MemTable table;
void Put(const std::string& key, const std::string& value);
```

第一行直接创建一个局部对象，不需要 Java 风格的 `new`。离开作用域时，它的析构函数会自动执行，里面的 map 和 string 会清理各自持有的资源；这里不需要手动释放内存。

参数里的 `&` 表示引用：参数绑定到调用者提供的字符串对象，避免为了传参先复制一份。`const` 表示函数不能通过这个参数修改字符串。Java 传对象引用的方式与 C++ 引用不同，不能把两者完全等同。

这里只在函数执行期间使用参数。Put 会把 key/value 复制进 map，不会保存指向调用者字符串的引用。

### optional 与 const 成员函数

```cpp
std::optional<std::string> Get(const std::string& key) const;
```

`std::optional<std::string>` 表示“可能有一个字符串，也可能没有”，可以类比 Java 的 `Optional<String>`。`std::nullopt` 表示没有值；`has_value()` 检查是否有值。

末尾的 `const` 修饰成员函数，表示这个操作不会修改对象的普通成员状态。因此 Get 可用于只读的 MemTable。它与参数前面的 `const` 管的是两个不同位置。

演示中的 `.value_or("<not found>")` 在结果不存在时提供打印用的默认文字。`<not found>` 不是内部的特殊值，存储本身仍用 optional 区分存在与否。

### auto、迭代器和 map 条目

```cpp
const auto it = entries_.find(key);
if (it == entries_.end()) {
    return std::nullopt;
}
return it->second;
```

`auto` 让编译器从右边推导类型，仍然是静态类型，不是动态类型。这里推导出一个 map 的只读迭代器。

迭代器表示容器里的位置。`end()` 是尾后位置，用来表示没找到，不可以访问它指向的内容。

map 的每个条目是一对 key/value，分别叫 `first` 和 `second`。`it->second` 访问找到的条目的 value；`->` 在这里可理解为先通过迭代器找到条目，再访问成员。

## 构建发生了什么变化

`kvstore_engine` 是新加入的静态库目标，里面只有 `memtable.cpp`。演示程序 `kvstore` 和测试程序 `memtable_test` 都链接这个库，避免各自维护一份存储实现。

头文件目录和 C++17 要求声明为 `PUBLIC`：库自己需要它们，使用该库的两个程序也需要，因为公开头文件使用了 C++17 的 optional。`target_link_libraries(... PRIVATE kvstore_engine)` 则声明各程序自己的库依赖。

`include(CTest)` 加入 CMake 的测试支持，默认启用 `BUILD_TESTING`。`add_test` 把测试程序登记到 CTest；构建程序不会自动运行测试，仍需要运行测试命令。

直接编译命令中的 `-I include` 告诉编译器到 `include` 目录里找头文件；编译时同时提供 `memtable.cpp`，才能链接到那三个函数的实现。

## 验证结果

使用本机 Apple Clang 21，以 C++17 和 `-Wall -Wextra -Wpedantic` 编译，演示与测试程序均编译通过，没有编译警告。

演示输出：

```text
After Put: Alice
After overwrite: Bob
After Delete: <not found>
```

8 个测试覆盖：未找到与删除不存在、新增与不同 key、覆盖、空字符串值、删除与重写、空 key、查询结果副本的有效性、不同实例互不影响。测试失败会报错并返回非零退出码，不依赖可能被构建选项关闭的 `assert`。

测试程序额外启用了 AddressSanitizer 和 UndefinedBehaviorSanitizer，运行输出 `All 8 MemTable tests passed.`，未报告内存错误或未定义行为。这些检查只覆盖实际执行过的代码路径，不代表后续功能已经正确。

本机仍未找到 CMake，所以本轮通过直接编译验证源码；CMake/CTest 配置的实际运行尚未验证。

## 当前边界与下一步

数据只在进程内存中，退出后消失。没有磁盘写入、并发访问保证或网络通信；整个 LSM 引擎仍未完成。

第 2 步先讨论 WAL 的记录格式：如何区分 Put/Delete，如何保存长度，以及如何识别只写了一半或损坏的记录。收到用户明确 OK 后再开始。
