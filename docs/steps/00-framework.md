# 第 0 步：看懂项目骨架

## 这次改了什么，为什么加

| 文件 | 作用 |
| --- | --- |
| `src/main.cpp` | 可执行程序的启动入口，当前只打印两行提示 |
| `CMakeLists.txt` | 告诉构建工具用哪个源文件、编译成什么程序、需要哪个 C++ 标准 |
| `.gitignore` | 将来使用 Git 时忽略构建产物 |
| `README.md` | 项目入口和运行方式 |
| `GOAL.md` | 固定两部分目标、验收项、用户确认后再推进的规则 |
| `ROADMAP.md` | 拆解第一阶段，标明每步做什么和验证什么 |
| 本文件 | 保存这一轮解释，方便回看 |

当前只有一个源文件，因为程序还没有存储功能。等第 1 步需要 MemTable 时，再解释如何拆分头文件、实现文件和调用代码。

## 程序怎么工作

```text
src/main.cpp → 编译与链接 → build/kvstore → 运行 main() → 输出两行文字 → 返回 0
```

CMake 配置构建规则；真正把 C++ 源码转成可执行程序的是编译器及链接器。可以先把 CMake 理解为与 Java 中 Maven/Gradle 的构建配置有相似职责的工具，不过它不是 JVM，也不是运行时。

`cmake -S . -B build` 读取当前目录的配置，在 `build` 中生成构建文件。`cmake --build build` 根据它们构建程序。`./build/kvstore` 才是启动程序。

## 用 Java 经验理解这几行 C++

```cpp
#include <iostream>

int main() {
    std::cout << "Replicated KV Store: framework ready.\n";
    std::cout << "Storage operations are not implemented yet.\n";
    return 0;
}
```

- `#include <iostream>`：在编译前引入标准库头文件的内容，使这里能使用输出流。它与 Java 的 `import` 使用方式不同：`import` 主要让类名可以被简写，`#include` 则参与文本层面的预处理。
- `int main()`：程序入口，可以直接写在类外。Java 常见入口是类里的 `public static void main(String[] args)`。这里返回 `int`，供操作系统判断退出状态。
- `std::cout`：标准输出流，作用类似 `System.out`。`std` 是标准库命名空间，`::` 表示从这个命名空间中取名字，不是 Java 的对象成员访问。
- `<<`：这里是向输出流写入内容的运算符。相同符号在整数表达式里也可以表示左移，含义由操作数类型决定。
- `\n`：换行字符，和 Java 字符串里的写法一致。
- `return 0`：表示程序成功退出。

`CMakeLists.txt` 中：

- `project(... LANGUAGES CXX)`：声明这是 C++ 项目；`CXX` 是 CMake 对 C++ 的名称。
- `add_executable(kvstore src/main.cpp)`：把这个源文件构建成名为 `kvstore` 的可执行程序。
- `target_compile_features(... cxx_std_17)`：要求至少支持 C++17，后面可以用 `std::optional` 表示“查询结果可能不存在”。
- `CXX_EXTENSIONS OFF`：使用标准 C++ 模式，减少对某个编译器私有扩展的依赖。
- `PRIVATE`：这里把标准要求用于当前目标；库的依赖传播规则等出现库目标时再解释。

## 这一轮如何验证

当前环境找到了 Apple Clang 编译器，没有找到 CMake。用 C++17 模式和警告选项直接编译当前唯一源文件，运行程序并核对输出及退出状态。CMake 配置的实际运行验证仍待本机具备 CMake 后完成。

本次没有存储逻辑，不添加空壳测试。下一步内存读写开始后，会用针对功能语义的测试验证正确性。

## 下一步讨论点

建议从 `Put(key, value)`、`Get(key)`、`Delete(key)` 三个操作开始。先明确：覆盖同名 key 如何处理、查询不存在和查询到空字符串如何区分、删除不存在的 key 如何处理。讨论并明确 OK 后才实现。
