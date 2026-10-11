# Replicated Key-Value Store

一个逐步实现、逐步讲解的 C++ 学习项目。先完成单机 LSM 存储引擎，再完成三节点 Raft 复制。两部分都是最终验收要求。

当前只完成 **第 0 步：项目骨架**。程序可以启动并输出提示，还不能保存或查询数据。

## 从哪里看

- [GOAL.md](GOAL.md)：最终目标、完成条件、推进规则。
- [ROADMAP.md](ROADMAP.md)：第一阶段的拆分，以及第二阶段的方向。
- [第 0 步讲解](docs/steps/00-framework.md)：本次每个文件的作用、程序如何运行、C++ 与 Java 的区别。
- `src/main.cpp`：当前唯一的 C++ 源文件。
- `CMakeLists.txt`：构建入口。

现在不预建 Raft、gRPC 或缓存模块；到对应步骤再引入文件和依赖。后续代码只在这个子文件夹中开发。

## 构建和运行

需要支持 C++17 的编译器，以及 CMake 3.16 或以上版本。在这个项目目录运行：

```sh
cmake -S . -B build
cmake --build build
./build/kvstore
```

如果本机还没有 CMake，可以先用编译器验证这个单文件骨架：

```sh
mkdir -p build
c++ -std=c++17 -Wall -Wextra -Wpedantic src/main.cpp -o build/kvstore
./build/kvstore
```

预期输出：

```text
Replicated KV Store: framework ready.
Storage operations are not implemented yet.
```

编译输出都放在 `build/`。以上直接编译方式只适用于当前骨架；后续有多个模块时使用 CMake。

## 协作节奏

每次只做一个已讨论清楚的小步骤，说明改动、原因、数据流、必要的 C++ 知识，并验证结果。你说 OK、完成讨论后，才进入下一步；提问本身不代表同意继续开发。
