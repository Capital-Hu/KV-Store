# 第 19 步：可复现的三节点混合负载 benchmark

这一步新增性能测量入口，继续调用现有 Client。Store、Raft、gRPC 服务、proto 和持久化规则保持原实现。测试对象是同一台机器上的三个真实 kvnode 进程，以及一个持续运行的 C++ 压测进程。

## 先看测量的路径

```text
Python 脚本启动三个 kvnode 和一个 kvbench
                              ↓ 每个工作线程
                         Client.Get/Put/Delete
                              ↓
                       Inspect 发现 Leader
                              ↓
                  业务 RPC、Raft 复制与持久化
                              ↓
                   确认提交和执行，返回调用结果
```

Python 只负责选端口、启动/关闭进程和保存报告，不参与每次请求的计时。kvbench 中的 C++ 线程复用 Client 和 gRPC 连接，不为每个请求启动一次 kvclient。

Get 使用第 17 步的正式一致性接口，每次追加并提交一条 NoOp；Put/Delete 等到提交并在返回节点执行后才计为 Ok。测量包含发现 Leader 的 Inspect 调用和这条完整路径，不能当作 MemTable 查询速度或直接访问已知 Leader 的 RPC 性能。

## 一轮怎么运行

1. 创建全新的独立输出目录，为三个节点选择不同回环端口。
2. 启动三个节点，等待全部能观察状态且存在一个 Leader。
3. 创建所有 Client、key、value 和固定的请求计划。
4. 逐个 Put 预装数据，要求每次确认成功。
5. 执行预热请求，使用之后正式测量的 Client。
6. 新一组工作线程准备好后统一放行，执行固定数量的正式请求。
7. 所有请求结束后计算统计、保存逐条延迟，再等待节点应用位置收敛。
8. 停止自己启动的节点，保留节点日志和数据，开始下一个全新集群。

第 1—5 步以及第 7—8 步不计入正式阶段的总时间。预热改变的数据保留到正式阶段，所以混合负载可能成功读到“不存在”；这种结果不是请求失败。

Client 构造在计时外，但 gRPC 可能延迟到首次调用才建连接。默认预热会让每个 Client 访问节点；若设置 warmup=0，正式调用中发生的连接建立耗时仍会计入。

## 默认参数

| 参数 | 默认值 |
| --- | --- |
| 负载 read95 | 95% Get / 5% Put |
| 负载 mixed | 50% Get / 40% Put / 10% Delete |
| 并发数 | 分别运行 1、4 个 Client |
| 重复次数 | 每种组合 3 次，共 12 轮；顺序执行 |
| 每轮预装 | 256 个不同 key |
| 每轮预热 / 正式请求数 | 128 / 1000 |
| key / value | 16 字节 key，256 字节 value；所有 Put 使用相同大小和值内容 |
| 访问分布 | 在有限 key 范围内用固定种子生成近似均匀访问，不包含热点模型 |
| seed | 4242；预热使用另外派生的固定种子 |
| 客户端总期限 / 单次业务 RPC 上限 | 都为 5000ms；服务端等待仍是默认 1000ms |
| 节点配置 | 原默认：tick 10ms、心跳 100ms、选举 600—1000ms、Store 自动刷盘 4 MiB |

Get 和 Delete 数量分别向下取整，剩余请求为 Put，因此总数始终准确。生成计划后打乱，key 与操作类型都在计时前确定。每个线程负责间隔固定的计划位置，例如并发为 4 时，线程 0 负责 0、4、8……。相同种子固定请求内容和线程分配；线程的实际交付顺序、Leader 身份与延迟仍可能不同。JSON 中记录 plan_hash，便于核对计划相同。

这是闭环负载：一个 Client 收到上次结果后才发下一个请求，每个线程至多一个请求在途。服务变慢时，发送速率会跟着下降。它测量给定并发下的完成速率和调用延迟，不代表固定到达速率下所有排队请求的延迟，也没有做 coordinated omission 校正。

## 吞吐量与 P99 怎样计算

使用单调时钟 steady_clock，避免系统校时影响经过时间。

- 单次延迟：紧挨着 Client 调用前后取时间差，包括内部发现和安全重试。
- 总时间：统一放行的时间点，到最后一个工作线程完成。包含线程唤醒、循环开销及调用之间的间隔，不包含线程创建、统计、CSV 或 JSON 写出。
- attempted ops/s：正式请求总数 / 总时间。
- Ok ops/s：正式阶段成功请求数 / 同一个总时间。
- P50/P95/P99：把延迟排序，取第 ceil(N × 百分比 / 100) 条，称为 nearest rank。

例如有 1000 条请求，P99 就是从小到大第 990 条的延迟，并非最慢一条。分位数没有请求时输出 null，不能写成 0ms。

JSON 同时记录全部调用和仅 Ok 调用的分位数，并分别统计 Get/Put/Delete。Unknown、Unavailable、Rejected 不会从全部调用的延迟中消失；Get 的 Ok + 不存在仍是成功。逐条 CSV 按请求计划位置保存，不代表全局调用时间顺序，也不是线性一致性历史。

脚本再从 CSV 独立计算数量、成功/失败、命中/缺失、吞吐量和分位数，与 C++ JSON 对照。每轮都单列，不把各轮的 P99 再取平均。预热或正式请求出现非 Ok 时保留结果，但命令以非零状态退出；预装失败则中止本轮，不把不完整预装当成成功基线。

## 如何构建与运行

先有 README 中构建好的 build/deps/install。性能构建单独放在 build/release，不改变既有构建和编辑器配置：

```sh
cmake -S . -B build/release -DCMAKE_BUILD_TYPE=Release -DKVSTORE_BUILD_GRPC=ON -DKVSTORE_BUILD_BENCHMARKS=ON -DBUILD_TESTING=ON -DCMAKE_PREFIX_PATH="$PWD/build/deps/install"
cmake --build build/release
```

运行完整默认矩阵，需要 Python 3.9 或更新版本，只使用标准库：

```sh
python3 scripts/benchmark.py --build-dir build/release
```

不需要手动启动节点。命令会打印每轮的成功吞吐量和 P99，最后打印报告位置，默认保存在 build/benchmark-results/ 下的独立时间目录。会拒绝覆盖已有输出目录。正式记录时让其他编译、测试和耗资源程序先结束。

只运行一个较短的组合：

```sh
python3 scripts/benchmark.py --build-dir build/release --profile mixed --threads 4 --repetitions 1 --operations 200
```

可以用 --keys、--value-bytes、--warmup、--seed、--timeout-ms 调整参数。kvbench --help 列出 C++ 入口参数；通常直接使用脚本即可。单独启动 kvbench 会向指定集群预装并修改 key- 开头的数据，应使用专用测试集群。

## 输出文件与代码入口

| 文件 | 内容 |
| --- | --- |
| 输出目录/report.md | 每轮结果表、环境、参数和解释范围 |
| 输出目录/summary.csv | 每轮总分位数、各操作 P99、结果计数和吞吐量 |
| 输出目录/environment.json | CPU/内存/系统、编译器、构建参数、依赖、源码指纹和程序 SHA-256 |
| 输出目录/results.json | 全部轮次的完整结果、命令和前后节点状态 |
| 每轮目录/samples.csv | 正式请求的操作、结果、延迟纳秒和 Get 是否命中 |
| 每轮目录/result.json、case.json | C++ 原始汇总和脚本补充信息 |
| 每轮目录/nodeN.log、nodeN/ | 节点输出和独立数据；保留以便检查 |
| benchmarks/workload.h | 参数上限、固定请求计划、统计与 JSON 字符串转义 |
| benchmarks/kvbench.cpp | 参数入口、预装/预热、并发调用与计时 |
| scripts/benchmark.py | 三进程生命周期、环境与样本核对、报告 |
| tests/benchmark/ | 分位数/失败统计测试和真实进程小规模验证 |

CMake 增加默认关闭的 KVSTORE_BUILD_BENCHMARKS 开关，需要同时启用 gRPC；benchmarks/CMakeLists.txt 仍属于同一个根项目。启用测试时增加 benchmark_workload 和 benchmark_smoke 两组。smoke 只验证流程与统计，不是性能记录；--allow-non-release 也只用于这类检查，正式脚本默认要求 Release 且不能带 sanitizer。

## 几个 C++ 细节

- steady_clock::time_point 是时刻；两个时刻相减得到 duration，再用 duration_cast 转为纳秒。类似 Java 的 System.nanoTime() 差值。
- C++17 没有 std::latch，这里用 mutex + condition_variable 等所有线程就绪。等待会释放锁，放行后请求循环不持有这把锁。
- 工作线程写不同的 Sample 元素；数组提前分配且不再扩容，最后 join 完成后主线程才读取并统计。
- 工作线程不能让异常直接越过线程入口，否则会终止进程；exception_ptr 保存异常，join 后主线程再 rethrow_exception。
- mt19937_64 生成固定序列，明确写出的整数取模和洗牌规则使计划可重建；取模有极小偏差，所以这里不声称严格无偏采样。

## 自动化回归

| 构建 | 本轮验证 |
| --- | --- |
| Release，启用 gRPC 和 benchmark | 25/25 组通过，132.43 秒 |
| ASan/UBSan，启用 gRPC 和 benchmark | 25/25 组通过，136.97 秒 |
| 默认非网络构建 | 构建通过，仍注册原有 19 组；这些用例已包含在上述完整回归中 |

只验证本步流程，或运行完整回归：

```sh
ctest --test-dir build/release -R '^benchmark_' --output-on-failure
ctest --test-dir build/release --output-on-failure
```

原有 23 组网络构建用例全部通过，新增两组也通过。benchmark_workload 验证已知分位数、失败不被隐藏、缺失读取计数、计划数量/种子与参数预算；benchmark_smoke 运行真实小集群，核对原始样本、预热/正式分离、节点收敛、输出保护和非法参数提前拒绝。构建日志无编译警告或错误。

两套完整回归在正式性能测量前结束；小规模 pilot 和 smoke 数据不放入正式结果表。以上秒数是自动化测试耗时，不是 benchmark 延迟。第三方依赖沿用已有 Release 构建，ASan/UBSan 主要检查项目代码；未新增 ThreadSanitizer 验证。

## 实测与验证范围

本轮使用 Apple M3、16 GiB 内存、8 个逻辑 CPU、macOS arm64，项目为 Release（-O3 -DNDEBUG），依赖为 gRPC 1.84.0 / Protobuf 35.1.0。全部 12 轮及完整环境见[实测快照](../benchmarks/step19-local.md)，保留更多精度的汇总及各操作 P99 见[CSV](../benchmarks/step19-local.csv)。原始 JSON、逐条 samples.csv、节点日志与数据保存在 build/benchmark-results/step19-local/。

下表是每个组合三次实测的最小—最大值，表示本轮观察范围，不是置信区间，也没有把分位数求平均：

| 负载 | 并发 | 成功 ops/s | P50 ms | P95 ms | P99 ms |
| --- | --- | --- | --- | --- | --- |
| 95% Get / 5% Put | 1 | 55.87—61.49 | 15.978—17.332 | 20.350—24.306 | 24.808—29.102 |
| 95% Get / 5% Put | 4 | 90.88—93.72 | 41.788—42.300 | 56.620—63.430 | 65.943—84.005 |
| 50% Get / 40% Put / 10% Delete | 1 | 46.25—51.20 | 19.104—21.320 | 26.069—29.098 | 29.593—33.179 |
| 50% Get / 40% Put / 10% Delete | 4 | 65.87—68.53 | 57.444—59.089 | 76.615—84.962 | 88.617—96.078 |

12,000 次正式调用全部 Ok，Unknown/Unavailable/Rejected 均为 0；3072 次预装和 1536 次预热也全部确认成功。每轮测量后应用位置收敛，退出码为 0；36 个节点目录均没有生成 SSTable，符合默认小工作集范围。脚本逐轮核对了原始延迟与结果统计，相同负载在不同轮次和并发下的请求计划 hash 一致。

本轮并发 4 的整体成功吞吐量高于并发 1，同时 P99 也更高。没有用这些数字直接定位磁盘、锁或日志复制中谁是瓶颈；这需要另外做 profiling 或有控制变量的实验。

默认 256 × 256 字节的工作集较小，运行结束还会记录实际 SSTable 文件数量。本步主要测量 MemTable 工作集下的同步持久化、复制和客户端完整路径，没有建立大数据集、冷热缓存、长期稳定合并或固定到达速率的性能结论。CPU、磁盘和网络都由本机进程共享；磁盘型号、操作系统缓存、温度和外部应用负载没有控制。

Raft 日志尚不截断，因此每轮必须使用新集群；参数按最坏操作大小限制在 48 MiB 保守预算以内，给 64 MiB 逻辑日志上限留余量。这个预算不是磁盘占用或进程内存上限。默认轮次短，1000 条请求只能提供初步尾延迟样本，不能据此声称稳定的生产容量或解释性能瓶颈。

工程验收完成后仍要讨论读写路径、恢复、提交和取舍；GOAL.md 中用户架构理解验收不会自动勾选。
