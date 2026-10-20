# 本机三节点混合负载实测

每行是独立的新集群；延迟为全部调用的 nearest-rank 分位数，吞吐量仅计 Ok。

测量方法、构建和解释见[第 19 步](../steps/19-benchmark.md)，保留更多精度的总统计及各操作 P99 见[CSV 快照](step19-local.csv)。以下保留全部 12 轮，没有从结果中挑选最好轮次；原始样本与节点数据位于环境信息中的 storage_directory。

| 负载 | 并发 | 轮次 | 退出码 | 预热非 Ok | Ok/请求 | Unknown/Unavailable/Rejected | Ok ops/s | P50 ms | P95 ms | P99 ms |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| read95 | 1 | 1 | 0 | 0 | 1000/1000 | 0/0/0 | 61.49 | 15.978 | 20.350 | 24.808 |
| read95 | 1 | 2 | 0 | 0 | 1000/1000 | 0/0/0 | 58.45 | 16.613 | 23.083 | 26.282 |
| read95 | 1 | 3 | 0 | 0 | 1000/1000 | 0/0/0 | 55.87 | 17.332 | 24.306 | 29.102 |
| read95 | 4 | 1 | 0 | 0 | 1000/1000 | 0/0/0 | 93.39 | 41.885 | 60.243 | 74.584 |
| read95 | 4 | 2 | 0 | 0 | 1000/1000 | 0/0/0 | 90.88 | 42.300 | 63.430 | 84.005 |
| read95 | 4 | 3 | 0 | 0 | 1000/1000 | 0/0/0 | 93.72 | 41.788 | 56.620 | 65.943 |
| mixed | 1 | 1 | 0 | 0 | 1000/1000 | 0/0/0 | 49.11 | 19.948 | 27.554 | 32.051 |
| mixed | 1 | 2 | 0 | 0 | 1000/1000 | 0/0/0 | 51.20 | 19.104 | 26.069 | 29.593 |
| mixed | 1 | 3 | 0 | 0 | 1000/1000 | 0/0/0 | 46.25 | 21.320 | 29.098 | 33.179 |
| mixed | 4 | 1 | 0 | 0 | 1000/1000 | 0/0/0 | 65.87 | 59.089 | 84.962 | 96.078 |
| mixed | 4 | 2 | 0 | 0 | 1000/1000 | 0/0/0 | 67.75 | 57.444 | 80.510 | 94.657 |
| mixed | 4 | 3 | 0 | 0 | 1000/1000 | 0/0/0 | 68.53 | 57.787 | 76.615 | 88.617 |

## 环境与复现参数

```json
{
  "utc": "2026-10-20T05:00:22.449772+00:00",
  "os": "macOS-27.0.1-arm64-arm-64bit",
  "architecture": "arm64",
  "logical_cpus": 8,
  "cpu": "Apple M3",
  "memory_bytes": "17179869184",
  "python": "3.9.6",
  "compiler": "Apple clang version 21.0.0 (clang-2100.3.34.2)\nTarget: arm64-apple-darwin27.0.0\nThread model: posix\nInstalledDir: /Library/Developer/CommandLineTools/usr/bin",
  "cmake": "cmake version 4.4.4\n\nCMake suite maintained and supported by Kitware (kitware.com/cmake).",
  "build_directory": "/Users/huxiangyu/Documents/CV/replicated-kv-store/build/release",
  "build_type": "Release",
  "cxx_flags": "-Wall -Wextra -Wpedantic -O3 -DNDEBUG",
  "dependencies": {
    "grpc": "1.84.0",
    "protobuf": "35.1.0"
  },
  "source_commit": "d5cd4b50a5bfcf3d110644852abc5e6d2c66098d",
  "worktree_dirty": true,
  "source_code_sha256": "56f794aa99b1a524c77e54ba6b778b6b3f7cbd5848bc4733b42b902b752c71f7",
  "binary_sha256": {
    "kvbench": "80de2c7be778348f2db83da4b2ff4a944dffe72e1bc68d7b756ed7f469e1dc66",
    "kvnode": "e063544e3f2218183349330f3552edc124d9ac683ea7360f843bda26fd7d728e",
    "kvadmin": "06016d17b8f56487f2cf625c9614c90140c066287e5aaf53abf954e50778597e"
  },
  "storage_directory": "/Users/huxiangyu/Documents/CV/replicated-kv-store/build/benchmark-results/step19-local",
  "storage_model": "OS filesystem; disk model and cache state uncontrolled",
  "runner_command": [
    "/Library/Developer/CommandLineTools/usr/bin/python3",
    "/Users/huxiangyu/Documents/CV/replicated-kv-store/scripts/benchmark.py",
    "--build-dir",
    "build/release",
    "--output",
    "build/benchmark-results/step19-local"
  ],
  "semantics": "Client Get with fresh NoOp; writes commit and apply; no write replay after Unknown",
  "load_model": "closed_loop; each worker has one outstanding request; no fixed offered rate",
  "purpose": "performance",
  "parameters": {
    "build_dir": "/Users/huxiangyu/Documents/CV/replicated-kv-store/build/release",
    "output": "/Users/huxiangyu/Documents/CV/replicated-kv-store/build/benchmark-results/step19-local",
    "operations": 1000,
    "warmup": 128,
    "keys": 256,
    "value_bytes": 256,
    "seed": 4242,
    "timeout_ms": 5000,
    "repetitions": 3,
    "run_timeout": 300,
    "threads": [
      1,
      4
    ],
    "profile": "all",
    "allow_non_release": false
  }
}
```

## 解释范围

read95 = 95% Get / 5% Put；mixed = 50% Get / 40% Put / 10% Delete。
预装、预热、Client 构造和请求计划生成不计入正式时间；总耗时从统一放行到最后一个工作线程完成。
底层连接若在正式调用期间才建立，其耗时仍包含在该调用中；warmup=0 时尤其需要注意。
闭环请求会在变慢时减少发出速率，不代表固定到达率下的排队延迟。每次 Get 仍追加并提交 NoOp。
所有节点共享本机 CPU、磁盘与回环网络。操作顺序受调度影响，固定 seed 只固定请求计划。
默认工作集落在 MemTable，不能据此评价 SSTable/Bloom/cache 或长期合并性能。日志容量有限，每轮全新目录。
逐次 JSON、samples.csv、节点日志及数据保存在本次输出目录；不把不同轮次的分位数再取平均。
