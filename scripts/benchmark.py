#!/usr/bin/env python3
"""Run isolated, sequential odd-sized Raft benchmarks using only the standard library."""

import argparse
import csv
import datetime
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import re
import socket
import subprocess
import sys
import time


ROOT = Path(__file__).resolve().parents[1]
PROFILES = {"read95": (95, 0), "mixed": (50, 10)}


def command_output(arguments, cwd=None):
    try:
        result = subprocess.run(arguments, cwd=cwd, text=True, capture_output=True, timeout=10)
        return result.stdout.strip() if result.returncode == 0 else "unavailable"
    except (OSError, subprocess.TimeoutExpired):
        return "unavailable"


def cache_values(build):
    values = {}
    for line in (build / "CMakeCache.txt").read_text().splitlines():
        match = re.match(r"([^#/:][^:]*):[^=]+=(.*)", line)
        if match:
            values[match[1]] = match[2]
    return values


def environment(build, output, cache):
    compiler = cache.get("CMAKE_CXX_COMPILER", "c++")
    flags = " ".join(cache.get(key, "") for key in ("CMAKE_CXX_FLAGS", "CMAKE_CXX_FLAGS_RELEASE"))
    source_hash = hashlib.sha256()
    source_names = command_output(["git", "ls-files", "--cached", "--others", "--exclude-standard"], ROOT)
    for name in sorted(source_names.splitlines()):
        source = ROOT / name
        if source.is_file() and (source.suffix in (".h", ".cpp", ".proto", ".py", ".sh") or source.name == "CMakeLists.txt"):
            source_hash.update(name.encode() + b"\0" + source.read_bytes() + b"\0")
    versions = {}
    for prefix in cache.get("CMAKE_PREFIX_PATH", "").split(";"):
        for package, relative in (("grpc", "grpc/gRPCConfigVersion.cmake"), ("protobuf", "protobuf/protobuf-config-version.cmake")):
            candidate = Path(prefix) / "lib/cmake" / relative
            if candidate.is_file():
                match = re.search(r'set\(PACKAGE_VERSION "([^"]+)"\)', candidate.read_text())
                if match:
                    versions[package] = match[1]
    return {
        "utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "os": platform.platform(), "architecture": platform.machine(), "logical_cpus": os.cpu_count(),
        "cpu": command_output(["sysctl", "-n", "machdep.cpu.brand_string"]),
        "memory_bytes": command_output(["sysctl", "-n", "hw.memsize"]),
        "python": platform.python_version(), "compiler": command_output([compiler, "--version"]),
        "cmake": command_output(["cmake", "--version"]), "build_directory": str(build),
        "build_type": cache.get("CMAKE_BUILD_TYPE", ""), "cxx_flags": flags.strip(),
        "dependencies": versions, "source_commit": command_output(["git", "rev-parse", "HEAD"], ROOT),
        "worktree_dirty": bool(command_output(["git", "status", "--porcelain"], ROOT)),
        "source_code_sha256": source_hash.hexdigest(),
        "binary_sha256": {name: hashlib.sha256((build / name).read_bytes()).hexdigest() for name in ("kvbench", "kvnode", "kvadmin")},
        "storage_directory": str(output), "storage_model": "OS filesystem; disk model and cache state uncontrolled",
        "runner_command": [sys.executable, str(Path(__file__).resolve()), *sys.argv[1:]],
        "semantics": "Client Get with fresh NoOp; writes commit and apply; no write replay after Unknown",
        "load_model": "closed_loop; each worker has one outstanding request; no fixed offered rate",
    }


def selected_addresses(count):
    reservations = []
    try:
        for _ in range(count):
            reservation = socket.socket()
            reservations.append(reservation)
            reservation.bind(("127.0.0.1", 0))
        return ["127.0.0.1:" + str(item.getsockname()[1]) for item in reservations]
    finally:
        for reservation in reservations:
            reservation.close()


def snapshot(build, addresses):
    statuses = []
    for address in addresses:
        result = subprocess.run([str(build / "kvadmin"), address, "status"], text=True, capture_output=True, timeout=3)
        if result.returncode != 0:
            return None
        fields = dict(item.split("=", 1) for item in result.stdout.split())
        statuses.append({key: value if key == "role" else int(value) for key, value in fields.items()})
    return statuses


def await_status(build, addresses, children, converged=False):
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        if any(child.poll() is not None for child in children):
            raise RuntimeError("A benchmark node exited; inspect the case's node logs")
        statuses = snapshot(build, addresses)
        if statuses and sum(item["role"] == "LEADER" for item in statuses) == 1:
            if not converged or len({item["commit"] for item in statuses}) == 1 and all(item["applied"] == item["commit"] for item in statuses):
                return statuses
        time.sleep(0.05)
    raise RuntimeError("Cluster readiness/convergence timed out")


def verify_samples(result, sample_path):
    """Recompute accounting from every raw latency, independently of C++ JSON output."""
    with sample_path.open(newline="") as source:
        rows = list(csv.DictReader(source))
    parameters = result["parameters"]
    if len(rows) != parameters["operations"] or [int(row["index"]) for row in rows] != list(range(len(rows))):
        raise RuntimeError("Missing, duplicated or unordered sample indices")
    expected_counts = {"get": len(rows) * parameters["read_percent"] // 100,
                       "delete": len(rows) * parameters["delete_percent"] // 100}
    expected_counts["put"] = len(rows) - sum(expected_counts.values())
    if any(sum(row["operation"] == kind for row in rows) != count for kind, count in expected_counts.items()):
        raise RuntimeError("Raw workload mix differs from the requested mix")
    for row in rows:
        if row["outcome"] not in ("ok", "unknown", "unavailable", "rejected") or int(row["latency_ns"]) < 0:
            raise RuntimeError("Invalid sample outcome or latency")
    measurement = result["measurement"]
    for kind in (None, "get", "put", "delete"):
        selected = rows if kind is None else [row for row in rows if row["operation"] == kind]
        stats = measurement["total"] if kind is None else measurement["by_operation"][kind]
        if stats["attempted"] != len(selected):
            raise RuntimeError("Attempt count mismatch")
        for outcome in ("ok", "unknown", "unavailable", "rejected"):
            if stats[outcome] != sum(row["outcome"] == outcome for row in selected):
                raise RuntimeError("Outcome count mismatch")
        for field, found in (("read_found", "1"), ("read_missing", "0")):
            if stats[field] != sum(row["operation"] == "get" and row["outcome"] == "ok" and row["found"] == found for row in selected):
                raise RuntimeError("Read hit/miss count mismatch")
        for scope in ("all", "ok"):
            values = sorted(int(row["latency_ns"]) / 1e6 for row in selected if scope == "all" or row["outcome"] == "ok")
            for percent in (50, 95, 99):
                expected = values[math.ceil(len(values) * percent / 100) - 1] if values else None
                actual = stats["latency_ms_" + scope]["p" + str(percent)]
                if (expected is None) != (actual is None) or expected is not None and not math.isclose(expected, actual, rel_tol=1e-10, abs_tol=1e-6):
                    raise RuntimeError("Raw samples disagree with latency percentiles")
        for name, count in (("attempted", len(selected)), ("ok", stats["ok"])):
            if not math.isclose(stats[name + "_ops_per_second"], count / measurement["seconds"], rel_tol=1e-10, abs_tol=1e-6):
                raise RuntimeError("Throughput denominator mismatch")


def run_case(args, build, output, profile, threads, repeat):
    case = output / f"{profile}-c{threads}-r{repeat}"
    case.mkdir()
    addresses = selected_addresses(args.nodes)
    members = ",".join(f"{index + 1}={address}" for index, address in enumerate(addresses))
    children, logs = [], []
    statuses_before = statuses_after = None
    try:
        for index in range(args.nodes):
            log = (case / f"node{index + 1}.log").open("x")
            logs.append(log)
            children.append(subprocess.Popen([str(build / "kvnode"), "--id", str(index + 1), "--directory",
                str(case / f"node{index + 1}"), "--mode", "new", "--members", members], stdout=log, stderr=subprocess.STDOUT))
        statuses_before = await_status(build, addresses, children)
        reads, deletes = PROFILES[profile]
        command = [str(build / "kvbench"), "--members", members, "--operations", str(args.operations),
            "--warmup", str(args.warmup), "--keys", str(args.keys), "--value-bytes", str(args.value_bytes),
            "--threads", str(threads), "--read-percent", str(reads), "--delete-percent", str(deletes),
            "--seed", str(args.seed), "--timeout-ms", str(args.timeout_ms), "--samples", str(case / "samples.csv")]
        with (case / "result.json").open("x") as result_file, (case / "benchmark.log").open("x") as log:
            completed = subprocess.run(command, stdout=result_file, stderr=log, timeout=args.run_timeout)
        if completed.returncode not in (0, 3):
            raise RuntimeError(f"kvbench failed ({completed.returncode}): {(case / 'benchmark.log').read_text()}")
        result = json.loads((case / "result.json").read_text())
        verify_samples(result, case / "samples.csv")
        statuses_after = await_status(build, addresses, children, converged=completed.returncode == 0)
        record = {"profile": profile, "threads": threads, "repeat": repeat, "directory": case.name,
                  "command": command, "returncode": completed.returncode, "result": result,
                  "status_before": statuses_before, "status_after": statuses_after}
    finally:
        for child in children:
            if child.poll() is None:
                child.terminate()
        for child in children:
            try:
                child.wait(timeout=5)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait()
        for log in logs:
            log.close()
    if any(child.returncode != 0 for child in children):
        raise RuntimeError("Benchmark node did not shut down cleanly; inspect node logs")
    record["files_after_shutdown"] = [
        {"node": index + 1, "sstable_files": len(list((case / f"node{index + 1}").glob("data.sst.*"))),
         "raft_bytes": (case / f"node{index + 1}" / "raft").stat().st_size} for index in range(args.nodes)]
    (case / "case.json").write_text(json.dumps(record, indent=2) + "\n")
    return record


def write_reports(output, metadata, records):
    (output / "results.json").write_text(json.dumps({"environment": metadata, "runs": records}, indent=2) + "\n")
    fields = ["profile", "threads", "repeat", "exit_code", "warmup_non_ok", "attempted", "ok", "unknown", "unavailable", "rejected", "seconds",
              "ok_ops_per_second", "p50_ms", "p95_ms", "p99_ms", "get_p99_ms", "put_p99_ms", "delete_p99_ms", "read_missing"]
    with (output / "summary.csv").open("w", newline="") as destination:
        writer = csv.DictWriter(destination, fields)
        writer.writeheader()
        for record in records:
            measured = record["result"]["measurement"]
            total = measured["total"]
            row = {name: record[name] for name in ("profile", "threads", "repeat")}
            warmup = record["result"]["warmup"]["total"]
            row.update(exit_code=record["returncode"], warmup_non_ok=warmup["attempted"] - warmup["ok"])
            row.update({name: total[name] for name in ("attempted", "ok", "unknown", "unavailable", "rejected", "ok_ops_per_second", "read_missing")})
            row["seconds"] = measured["seconds"]
            row.update({f"p{percent}_ms": total["latency_ms_all"][f"p{percent}"] for percent in (50, 95, 99)})
            row.update({f"{kind}_p99_ms": measured["by_operation"][kind]["latency_ms_all"]["p99"] for kind in ("get", "put", "delete")})
            writer.writerow(row)
    lines = [f"# 本机 {metadata['parameters']['nodes']} 节点混合负载实测", "", "每行是独立的新集群；延迟为全部调用的 nearest-rank 分位数，吞吐量仅计 Ok。", "",
             "| 负载 | 并发 | 轮次 | 退出码 | 预热非 Ok | Ok/请求 | Unknown/Unavailable/Rejected | Ok ops/s | P50 ms | P95 ms | P99 ms |",
             "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |"]
    for record in records:
        total = record["result"]["measurement"]["total"]
        latency = total["latency_ms_all"]
        warmup = record["result"]["warmup"]["total"]
        lines.append(f"| {record['profile']} | {record['threads']} | {record['repeat']} | {record['returncode']} | "
                     f"{warmup['attempted'] - warmup['ok']} | {total['ok']}/{total['attempted']} | "
                     f"{total['unknown']}/{total['unavailable']}/{total['rejected']} | {total['ok_ops_per_second']:.2f} | "
                     f"{latency['p50']:.3f} | {latency['p95']:.3f} | {latency['p99']:.3f} |")
    lines.extend(["", "## 环境与复现参数", "", "```json", json.dumps(metadata, indent=2, ensure_ascii=False), "```", "",
                  "## 解释范围", "", "read95 = 95% Get / 5% Put；mixed = 50% Get / 40% Put / 10% Delete。",
                  "预装、预热、Client 构造和请求计划生成不计入正式时间；总耗时从统一放行到最后一个工作线程完成。",
                  "底层连接若在正式调用期间才建立，其耗时仍包含在该调用中；warmup=0 时尤其需要注意。",
                  "闭环请求会在变慢时减少发出速率，不代表固定到达率下的排队延迟。每次 Get 仍追加并提交 NoOp。",
                  "所有节点共享本机 CPU、磁盘与回环网络。操作顺序受调度影响，固定 seed 只固定请求计划。",
                  "默认工作集落在 MemTable，不能据此评价 SSTable/Bloom/cache 或长期合并性能。日志容量有限，每轮全新目录。",
                  "逐次 JSON、samples.csv、节点日志及数据保存在本次输出目录；不把不同轮次的分位数再取平均。", ""])
    (output / "report.md").write_text("\n".join(lines))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build-dir", type=Path, default=ROOT / "build/release")
    parser.add_argument("--output", type=Path)
    for name, default in (("nodes", 3), ("operations", 1000), ("warmup", 128), ("keys", 256), ("value-bytes", 256),
                          ("seed", 4242), ("timeout-ms", 5000), ("repetitions", 3), ("run-timeout", 300)):
        parser.add_argument("--" + name, type=int, default=default)
    parser.add_argument("--threads", default="1,4", help="comma-separated client concurrency levels")
    parser.add_argument("--profile", choices=["all", *PROFILES], default="all")
    parser.add_argument("--allow-non-release", action="store_true", help="validation only; allow non-Release/instrumented binaries")
    args = parser.parse_args()
    build = args.build_dir.resolve()
    cache = cache_values(build)
    flags = cache.get("CMAKE_CXX_FLAGS", "") + " " + cache.get("CMAKE_CXX_FLAGS_RELEASE", "")
    if not args.allow_non_release and (cache.get("CMAKE_BUILD_TYPE") != "Release" or "sanitize=" in flags or re.search(r"(?:^|\s)-O0(?:\s|$)", flags)):
        parser.error("Use a Release build without sanitizers; --allow-non-release is only for validation")
    threads = [int(item) for item in args.threads.split(",")]
    if (args.nodes < 1 or args.nodes % 2 == 0 or args.nodes > 65535
            or not 1 <= args.operations <= 200000 or not 0 <= args.warmup <= 200000 or not 1 <= args.keys <= 100000
            or not 0 <= args.value_bytes <= 65536 or not 1 <= args.repetitions <= 100 or not 0 <= args.seed < 2**64
            or not 1 <= args.timeout_ms <= 30000 or args.run_timeout <= 0 or len(set(threads)) != len(threads)
            or any(not 1 <= item <= min(32, args.operations) for item in threads)
            or (args.operations + args.warmup + args.keys) * (25 + 16 + args.value_bytes) > 48 * 1024 * 1024):
        parser.error("Require a positive odd node count and valid workload sizes, concurrency and log budget")
    for name in ("kvbench", "kvnode", "kvadmin"):
        if not os.access(build / name, os.X_OK):
            parser.error(f"Build {name} first")
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    output = (args.output or ROOT / "build/benchmark-results" / stamp).resolve()
    output.mkdir(parents=True, exist_ok=False)
    metadata = environment(build, output, cache)
    metadata["purpose"] = "validation_only" if args.allow_non_release else "performance"
    metadata["parameters"] = {**vars(args), "build_dir": str(build), "output": str(output), "threads": threads}
    (output / "environment.json").write_text(json.dumps(metadata, indent=2) + "\n")
    profiles = list(PROFILES) if args.profile == "all" else [args.profile]
    records = []
    for profile in profiles:
        for count in threads:
            for repeat in range(1, args.repetitions + 1):
                print(f"{profile}: concurrency={count}, repeat={repeat}/{args.repetitions}", flush=True)
                record = run_case(args, build, output, profile, count, repeat)
                records.append(record)
                total = record["result"]["measurement"]["total"]
                print(f"  {total['ok']}/{total['attempted']} Ok, {total['ok_ops_per_second']:.2f} Ok ops/s, P99={total['latency_ms_all']['p99']:.3f} ms", flush=True)
                write_reports(output, metadata, records)
    print(f"Report: {output / 'report.md'}", flush=True)
    return 0 if all(record["returncode"] == 0 for record in records) else 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        print(f"Benchmark runner failed: {error}", file=sys.stderr)
        sys.exit(2)
