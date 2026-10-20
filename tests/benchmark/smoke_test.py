"""Exercise real benchmark startup, sample accounting and artifact protection."""

import csv
import json
from pathlib import Path
import subprocess
import sys
import tempfile


def check(condition, message):
    if not condition:
        raise RuntimeError(message)


def exercise(runner, build, output, nodes):
    command = [sys.executable, str(runner), "--build-dir", str(build), "--output", str(output),
               "--operations", "40", "--warmup", "8", "--keys", "8", "--value-bytes", "16",
               "--threads", "2", "--repetitions", "1", "--profile", "mixed", "--nodes", str(nodes), "--allow-non-release"]
    result = subprocess.run(command, text=True, capture_output=True, timeout=60)
    check(result.returncode == 0, result.stdout + result.stderr)
    report = json.loads((output / "results.json").read_text())
    check(len(report["runs"]) == 1, "One case expected")
    run = report["runs"][0]
    check(run["result"]["parameters"]["nodes"] == nodes and len(run["files_after_shutdown"]) == nodes, "Requested cluster size used and recorded")
    measured = run["result"]["measurement"]
    check(measured["total"]["attempted"] == measured["total"]["ok"] == 40, "All calls accounted for")
    check([measured["by_operation"][kind]["attempted"] for kind in ("get", "put", "delete")] == [20, 16, 4], "Requested operation mix")
    check(run["result"]["preload_ok"] == 8 and run["result"]["warmup"]["total"]["ok"] == 8, "Preload and warmup excluded")
    check(all(item["commit"] == item["applied"] for item in run["status_after"]), "Nodes caught up")
    check(len(report["environment"]["binary_sha256"]) == 3, "Binary identity recorded")
    with (output / run["directory"] / "samples.csv").open(newline="") as source:
        check(len(list(csv.DictReader(source))) == 40, "One raw latency per measured call")
    original = (output / "report.md").read_bytes()
    again = subprocess.run(command, text=True, capture_output=True, timeout=10)
    check(again.returncode != 0 and (output / "report.md").read_bytes() == original, "Existing results must not be overwritten")


def main():
    root = Path(__file__).resolve().parents[2]
    build = Path(sys.argv[1]).resolve()
    runner = root / "scripts/benchmark.py"
    with tempfile.TemporaryDirectory(prefix="kvstore-benchmark-") as temporary:
        for nodes in (1, 3, 5):
            exercise(runner, build, Path(temporary) / f"nodes{nodes}", nodes)
        for nodes in (0, 2, 4, 6):
            output = Path(temporary) / f"invalid{nodes}"
            result = subprocess.run([sys.executable, str(runner), "--build-dir", str(build), "--output", str(output),
                                     "--nodes", str(nodes), "--allow-non-release"], text=True, capture_output=True, timeout=10)
            check(result.returncode == 2 and not output.exists(), "Invalid node count rejected before startup")
        members = "1=127.0.0.1:61001,2=127.0.0.1:61002,3=127.0.0.1:61003"
        for arguments in (["--operations", "0"], ["--read-percent", "95", "--delete-percent", "10"],
                          ["--value-bytes", "65536", "--operations", "200000"], ["--keys", "8", "--keys", "8"]):
            rejected = subprocess.run([str(build / "kvbench"), "--members", members, *arguments], text=True, capture_output=True, timeout=5)
            check(rejected.returncode == 2, "Invalid workload rejected before preloading")
    print("Benchmark 1/3/5-node processes, raw accounting and output protection checks passed")


if __name__ == "__main__":
    main()
