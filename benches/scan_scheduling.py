"""Measure streaming scheduling in isolated processes using only Python's stdlib.

Build the Rust target with cargo bench --locked --bench scan_scheduling --no-run.
Pass its executable as --binary baseline=PATH and choose an empty --output-dir.
The runner saves executable copies, fixture hashes, environment metadata, warmups,
every measured result, and summaries. A later --binary candidate=PATH runs both
versions in counterbalanced order on the same fixtures. No scheduler fix is applied.

Execution timings exclude process startup, fixture generation, table loading, and
scan planning. CPU seconds cover the whole child. Peak RSS includes the HTTP server
for HTTP cases. Warmups prime the OS file cache; each measurement starts a fresh
reader, runtime, and optional HTTP server. Sampled task/batch counters are not exact
permit or queue high-water marks. Strict resource bounds need scheduler tests.
"""

import argparse
import csv
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import platform
import random
import re
import resource
import shutil
import statistics
import subprocess
import sys

from run_order import balanced_orders


@dataclass(frozen=True)
class Case:
    role: str
    shape: str
    transport: str
    partitions: int
    cap: int | None
    prefetch: int
    delay_us: int = 0
    limit: int | None = None
    backend: str = "direct"

    @property
    def name(self):
        return (
            f"{self.shape}-{self.transport}-p{self.partitions}-c{optional(self.cap)}"
            f"-pf{self.prefetch}-d{self.delay_us}-l{optional(self.limit)}-{self.backend}"
        )


def optional(value):
    return "none" if value is None else str(value)


def cases():
    result = []
    for shape in ("small", "batches", "unequal", "large"):
        for transport in ("local", "http"):
            # These controls have enough permits for every active partition.
            for partitions, cap, prefetch in (
                (1, 1, 0), (1, 8, 2), (16, None, 2),
                (16, None, 0), (4, 8, 0), (2, 8, 2),
            ):
                result.append(Case("performance", shape, transport, partitions, cap, prefetch))
            if shape in ("small", "large"):
                # One file per partition needs one permit even with prefetch=2.
                file_count = 64 if shape == "small" else 8
                result.append(Case("performance", shape, transport, file_count, file_count, 2))
            if shape in ("small", "batches"):
                result.append(Case("performance", shape, transport, 16, None, 2, delay_us=1000))
                result.append(Case("performance", shape, transport, 16, None, 2, limit=1))
                for cap, prefetch in ((1, 2), (8, 0), (8, 2)):
                    result.append(Case("progress", shape, transport, 16, cap, prefetch))
                # Kernel ignores prefetch; its concurrency controls still share the limiter.
                for partitions, cap in ((16, None), (4, 8)):
                    result.append(Case("performance", shape, transport, partitions, cap, 2, backend="kernel"))
                result.append(Case("progress", shape, transport, 16, 8, 2, backend="kernel"))
    assert len({case.name for case in result}) == len(result)
    return result


def file_hash(path):
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def tree_hash(root):
    digest = hashlib.sha256()
    for path in sorted(root.rglob("*")):
        if path.is_file():
            digest.update(path.relative_to(root).as_posix().encode() + b"\0")
            digest.update(bytes.fromhex(file_hash(path)))
    return digest.hexdigest()


def revision(repo):
    return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repo, text=True).strip()


def summarize(samples, output):
    groups = {}
    for sample in samples:
        if sample["phase"] == "measured":
            groups.setdefault((sample["case"], sample["binary"]), []).append(sample)
    rows = []
    for (case, binary), measurements in sorted(groups.items()):
        completed = [x for x in measurements if x["status"] == "ok"]
        row = {
            "case": case, "binary": binary, "role": measurements[0]["role"],
            "runs": len(measurements), "completed": len(completed),
            "timeouts": sum(x["status"] == "timeout" for x in measurements),
            "other_failures": sum(x["status"] not in ("ok", "timeout") for x in measurements),
        }
        # Mixed success/timeout cases are censored, so do not report a performance median.
        valid = len(completed) == len(measurements)
        for metric in ("elapsed_us", "first_batch_us", "rows_per_second", "process_peak_rss_bytes", "cpu_seconds"):
            values = [x[metric] for x in completed if x.get(metric) is not None]
            row[f"median_{metric}"] = statistics.median(values) if valid and values else None
            if metric == "elapsed_us":
                row["min_elapsed_us"] = min(values) if valid and values else None
                row["max_elapsed_us"] = max(values) if valid and values else None
        rows.append(row)
    if rows:
        with output.open("w", newline="") as destination:
            writer = csv.DictWriter(destination, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", action="append", required=True, metavar="LABEL=PATH")
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--repetitions", type=int, default=8)
    parser.add_argument("--probe-repetitions", type=int, default=3)
    parser.add_argument("--timeout-seconds", type=int, default=15, help="deadline for performance controls")
    parser.add_argument("--probe-timeout-seconds", type=int, default=5, help="deadline for progress probes")
    parser.add_argument("--case-filter", default=".")
    parser.add_argument("--suite", choices=("all", "performance", "progress"), default="all")
    args = parser.parse_args()
    if not (1 <= args.repetitions <= 128 and 1 <= args.probe_repetitions <= 128
            and 1 <= args.timeout_seconds <= 120 and 1 <= args.probe_timeout_seconds <= 120):
        parser.error("repetitions must be 1..128 and timeout must be 1..120 seconds")
    selected = [case for case in cases() if re.search(args.case_filter, case.name)
                and args.suite in ("all", case.role)]
    if not selected:
        parser.error("no cases selected")
    binaries = {}
    for spec in args.binary:
        label, separator, path = spec.partition("=")
        if not separator or not re.fullmatch(r"[a-zA-Z0-9_-]+", label) or label in binaries:
            parser.error("each binary needs a unique simple LABEL=PATH")
        source = Path(path).resolve(strict=True)
        if not source.is_file() or not os.access(source, os.X_OK):
            parser.error(f"not an executable: {source}")
        binaries[label] = source
    root = args.output_dir.resolve()
    root.mkdir(parents=True, exist_ok=False)
    (root / "bin").mkdir()
    (root / "fixtures").mkdir()
    for label, source in binaries.items():
        binaries[label] = Path(shutil.copy2(source, root / "bin" / label))
    repo = Path(__file__).resolve().parent.parent
    for relative in ("Cargo.toml", "Cargo.lock", "benches/scan_scheduling.rs", "benches/scan_scheduling.py",
                     "benches/run_order.py", "benches/range_planning/controlled_http.rs",
                     "tests/scan_scheduling_benchmark_harness.rs"):
        destination = root / "source" / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(repo / relative, destination)
    metadata = {
        "schema_version": 1, "started_at": datetime.now(timezone.utc).isoformat(),
        "command": sys.argv, "git_head": revision(repo),
        "production_source_sha256": tree_hash(repo / "src"),
        "cargo_lock_sha256": file_hash(repo / "Cargo.lock"),
        "harness_sha256": file_hash(repo / "benches/scan_scheduling.rs"),
        "controlled_http_sha256": file_hash(repo / "benches/range_planning/controlled_http.rs"),
        "runner_sha256": file_hash(Path(__file__)),
        "platform": platform.platform(), "python": platform.python_version(),
        "cpu_affinity": sorted(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else None,
        "load_average_before": os.getloadavg(), "worker_threads": 4,
        "binaries": {name: {"path": str(path), "sha256": file_hash(path)} for name, path in binaries.items()},
        "fixtures": {}, "repetitions": args.repetitions, "probe_repetitions": args.probe_repetitions,
        "timeout_seconds": args.timeout_seconds, "probe_timeout_seconds": args.probe_timeout_seconds,
        "ordering_seed": 122,
        "method": "fresh process per run; one warmup per performance case; execution-only timer; ID checksum included; process CPU/RSS include setup and controlled HTTP server",
    }
    for shape in sorted({case.shape for case in selected}):
        fixture = root / "fixtures" / shape
        prepared = subprocess.run([str(next(iter(binaries.values()))), "prepare", str(fixture), shape],
                                  check=True, capture_output=True, text=True)
        metadata["fixtures"][shape] = dict(json.loads(prepared.stdout), sha256=tree_hash(fixture))
    (root / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
    samples = []
    orders = balanced_orders(tuple(binaries))
    case_indices = {case.name: index for index, case in enumerate(selected)}
    with (root / "runs.jsonl").open("w") as raw:
        for phase in ("warmup", "measured"):
            rounds = 1 if phase == "warmup" else max(args.repetitions, args.probe_repetitions)
            for repetition in range(rounds):
                ordered_cases = selected.copy()
                random.Random(122 + repetition).shuffle(ordered_cases)
                for position, case in enumerate(ordered_cases):
                    if phase == "warmup" and case.role == "progress":
                        continue
                    count = args.repetitions if case.role == "performance" else args.probe_repetitions
                    if phase == "measured" and repetition >= count:
                        continue
                    order = orders[(repetition + case_indices[case.name]) % len(orders)]
                    for binary_position, label in enumerate(order):
                        deadline = args.timeout_seconds if case.role == "performance" else args.probe_timeout_seconds
                        command = [str(binaries[label]), "run", str(root / "fixtures" / case.shape),
                                   case.transport, case.backend, str(case.partitions), optional(case.cap), str(case.prefetch),
                                   str(case.delay_us), optional(case.limit), str(deadline)]
                        usage_before = resource.getrusage(resource.RUSAGE_CHILDREN)
                        try:
                            child = subprocess.run(command, capture_output=True, text=True,
                                                   timeout=deadline + 30)
                            if child.returncode:
                                measurement = {"status": "child_error", "exit_code": child.returncode, "stderr": child.stderr}
                            else:
                                measurement = json.loads(child.stdout)
                        except subprocess.TimeoutExpired:
                            measurement = {"status": "process_timeout"}
                        usage_after = resource.getrusage(resource.RUSAGE_CHILDREN)
                        measurement.update({
                            "case": case.name, "role": case.role, "binary": label, "phase": phase,
                            "repetition": repetition + 1, "order_position": position,
                            "binary_order_position": binary_position,
                            "cpu_seconds": (usage_after.ru_utime + usage_after.ru_stime
                                            - usage_before.ru_utime - usage_before.ru_stime),
                            "load_average": os.getloadavg()[0],
                        })
                        raw.write(json.dumps(measurement) + "\n")
                        raw.flush()
                        samples.append(measurement)
                        summarize(samples, root / "summary.csv")
                        print(f"{phase} {repetition+1} {label} {case.name}: "
                              f"{measurement['status']} {measurement.get('elapsed_us', '')} us", file=sys.stderr)
                        if measurement["status"] not in ("ok", "timeout") or (
                            case.role == "performance" and measurement["status"] != "ok"
                        ):
                            raise RuntimeError(f"benchmark control failed: {measurement}")
    metadata["finished_at"] = datetime.now(timezone.utc).isoformat()
    metadata["load_average_after"] = os.getloadavg()
    (root / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
    print(root / "summary.csv")


if __name__ == "__main__":
    main()
