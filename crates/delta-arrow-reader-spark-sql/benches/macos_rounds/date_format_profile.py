"""Run a frozen phase attribution probe on native releases."""
import argparse
from collections import defaultdict
import csv
import io
import json
from pathlib import Path
import shutil
from statistics import median
import subprocess
import sys
import time


sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "macos_baseline"))
from build import QOS, digest, git, replace
from native import BENCHES, REPO, archive, bounded_timeout, check_host, emit, session


WORKSPACE = Path("crates/delta-arrow-reader-spark-sql")
PACKAGE = "sail-function"


def build(args, protocol):
    assert not git(args.worktree, "status", "--porcelain")
    assert git(args.worktree, "branch", "--show-current") == ""
    initial_head = git(args.worktree, "rev-parse", "HEAD")
    source = args.worktree / WORKSPACE / protocol["source_file"]
    probe = BENCHES / protocol["probe"]
    assert digest(probe) == protocol["probe_sha256"]
    name = protocol["test"].split("::")[-1]
    anchor = f"    fn {name}() -> Result<()> {{"
    wrappers = (f"    fn {name}() -> Result<()> {{ run_phases(false) }}\n"
                f"    #[test]\n    fn {name}_reverse() -> Result<()> {{ run_phases(true) }}\n"
                "    fn run_phases(reverse: bool) -> Result<()> {\n"
                "        prefer_performance_cores();")
    test = replace(probe.read_text(), "    #[test]\n" + anchor, QOS + "    #[test]\n" + wrappers)
    test = replace(test, "for sample in 0..7 {", "for sample in 0..9 {")
    test = replace(test, "let operation = OPERATIONS[(sample + position) % OPERATIONS.len()];",
                   "let index = (sample + position) % OPERATIONS.len();\n"
                   "                let index = if reverse { OPERATIONS.len() - 1 - index } else { index };\n"
                   "                let operation = OPERATIONS[index];")
    try:
        subprocess.run(["git", "switch", "--detach", protocol["source"]], cwd=args.worktree, check=True)
        original = source.read_bytes()
        try:
            source.write_bytes(original + b"\n" + test.encode())
            command = ["cargo", "+1.96.0", "test", "--locked", "--release", "-j", "3",
                       "--config", "profile.release.debug=1", "--target-dir", args.target,
                       "-p", PACKAGE, "--lib", "--no-run", "--message-format=json"]
            result = subprocess.run(command, cwd=args.worktree / WORKSPACE, stdout=subprocess.PIPE,
                                    text=True, timeout=1800, check=True)
            executable, = [row["executable"] for line in result.stdout.splitlines()
                           if (row := json.loads(line))["reason"] == "compiler-artifact"
                           and row["target"]["name"] == "sail_function" and row.get("executable")]
            args.output.mkdir()
            binary = args.output / "phase-probe"
            shutil.copy2(executable, binary)
            receipt = {"source": protocol["source"], "probe_sha256": digest(probe),
                       "overlay_sha256": digest(source), "executable_sha256": digest(binary),
                       "rustc": "1.96.0", "profile": "release with debug level 1"}
            (args.output / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
        finally:
            source.write_bytes(original)
    finally:
        assert not git(args.worktree, "status", "--porcelain")
        subprocess.run(["git", "switch", "--detach", initial_head], cwd=args.worktree, check=True)


def collect(binary, protocol, timeout, reverse):
    check_host()
    header = "sample,phase,rows,elapsed_ns"
    name = protocol["test"] + ("_reverse" if reverse else "")
    command = [binary, name, "--exact", "--nocapture", "--test-threads=1"]
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=timeout,
                                env={"PATH": "/usr/bin:/bin", "TZ": "UTC"})
    except subprocess.TimeoutExpired as error:
        return "timeout", read_samples(error.stdout.decode(errors="replace") if error.stdout else "", header)
    records = read_samples(result.stdout, header)
    expected = {(sample, phase) for sample in range(9) for phase in protocol["phases"]}
    actual = {(row["sample"], row["phase"]) for row in records}
    valid = len(records) == len(expected) and actual == expected
    valid &= all(row["rows"] == protocol["rows"] and row["elapsed_ns"] > 0 for row in records)
    return "ok" if result.returncode == 0 and valid else "failed", records


def read_samples(stdout, header):
    payload = [line for line in stdout.splitlines() if line[:1].isdigit()]
    rows = []
    for row in csv.DictReader(io.StringIO("\n".join([header, *payload]))):
        if row.get("elapsed_ns", "").isdigit():
            rows.append({key: value if key == "phase" else int(value) for key, value in row.items()})
    return rows


def summarize(values, protocol):
    differences = {}
    for name, (spark, native) in protocol["comparisons"].items():
        results = {}
        for arm, blocks in values.items():
            results[arm] = [median(p[spark]) - median(p[native]) for p in blocks]
        a, b = results["a"], results["b"]
        gap = median([*a, *b])
        floor = max(abs(median(a) - median(b)), (max(a) - min(a)) / 2, (max(b) - min(b)) / 2)
        differences[name] = {"spark_ns_per_row": median(median(p[spark]) for blocks in values.values() for p in blocks),
                             "native_ns_per_row": median(median(p[native]) for blocks in values.values() for p in blocks),
                             "gap_ns_per_row": gap, "floor_ns_per_row": floor,
                             "decision": "slower" if gap > floor else "faster" if gap < -floor else "within floor"}
    return differences


def measure(args, protocol):
    binary = args.build / "phase-probe"
    receipt = json.loads((args.build / "receipt.json").read_text())
    assert receipt["source"] == protocol["source"] and receipt["executable_sha256"] == digest(binary)
    assert receipt["probe_sha256"] == protocol["probe_sha256"]
    assert protocol["blocks"] >= 9
    with session(args.protocol) as host:
        args.output.mkdir()
        deadline = time.monotonic() + protocol["max_seconds"]
        values = defaultdict(list)
        with archive(args.output / "samples.jsonl.gz") as stream:
            emit(stream, {"kind": "session", **host, "build": receipt,
                          "protocol_sha256": digest(args.protocol)})
            for block in range(1, protocol["blocks"] + 1):
                arms = ("a", "b") if block % 2 else ("b", "a")
                for arm in arms:
                    timeout = bounded_timeout(deadline, protocol["max_process_seconds"])
                    status, records = collect(binary, protocol, timeout, block % 2 == 0)
                    emit(stream, {"kind": "process", "block": block, "arm": arm,
                                  "status": status, "records": records})
                    if status == "ok":
                        phases = defaultdict(list)
                        for row in records:
                            phases[row["phase"]].append(row["elapsed_ns"] / row["rows"])
                        values[arm].append(phases)
        complete = all(len(values[arm]) == protocol["blocks"] for arm in ("a", "b"))
        result = {"complete": complete, "comparisons": summarize(values, protocol) if complete else {}}
        (args.output / "comparisons.json").write_text(json.dumps(result, indent=2) + "\n")


def main():
    cli = argparse.ArgumentParser(description=__doc__)
    actions = cli.add_subparsers(dest="action", required=True)
    for action, names in (("build", ("worktree", "target")), ("measure", ("build",))):
        sub = actions.add_parser(action)
        for name in ("protocol", *names, "output"):
            sub.add_argument(name, type=lambda path: Path(path).resolve())
    args = cli.parse_args()
    assert not args.output.is_relative_to(REPO)
    assert git(REPO, "show", f"HEAD:{args.protocol.relative_to(REPO)}") == args.protocol.read_text().strip()
    protocol = json.loads(args.protocol.read_text())
    (build if args.action == "build" else measure)(args, protocol)


if __name__ == "__main__":
    main()
