"""Run the frozen #1516 restricted function comparisons natively on macOS."""

import argparse
from collections import defaultdict
import json
from pathlib import Path
import runpy
from statistics import median
import sys
import time
from unittest.mock import patch

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "macos_baseline"))
import build as mac_build
import cells
from native import BENCHES, REPO, archive, bounded_timeout, call, check_host, emit, paired, safe_record, session

HISTORY = BENCHES / "functions_1516"
MANIFEST = HISTORY / "manifest.json"
SCHEDULE = BENCHES / "reports/macos-baseline-1560-noise.json"
MODES = ("native", "spark-off", "spark-on")
ARMS = (*MODES, "a", "b")


def restricted_overlay(workspace, targets):
    assert targets == ("functions",)
    original_path = sys.path[:]
    try:
        legacy = runpy.run_path(str(HISTORY / "release.py"))
        paths = dict(legacy["restricted_changes"](workspace, False))
    finally:
        sys.path[:] = original_path
    path = workspace / "benches/functions.rs"
    source = paths[path]
    start = source.index('    let status = fs::read_to_string("/proc/self/status")?;')
    end = source.index("    Ok(", start)
    source = source[:start] + "    let peak_rss: Option<u64> = None;\n" + source[end:]
    source = mac_build.replace(source, "for iteration in 0..4 {", "for iteration in 0..=sample_count {")
    source = mac_build.replace(source, "configuration: &str, input_rows: usize) -> Result<Value>",
                               "configuration: &str, input_rows: usize, sample_count: usize) -> Result<Value>")
    source = mac_build.replace(source, "async fn run(args: &[String])",
                               "async fn run(args: &[String], sample_count: usize)")
    measure = "measure(contexts.get(&args[3]), case, &args[3], contexts.input_rows)"
    assert source.count(measure) == 2
    source = source.replace(measure, measure[:-1] + ", sample_count)")
    source = mac_build.replace(source, "    let args = std::env::args().skip(1).collect::<Vec<_>>();",
                               "    let mut args = std::env::args().skip(1).collect::<Vec<_>>();\n"
                               "    let sample_count = retained_samples(&mut args, 3);")
    source = mac_build.replace(source, "runtime.block_on(run(&args))",
                               "runtime.block_on(run(&args, sample_count))")
    paths[path] = mac_build.worker_callback(mac_build.sync_main(source)) + mac_build.SAMPLES
    return paths


def build_release(args, protocol):
    source = protocol["source"]
    with patch.object(mac_build, "overlay", restricted_overlay):
        mac_build.build(source, args.worktree, args.target, args.output, ("functions",))
    receipt_path = args.output / "receipt.json"
    receipt = cells.read(receipt_path)
    receipt["adapter_sha256"] = mac_build.digest(Path(__file__))
    receipt["manifest_sha256"] = mac_build.digest(MANIFEST)
    cells.write(receipt_path, receipt)


def verify_release(args, protocol):
    receipt = cells.read(args.release / "receipt.json")
    assert receipt["source"] == protocol["source"] and receipt["rustc"] == "1.96.0"
    assert receipt["profile"] == "release" and receipt["manifest_sha256"] == mac_build.digest(MANIFEST)
    assert receipt["adapter_sha256"] == mac_build.digest(Path(__file__))
    assert receipt["recipe_sha256"] == mac_build.digest(Path(mac_build.__file__))
    assert receipt["binaries"] == {"functions": mac_build.digest(args.release / "functions")}
    return receipt


def preflight(args, protocol, receipt, cases):
    args.output.mkdir()
    deadline = time.monotonic() + protocol["preflight_seconds"]
    results = {}
    for case in cases:
        key = case["id"]
        if key in protocol["prior_decisions"]:
            results[key] = {"status": "prior judgment", "reason": protocol["prior_decisions"][key]}
            continue
        if "restricted_input_sql" not in case:
            results[key] = {"status": "source review", "reason": case["source_review"]}
            continue
        work = args.output / "preflight" / key
        status, records = call(args.release / "functions",
                               ["preflight", MANIFEST, key, work / "plans"], work,
                               bounded_timeout(deadline, protocol["preflight_process_seconds"]))
        if status != "ok" or len(records) != 1:
            results[key] = {"status": "untimable", "reason": f"preflight {status}"}
            continue
        record, = records
        rows = record["input_rows"]
        output_rows = 1 if key.startswith("aggregate.") else rows
        modes = {row["configuration"]: row for row in record["results"]}
        eligible = (
            tuple(modes) == MODES and 0 < rows <= 1_000_000
            and len(record["baselines"]) == 3
            and all(row["rows"] == rows for row in record["baselines"])
            and all(row["status"] == "ok" and row["rows"] == output_rows
                    and row["source_guard_rows"] == rows for row in modes.values())
            and all(row["types"] == modes["native"]["types"] for row in modes.values())
            and all(modes[mode].get("comparison", {}).get("equal") for mode in MODES[1:])
        )
        result = {"status": "eligible" if eligible else "excluded",
                  "input_rows": rows, "output_rows": output_rows,
                  "results": record["results"]}
        if eligible:
            result["types"] = modes["native"]["types"]
        else:
            result["reason"] = "Rows, executed types or input guards differ from the restricted reference."
        result["plan_sha256"] = {plan.name: mac_build.digest(plan) for plan in sorted((work / "plans").glob("*.plan"))}
        results[key] = result
    roots = ((args.output, "EVIDENCE"), (args.release, "RELEASE"), (REPO, "REPOSITORY"),
             (Path.home(), "HOME_ROOT"))
    cells.write(args.output / "proof.json", safe_record({
        "protocol_sha256": mac_build.digest(args.protocol), "release": receipt, "cases": results,
    }, roots))


def process(binary, case, mode, work, proof, samples, deadline, timeout):
    ids = work.with_suffix(".ids.json")
    ids.parent.mkdir(parents=True, exist_ok=True)
    cells.write(ids, [case])
    status, records = call(binary, ["--macos-samples", samples, "measure-set", MANIFEST, ids, mode],
                           work, bounded_timeout(deadline, timeout))
    if status != "ok":
        return status, None, records
    try:
        result, values = cells.function_samples(
            "restricted-1516", records, {"id": case, "configuration": mode}, samples
        )
        assert result["types"] == proof["types"]
        assert [row["iteration"] for row in values] == list(range(1, samples + 1))
        assert all(row["input_rows"] == proof["input_rows"] and row["output_rows"] == proof["output_rows"]
                   for row in values)
    except (AssertionError, ValueError, KeyError):
        return "timed_validation_failure", None, records
    return "ok", median(row["increment_ns"] / row["input_rows"] for row in values), records


def measure(args, protocol, receipt, cases):
    proof = cells.read(args.proof)
    assert proof["protocol_sha256"] == mac_build.digest(args.protocol) and proof["release"] == receipt
    eligible = [case["id"] for case in cases if proof["cases"][case["id"]]["status"] == "eligible"]
    assert eligible
    tracks = {case: defaultdict(list) for case in eligible}
    failed = {}
    blocks, samples = (protocol[key] for key in ("blocks", "samples"))
    roots = ((args.output, "EVIDENCE"), (args.release, "RELEASE"), (REPO, "REPOSITORY"),
             (Path.home(), "HOME_ROOT"))
    with session(args.protocol) as binding:
        args.output.mkdir()
        deadline = time.monotonic() + protocol["max_seconds"]
        with archive(args.output / "samples.jsonl.gz") as stream:
            emit(stream, {"kind": "session", **binding, "release": receipt,
                          "proof_sha256": mac_build.digest(args.proof),
                          "protocol_sha256": mac_build.digest(args.protocol), "eligible": eligible})
            for block in range(1, blocks + 1):
                order = 1 if block % 2 else -1
                for case in eligible[::order]:
                    if case in failed:
                        continue
                    for arm in ARMS[::order]:
                        mode = arm if arm in MODES else "native"
                        work = args.output / f"{block:02}" / case / arm
                        check_host()
                        try:
                            status, value, records = process(
                                args.release / "functions", case, mode, work, proof["cases"][case],
                                samples, deadline, protocol["process_seconds"],
                            )
                        except TimeoutError:
                            status, value, records = "schedule_timeout", None, []
                        emit(stream, {"kind": "process", "block": block, "case": case, "arm": arm,
                                      "configuration": mode, "status": status, "records": records}, roots)
                        if status != "ok":
                            failed[case] = {"block": block, "arm": arm, "status": status}
                            break
                        tracks[case][arm].append(value)
                print(f"restricted functions: block {block}/{blocks}", flush=True)
    comparisons = {}
    for case, arms in tracks.items():
        if case in failed:
            comparisons[case] = {"status": "untimable", **failed[case]}
        else:
            comparisons[case] = {"status": "ok", **{
                mode: paired({"before": arms["native"], "after": arms[mode], "a": arms["a"], "b": arms["b"]},
                             blocks) for mode in MODES[1:]
            }}
    cells.write(args.output / "comparisons.json", comparisons)


def main():
    cli = argparse.ArgumentParser(description=__doc__)
    commands = cli.add_subparsers(dest="action", required=True)
    for action, names in (("build", ("worktree", "target")), ("preflight", ("release",)),
                          ("measure", ("release", "proof"))):
        command = commands.add_parser(action)
        for name in ("protocol", *names, "output"):
            command.add_argument(name, type=lambda value: Path(value).resolve())
    args = cli.parse_args()
    assert not args.output.is_relative_to(REPO)
    assert mac_build.git(REPO, "show", f"HEAD:{args.protocol.relative_to(REPO)}") == args.protocol.read_text().strip()
    protocol = cells.read(args.protocol)
    assert protocol["manifest_sha256"] == mac_build.digest(MANIFEST)
    assert protocol["legacy_overlay_sha256"] == mac_build.digest(HISTORY / "release.py")
    assert protocol["schedule_sha256"] == mac_build.digest(SCHEDULE)
    assert protocol["blocks"] == 9 and protocol["samples"] == cells.read(SCHEDULE)["samples"]["functions"]
    cases = cells.read(MANIFEST)["functions"]
    assert len(cases) == len({case["id"] for case in cases}) == 29
    if args.action == "build":
        build_release(args, protocol)
    else:
        receipt = verify_release(args, protocol)
        (preflight if args.action == "preflight" else measure)(args, protocol, receipt, cases)


if __name__ == "__main__":
    main()
