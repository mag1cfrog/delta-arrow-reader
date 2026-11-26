"""Compare two native release builds over the frozen planning statements."""

import argparse
from collections import defaultdict
import gzip
import json
from pathlib import Path
import sys

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "macos_baseline"))
import cells
from build import digest, git
from native import BENCHES, REPO, archive, call, emit, paired, session
from preflight import planning as check_results
from run import PROTOCOL, source_receipt, trial

SUITE = "planning_attribution"
MODES = cells.CONFIGURATIONS[SUITE]
PHASES = ("logical_ns", "physical_ns", "total_ns")
NOISE = BENCHES / "reports/macos-baseline-1560-noise.json"
QUERIES = BENCHES / "planning/queries.json"


def verify_builds(args, protocol):
    before, after = (source_receipt(path) for path in (args.binaries, args.candidate))
    assert before["source"] == protocol["staging"]
    assert after["source"] == protocol["candidate"]
    assert before["overlays"] == after["overlays"]
    assert before["binaries"].keys() == after["binaries"].keys() == {"planning", SUITE}
    assert before["binaries"][SUITE] != after["binaries"][SUITE]
    return before, after


def manifest(protocol):
    assert digest(QUERIES) == protocol["queries_sha256"]
    return [{"suite": SUITE, "id": case, "label": f"planning/{case}/{mode}",
             "configuration": mode, "metric": protocol["target_metric"]}
            for case in protocol["statements"] for mode in MODES]


def preflight(args, specs, builds):
    args.output.mkdir()
    fixture = args.output / "delta"
    cells.prepare_planning(args.binaries, fixture)
    traces = {}
    for mode in MODES:
        staged = args.output / "traces" / mode / "before"
        trial_path = args.output / "traces" / mode / "after"
        status, rows = call(args.binaries / SUITE, ["trace", fixture, mode, "before", "-"], staged, 180)
        count = 309 if mode.startswith("spark-") else 30
        assert status == "ok" and len(rows) == count and len({r["id"] for r in rows}) == count, mode
        reference = staged / "stdout.jsonl"
        status, rows = call(args.candidate / SUITE, ["trace", fixture, mode, "after", reference], trial_path, 180)
        assert status == "ok" and len(rows) == count and len({r["id"] for r in rows}) == count, mode
        assert digest(reference) == digest(trial_path / "stdout.jsonl"), f"{mode} canonical trace mismatch"
        traces[mode] = {"rows": count, "sha256": digest(reference)}
    for role, release in (("before", args.binaries), ("after", args.candidate)):
        failures = check_results(release, args.output / role, specs)
        assert not failures, f"{role} typed result or scan check failed: {failures}"
    cells.write(args.output / "proof.json", {"builds": builds, "traces": traces,
                "typed_result_checks_per_build": 4 * len({s["id"] for s in specs}),
                "protocol_sha256": digest(args.protocol)})


def phases(path, specs, primary, blocks):
    records = defaultdict(lambda: defaultdict(dict))
    with gzip.open(path, "rt") as stream:
        for line in stream:
            row = json.loads(line)
            if row["kind"] != "process" or row["status"] != "ok":
                continue
            for phase in PHASES:
                key = row["label"], phase
                assert row["block"] not in records[key][row["arm"]]
                records[key][row["arm"]][row["block"]] = cells.metric(
                    {**row["cell"], "metric": phase}, row["records"], row["samples_per_process"])
    results = []
    for spec in specs:
        label = spec["label"]
        for phase in PHASES:
            if primary[label]["status"] != "ok":
                result = primary[label]
            else:
                arms = {arm: [arm_data[i] for i in range(1, blocks + 1)]
                        for arm, arm_data in records[label, phase].items()}
                result = {"status": "ok", **paired(arms, blocks)}
            results.append({"case": spec["id"], "configuration": spec["configuration"],
                            "metric": phase, **result})
    return results


def measure(args, specs, builds, settings, counts):
    proof = cells.read(args.proof)
    assert proof["builds"] == list(builds) and proof["protocol_sha256"] == digest(args.protocol)
    assert proof["typed_result_checks_per_build"] == len(specs)
    assert sum(trace["rows"] for trace in proof["traces"].values()) == 678
    with session(args.protocol) as binding:
        args.output.mkdir()
        fixture = args.output / "delta"
        cells.prepare_planning(args.binaries, fixture)
        timing = args.output / "samples.jsonl.gz"
        with archive(timing) as stream:
            emit(stream, {"kind": "session", **binding, "builds": builds, "protocol_sha256": digest(args.protocol),
                          "proof_sha256": digest(args.proof), "queries_sha256": digest(QUERIES)})
            schedule = {**settings, "cell_session_seconds": cells.read(args.protocol)["max_seconds"]}
            primary = trial(specs, args, schedule, counts, stream, {"planning_delta": fixture}, "planning")
        report = phases(timing, specs, primary, settings["blocks"])
        cells.write(args.output / "comparisons.json", report)


def main():
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("action", choices=("preflight", "measure"))
    for arg in ("protocol", "binaries", "candidate", "output"):
        cli.add_argument(arg, type=lambda p: Path(p).resolve())
    cli.add_argument("--proof", type=lambda p: Path(p).resolve())
    args = cli.parse_args()
    assert not args.output.is_relative_to(REPO)
    protocol = cells.read(args.protocol)
    assert git(REPO, "show", f"HEAD:{args.protocol.relative_to(REPO)}") == args.protocol.read_text().strip()
    settings, noise = cells.read(PROTOCOL), cells.read(NOISE)
    assert digest(NOISE) == protocol["schedule_sha256"]
    assert settings["blocks"] == protocol["blocks"] == noise["blocks"] == 9
    assert noise["samples"][SUITE] == protocol["samples_per_process"] == 9
    specs, builds = manifest(protocol), verify_builds(args, protocol)
    args.fixture = None
    if args.action == "preflight":
        preflight(args, specs, builds)
    else:
        assert args.proof is not None
        measure(args, specs, builds, settings, noise["samples"])


if __name__ == "__main__":
    main()
