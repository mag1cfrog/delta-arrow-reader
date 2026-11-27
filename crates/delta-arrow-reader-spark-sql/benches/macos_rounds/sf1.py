"""Compare SF1 SQL releases using the frozen macOS cell schedule."""

import argparse
import hashlib
import json
from pathlib import Path
import sys
import time

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "macos_baseline"))
import cells
from build import digest, git
from native import BENCHES, REPO, archive, bounded_timeout, call, emit, legacy_runner, safe_record, session
from run import HERE, PROTOCOL, resources, roots, source_receipt, trial

BASELINE = HERE / "baseline-cells.json"
NOISE = BENCHES / "reports/macos-baseline-1560-noise.json"
FIXTURE = BENCHES / "reports/macos-baseline-1560-fixture.json"


def manifest(protocol):
    assert digest(BASELINE) == protocol["baseline_manifest_sha256"]
    original = {(r["suite"], r["id"]): r["expected_rows"] for r in cells.read(BASELINE)["cells"]
                if r["suite"] in cells.PREFIX}
    specs = []
    for entry in protocol["statements"]:
        suite, case = entry["suite"], entry["id"]
        for mode in entry["configurations"]:
            assert mode in cells.CONFIGURATIONS[suite]
            specs.append({"suite": suite, "id": case, "configuration": mode,
                          "label": f"{suite}/{case}/{mode}", "expected_rows": original[suite, case],
                          "metric": "query_complete_ns"})
    assert len({s["label"] for s in specs}) == len(specs)
    return specs


def collect(args, builds, specs, snapshot):
    args.output.mkdir()
    deadline = time.monotonic() + 7200
    observed = {"staging": {}, "candidate": {}}
    excluded = {}

    def exclude(spec, reason):
        excluded.setdefault((spec["suite"], spec["id"]), []).append(reason)

    for role, release in (("staging", args.binaries), ("candidate", args.candidate)):
        for spec in specs:
            suite, case, mode, label = (spec[k] for k in ("suite", "id", "configuration", "label"))
            work = args.output / role / label
            status, rows = call(release / "execution", [*cells.PREFIX[suite], "preflight", snapshot,
                                work, mode, case], work, bounded_timeout(deadline, 300))
            if status != "ok" or len(rows) != 1:
                exclude(spec, f"{role}/{mode}: preflight {status}")
                continue
            row, = rows
            assert (row["id"], row["configuration"]) == (case, mode)
            if row["rows"] != spec["expected_rows"] or not row["source_io"]["counters_complete"]:
                exclude(spec, f"{role}/{mode}: row count or source counters differ")
                continue
            row["ipc_sha256"] = digest(Path(row["ipc_path"]))
            observed[role][label] = row

    comparisons = {}

    def compare(spec, left, right, kind, require_io):
        label = spec["label"]
        path = args.output / "compare" / kind / label
        if (left["arrow_types"], left["sql_schema"]) != (right["arrow_types"], right["sql_schema"]):
            exclude(spec, f"{kind}/{label}: executed types differ")
            return
        if require_io:
            try:
                require_same_io(left["source_io"], right["source_io"])
            except AssertionError as error:
                exclude(spec, f"{kind}/{label}: {error}")
                return
        status, proof = call(args.binaries / "execution", ["compare", left["ipc_path"], right["ipc_path"]],
                             path, bounded_timeout(deadline, 300))
        if status != "ok" or len(proof) != 1 or proof[0].get("rows_compared") != spec["expected_rows"]:
            exclude(spec, f"{kind}/{label}: decoded rows differ or comparison failed ({status})")
            return
        comparisons[f"{kind}/{label}"] = proof[0]["rows_compared"]

    with legacy_runner("execution/run.py") as runner:
        require_same_io = runner["require_same_source_io"]
        for spec in specs:
            suite, case, mode, label = (spec[k] for k in ("suite", "id", "configuration", "label"))
            if (suite, case) in excluded:
                continue
            before, after = (observed[role][label] for role in ("staging", "candidate"))
            compare(spec, before, after, "cross-release", True)
            if mode != "native":
                reference = f"{suite}/{case}/native"
                if reference in observed["staging"]:
                    compare(spec, observed["staging"][reference], before, "native-reference", False)
                elif mode == "spark-on":
                    compare(spec, observed["staging"][f"{suite}/{case}/spark-off"], before,
                            "ansi-reference", False)
        for role, values in observed.items():
            for mode in ("spark-off", "spark-on"):
                cast = values.get(f"parsers/interval_day_time/{mode}")
                projection = values.get(f"parsers/interval_day_time_input/{mode}")
                if cast and projection:
                    try:
                        require_same_io(cast["source_io"], projection["source_io"])
                    except AssertionError as error:
                        excluded.setdefault(("parsers", "interval_day_time"), []).append(f"{role}/{mode}: {error}")

    for rows in observed.values():
        for row in rows.values():
            row.pop("operator_metrics", None)
            row.pop("pool_peak_bytes", None)
            source_io = row["source_io"]
            ranges = source_io.pop("ranges")
            source_io["range_count"] = len(ranges)
            source_io["ranges_sha256"] = hashlib.sha256(json.dumps(ranges, sort_keys=True).encode()).hexdigest()
    cells.write(args.output / "proof.json", safe_record({
        "builds": builds, "protocol_sha256": digest(args.protocol),
        "fixture_proof_sha256": digest(FIXTURE), "observed": observed, "row_comparisons": comparisons,
        "excluded": {s["label"]: excluded[s["suite"], s["id"]] for s in specs if (s["suite"], s["id"]) in excluded},
    }, roots(args)))


def measure(args, builds, specs, data, settings, counts):
    proof = cells.read(args.proof)
    assert proof["builds"] == builds and proof["protocol_sha256"] == digest(args.protocol)
    assert proof["fixture_proof_sha256"] == digest(FIXTURE)
    included = [s for s in specs if s["label"] not in proof["excluded"]]
    assert included and all(f"cross-release/{s['label']}" in proof["row_comparisons"] for s in included)
    with session(args.protocol) as binding:
        args.output.mkdir()
        with archive(args.output / "samples.jsonl.gz") as stream:
            emit(stream, {"kind": "session", **binding, "builds": builds,
                          "protocol_sha256": digest(args.protocol), "proof_sha256": digest(args.proof),
                          "fixture_proof_sha256": digest(FIXTURE), "excluded": proof["excluded"]})
            results = trial(included, args, settings, counts, stream, data, "sf1")
        cells.write(args.output / "comparisons.json", results)


def main():
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("action", choices=("preflight", "measure"))
    for name in ("protocol", "binaries", "candidate", "fixture", "output"):
        cli.add_argument(name, type=lambda path: Path(path).resolve())
    cli.add_argument("--proof", type=lambda path: Path(path).resolve())
    cli.set_defaults(needs_planning=False, nullif_eligible=None, range_eligible=None, stock_cli=None)
    args = cli.parse_args()
    assert not args.output.is_relative_to(REPO)
    assert git(REPO, "show", f"HEAD:{args.protocol.relative_to(REPO)}") == args.protocol.read_text().strip()
    protocol, settings, schedule = (cells.read(path) for path in (args.protocol, PROTOCOL, NOISE))
    assert protocol["schedule_sha256"] == digest(NOISE) and settings["blocks"] == protocol["blocks"] == schedule["blocks"]
    assert protocol["fixture_proof_sha256"] == digest(FIXTURE) == digest(args.fixture / "fixture-proof.json")
    builds = [source_receipt(path) for path in (args.binaries, args.candidate)]
    assert [r["source"] for r in builds] == [protocol["staging"], protocol["candidate"]]
    assert builds[0]["overlays"] == builds[1]["overlays"]
    assert all(r["binaries"].keys() == {"execution"} for r in builds)
    assert builds[0]["binaries"] != builds[1]["binaries"]
    specs, data = manifest(protocol), resources(args)
    if args.action == "preflight":
        collect(args, builds, specs, data["snapshot"])
    else:
        assert args.proof is not None
        measure(args, builds, specs, data, {**settings, "cell_session_seconds": protocol["max_seconds"]},
                schedule["samples"])


if __name__ == "__main__":
    main()
