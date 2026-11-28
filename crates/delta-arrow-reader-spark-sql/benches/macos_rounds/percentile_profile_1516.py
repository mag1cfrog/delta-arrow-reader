"""Attribute the #1516 approximate-percentile frame replay on native macOS."""
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
import restricted_1516 as frozen

EXTRA = {"summary_insert": "SummaryInsert", "summary_compress": "SummaryCompress",
         "summary_query": "SummaryQuery"}
SAIL = Path("vendor/sail/crates/sail-function/src/aggregate/approx_percentile.rs")
IMPORT = "use datafusion::physical_expr::aggregate::dar_aggregate_probe::{self as probe, Phase};\n"


def overlay(workspace, targets):
    source_path = workspace / "benches/functions.rs"
    paths = frozen.restricted_overlay(workspace, targets)
    instrument = runpy.run_path(str(workspace / "benches/window_aggregates_1408/instrument.py"))
    phases = (*instrument["PHASES"], *EXTRA.keys())
    names = " ".join([instrument["NAMES"], *EXTRA.values()])
    with patch.dict(instrument["aggregate_factory"].__globals__, {"PHASES": phases, "NAMES": names}):
        old = dict(instrument["changes"](workspace, True))

    replace = instrument["replace"]
    profile = "\nasync fn profile_query("
    assert old[source_path].count(profile) == 1
    source = instrument["add_import"](
        paths[source_path], "use datafusion::physical_expr::aggregate::dar_aggregate_probe;\n"
    )
    source = replace(source, "drain(context, physical).await?", "profile_query(context, physical).await?", 2)
    paths[source_path] = source + profile + old[source_path].split(profile, 1)[1]
    for path, text in old.items():
        if path not in paths:
            paths[path] = text

    summary = instrument["add_import"]((workspace / SAIL).read_text(), IMPORT)
    before = """        for value in values
            .as_primitive::<datafusion::arrow::datatypes::Float64Type>()
            .iter()
            .flatten()
        {
            self.summary.insert(value);
        }"""
    after = """        let values = values.as_primitive::<datafusion::arrow::datatypes::Float64Type>();
        let started = std::time::Instant::now();
        for value in values.iter().flatten() {
            self.summary.insert(value);
        }
        probe::record(Phase::SummaryInsert, started, values.len() - values.null_count());"""
    summary = replace(summary, before, after)
    summary = replace(summary, """    fn evaluate(&mut self) -> Result<ScalarValue> {
        self.summary.compress();""", """    fn evaluate(&mut self) -> Result<ScalarValue> {
        probe::time(Phase::SummaryCompress, 0, || self.summary.compress());""")
    summary = replace(summary,
                      "ScalarValue::Float64(Some(self.summary.query(percentile))).cast_to(&self.value_type)",
                      """let value = probe::time(Phase::SummaryQuery, 0, || self.summary.query(percentile));
            ScalarValue::Float64(Some(value)).cast_to(&self.value_type)""")
    paths[workspace / SAIL] = summary
    return paths, phases


def build(args, protocol):
    def profile(workspace, targets):
        paths, phases = overlay(workspace, targets)
        assert phases == tuple(protocol["phases"])
        return paths

    with patch.object(frozen.mac_build, "overlay", profile):
        frozen.mac_build.build(protocol["source"], args.worktree, args.target, args.output, ("functions",))
    path = args.output / "receipt.json"
    receipt = frozen.cells.read(path)
    receipt["profile_adapter_sha256"] = frozen.mac_build.digest(Path(__file__))
    receipt["restricted_adapter_sha256"] = frozen.mac_build.digest(Path(frozen.__file__))
    frozen.cells.write(path, receipt)


def verify(args, protocol):
    receipt = frozen.cells.read(args.release / "receipt.json")
    assert receipt["source"] == protocol["source"]
    assert receipt["rustc"] == "1.96.0" and receipt["profile"] == "release"
    assert receipt["binaries"] == {"functions": frozen.mac_build.digest(args.release / "functions")}
    assert receipt["profile_adapter_sha256"] == frozen.mac_build.digest(Path(__file__))
    assert receipt["restricted_adapter_sha256"] == frozen.mac_build.digest(Path(frozen.__file__))
    return receipt


def read_snapshots(path):
    log = path / "stderr.log"
    if not log.is_file():
        return []
    marker = "aggregate-phases "
    return [json.loads(line[len(marker):]) for line in log.read_text().splitlines()
            if line.startswith(marker)]


def slots(snapshots, phases, count):
    assert len(snapshots) == count + 1
    assert all(len(sample) == len(phases) for sample in snapshots)
    assert all(len(phase) == 3 and min(phase) >= 0 for sample in snapshots for phase in sample)
    return [dict(zip(phases, record, strict=True)) for record in snapshots[1:]]

def phase_result(records, rows):
    phases = list(records[0])
    groups = {phase: [row[phase][0] / rows for row in records] for phase in phases}
    for key in ("summary_total", "timed_frame_work", "frame_work_excluding_summary"):
        groups[key] = []
    for row in records:
        summary = sum(row[name][0] for name in EXTRA)
        window = sum(row[name][0] for name in ("create", "discard", "replay", "update", "evaluate"))
        groups["summary_total"].append(summary / rows)
        groups["timed_frame_work"].append(window / rows)
        groups["frame_work_excluding_summary"].append((window - summary) / rows)
    return {name: median(values) for name, values in groups.items()}


def measure(args, protocol, receipt):
    proof = frozen.cells.read(args.proof)
    assert proof["protocol_sha256"] == frozen.mac_build.digest(args.protocol) and proof["release"] == receipt
    cases = protocol["case_ids"]
    assert all(proof["cases"][case]["status"] == "eligible" for case in cases)
    blocks, count = protocol["blocks"], protocol["samples"]
    estimates = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    failures = {}
    roots = ((args.output, "EVIDENCE"), (args.release, "RELEASE"), (frozen.REPO, "REPOSITORY"),
             (Path.home(), "HOME_ROOT"))
    with frozen.session(args.protocol) as binding:
        args.output.mkdir()
        deadline = time.monotonic() + protocol["max_seconds"]
        with frozen.archive(args.output / "samples.jsonl.gz") as stream:
            frozen.emit(stream, {"kind": "session", **binding, "release": receipt, "phases": protocol["phases"],
                                 "protocol_sha256": frozen.mac_build.digest(args.protocol),
                                 "proof_sha256": frozen.mac_build.digest(args.proof)})
            for block in range(1, blocks + 1):
                order = 1 if block % 2 else -1
                for case in cases[::order]:
                    for mode in frozen.MODES[::order]:
                        key = f"{case}/{mode}"
                        if key in failures:
                            continue
                        for arm in ("a", "b")[::order]:
                            path = args.output / f"{block:02}" / case / mode / arm
                            frozen.check_host()
                            raw_phases, snapshots, records = [], [], []
                            try:
                                status, _median, records = frozen.process(
                                    args.release / "functions", case, mode, path, proof["cases"][case],
                                    count, deadline, protocol["process_seconds"],
                                )
                                raw_phases = read_snapshots(path)
                                if status == "ok":
                                    snapshots = slots(raw_phases, protocol["phases"], count)
                            except (AssertionError, TimeoutError, ValueError):
                                status = "phase validation failure"
                            frozen.emit(stream, {"kind": "process", "block": block, "arm": arm, "case": case,
                                                 "configuration": mode, "status": status, "records": records,
                                                 "phase_snapshots": raw_phases}, roots)
                            if status != "ok":
                                failures[key] = {"status": status, "block": block, "arm": arm}
                                break
                            for phase, ns in phase_result(snapshots, proof["cases"][case]["input_rows"]).items():
                                estimates[key][phase][arm].append(ns)
                print(f"percentile attribution: block {block}/{blocks}", flush=True)
    result = {}
    for case in cases:
        for mode in frozen.MODES:
            key = f"{case}/{mode}"
            if key in failures:
                result[key] = failures[key]
                continue
            result[key] = {"status": "ok", "phases": {}}
            for name, arms in estimates[key].items():
                a, b = arms["a"], arms["b"]
                assert len(a) == len(b) == blocks
                floor = max(abs(median(a) - median(b)), (max(a) - min(a)) / 2, (max(b) - min(b)) / 2)
                result[key]["phases"][name] = {"ns_per_input_row": median([*a, *b]), "aa_floor": floor}
    frozen.cells.write(args.output / "phases.json", result)


def main():
    cli = argparse.ArgumentParser(description=__doc__)
    sub = cli.add_subparsers(dest="action", required=True)
    for action, names in (("build", ("worktree", "target")), ("preflight", ("release",)),
                          ("measure", ("release", "proof"))):
        parser = sub.add_parser(action)
        for name in ("protocol", *names, "output"):
            parser.add_argument(name, type=lambda value: Path(value).resolve())
    args = cli.parse_args()
    assert not args.output.is_relative_to(frozen.REPO)
    assert frozen.mac_build.git(frozen.REPO, "show", f"HEAD:{args.protocol.relative_to(frozen.REPO)}") == args.protocol.read_text().strip()
    protocol = frozen.cells.read(args.protocol)
    assert protocol["manifest_sha256"] == frozen.mac_build.digest(frozen.MANIFEST)
    assert protocol["schedule_sha256"] == frozen.mac_build.digest(frozen.SCHEDULE)
    assert protocol["blocks"] == 9 and protocol["samples"] == frozen.cells.read(frozen.SCHEDULE)["samples"]["functions"]
    assert protocol["instrument_sha256"] == frozen.mac_build.digest(frozen.BENCHES / "window_aggregates_1408/instrument.py")
    if args.action == "build":
        build(args, protocol)
    else:
        receipt = verify(args, protocol)
        if args.action == "preflight":
            cases = [case for case in frozen.cells.read(frozen.MANIFEST)["functions"] if case["id"] in protocol["case_ids"]]
            assert len(cases) == len(protocol["case_ids"])
            frozen.preflight(args, protocol, receipt, cases)
        else:
            measure(args, protocol, receipt)


if __name__ == "__main__":
    main()
