"""Verify exact results for the non-function baseline cells before timing."""

import argparse
from pathlib import Path
import sys
import time

sys.dont_write_bytecode = True
import cells
from build import digest
from native import BENCHES, HERE, REPO, bounded_timeout, call, legacy_runner, safe_record
from run import source_receipt


def execution(binaries, output, snapshot, specs):
    output.mkdir()
    deadline = time.monotonic() + 7200
    binary = binaries / "execution"
    failures = {}
    for suite in ("parsers", "strings"):
        prefix = cells.PREFIX[suite]
        selection = {r["id"]: r for r in specs if r["suite"] == suite}
        observed = {}
        for case, spec in selection.items():
            modes = ("spark-off", "spark-on") if case.startswith("interval_day_time") else cells.CONFIGURATIONS[suite]
            results = {}
            for mode in modes:
                path = output / suite / case / mode
                status, rows = call(binary, [*prefix, "preflight", snapshot, path, mode, case], path,
                                    bounded_timeout(deadline, 300))
                if status != "ok" or len(rows) != 1:
                    failures.setdefault((suite, case), []).append(f"{mode}: preflight {status}")
                    continue
                row, = rows
                assert row["id"] == case and row["configuration"] == mode
                if row["rows"] != spec["expected_rows"] or not row["source_io"]["counters_complete"]:
                    failures.setdefault((suite, case), []).append(f"{mode}: row count or source counters differ")
                row["ipc_sha256"] = digest(Path(row["ipc_path"]))
                results[mode] = row
            observed[case] = results
            ref_mode = modes[0]
            if ref_mode not in results:
                continue
            ref = results[ref_mode]
            for mode in modes[1:]:
                if mode not in results:
                    continue
                row = results[mode]
                if row["arrow_types"] != ref["arrow_types"] or row["sql_schema"] != ref["sql_schema"]:
                    failures.setdefault((suite, case), []).append(
                        f"{mode}: executed types differ from {ref_mode}: {row['arrow_types']} versus {ref['arrow_types']}")
                    continue
                status, proof = call(binary, ["compare", ref["ipc_path"], row["ipc_path"]],
                                     output / suite / case / f"compare-{mode}", bounded_timeout(deadline, 180))
                if status != "ok" or len(proof) != 1 or proof[0].get("rows_compared") != spec["expected_rows"]:
                    failures.setdefault((suite, case), []).append(f"{mode}: full decoded rows differ from {ref_mode}")
            print(f"{suite} {case} preflight complete", flush=True)
        if suite == "parsers":
            with legacy_runner("execution/run.py") as runner:
                for mode in ("spark-off", "spark-on"):
                    left, right = (observed[name].get(mode) for name in ("interval_day_time", "interval_day_time_input"))
                    if not left or not right:
                        failures.setdefault((suite, "interval_day_time"), []).append(f"{mode}: missing projection comparison")
                        continue
                    try:
                        runner["require_same_source_io"](left["source_io"], right["source_io"])
                    except AssertionError as error:
                        failures.setdefault((suite, "interval_day_time"), []).append(f"{mode}: {error}")
        cells.write(output / f"{suite}.json", safe_record(observed, ((output, "PREFLIGHT"), (snapshot, "SF1"), (Path.home(), "HOME_ROOT"))))
    return failures


def planning(binaries, output, specs):
    output.mkdir()
    delta = output / "delta"
    cells.prepare_planning(binaries, delta)
    failures = {}
    for case in dict.fromkeys(row["id"] for row in specs if row["suite"] == "planning_attribution"):
        status, results = call(binaries / "planning", ["preflight", delta, case], output / case, 180)
        if status != "ok" or len(results) != 4 or any(not r["matches"] or r["extra_delta_io"] for r in results):
            failures["planning_attribution", case] = [f"planning preflight {status}: result, type or scan comparison failed"]
    return failures


def nullif(binaries, output):
    output.mkdir()
    binary = binaries / "nullif_620"
    deadline = time.monotonic() + 2400
    failures = {}
    with legacy_runner("nullif_620/run.py") as runner:
        runner["check_space"].__globals__["EVIDENCE"] = output
        _, records = runner["call"](binary, ["preflight", "nullif", output / "ipc"], output / "full", deadline)
        expected = runner["cells"]("nullif")
        rows = {(r["id"], r["configuration"]): r for r in records if r["kind"] == "preflight"}
        assert len(expected) == 72 and len(rows) == 72 * 3
        sets = []
        for case in expected:
            native = rows[case, "native"]
            comparisons = []
            for mode in ("spark-off", "spark-on"):
                row = rows[case, mode]
                proof = runner["compare"](binary, native["ipc_path"], row["ipc_path"],
                                          output / f"{case}-{mode}", deadline)
                if not proof["equal"] or not row["formula_match"] or not native["formula_match"]:
                    failures.setdefault(("nullif_620", case), []).append(f"{mode}: typed output differs from native or formula")
                comparisons.append({"mode": mode, **proof})
            sets.append({"id": case, "rows": native["rows"], "comparisons": comparisons})
    cells.write(output / "eligibility.json", {"excluded": sorted(case for _, case in failures),
                "binary_sha256": digest(binary), "sets": sets})
    return failures


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("binaries", "fixture", "output"):
        parser.add_argument(name, type=lambda path: Path(path).resolve())
    args = parser.parse_args()
    args.output.mkdir()
    assert not args.output.is_relative_to(REPO)
    build = source_receipt(args.binaries)
    fixture = cells.read(args.fixture / "fixture-proof.json")
    assert fixture["snapshot_matches"] or fixture.get("content_matches")
    settings = HERE / "baseline-cells.json"
    specs = cells.read(settings)["cells"]
    failed = {}
    for name, run in (
        ("planning", lambda: planning(args.binaries, args.output / "planning", specs)),
        ("sf1", lambda: execution(args.binaries, args.output / "sf1", args.fixture / "delta-snapshot", specs)),
        ("nullif", lambda: nullif(args.binaries, args.output / "nullif")),
    ):
        failed.update(run())
        print(f"{name} preflight complete; {len(failed)} exclusions so far", flush=True)
    excluded = {row["label"]: failed[row["suite"], row["id"]]
                for row in specs if (row["suite"], row["id"]) in failed}
    summary = {"source": build["source"], "binaries": build["binaries"], "manifest_sha256": digest(settings),
               "fixture_proof_sha256": digest(args.fixture / "fixture-proof.json"), "excluded": excluded}
    cells.write(args.output / "cell-eligibility.json", summary)


if __name__ == "__main__":
    main()
