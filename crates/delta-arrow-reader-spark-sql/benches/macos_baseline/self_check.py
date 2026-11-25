"""Check one untimed statement or control through each frozen harness."""

import argparse
from pathlib import Path
import sys
import time

sys.dont_write_bytecode = True
from build import digest
import cells
from native import BENCHES, call, legacy_runner, prefer_performance_cores, safe_record
from run import source_receipt

NULLIF = "nullif-columns-sparse-8192"


def checked(binary, arguments, output, limit=300):
    status, rows = call(binary, arguments, output, limit)
    assert status == "ok", f"{output.name}: {status}"
    return rows


def function(binary, case_id, output):
    case, = [c for c in cells.read(cells.FUNCTIONS)["functions"] if c["id"] == case_id]
    with legacy_runner("functions/run.py") as runner:
        schedule = cells.read(BENCHES / "functions/schedule.json")
        row = cells.function_preflight(binary, case, output, time.monotonic() + 300, schedule, runner)
    assert row["status"] in ("eligible", "same_verified"), row["reason"]
    return row


def execution(binary, snapshot, output, suite, case):
    with legacy_runner(f"{suite}/run.py") as runner:
        if suite == "strings":
            schedule = cells.read(BENCHES / "strings/schedule.json")
            row = runner["invoke"](binary, snapshot, output, schedule, time.monotonic() + 300,
                                   "preflight", "native", case)
        else:
            row = runner["invoke"](["taskset", "-c", "2", binary, "preflight", snapshot, output, "native", case],
                                   output, 300, 12 * 1024**3)
    assert row["kind"] == "preflight_result" and row["id"] == case and row["rows"] == 6_001_215
    assert row["source_io"]["counters_complete"] and row["arrow_types"]
    return row


def nullif(binary, output, *, recheck):
    name = "nullif_620_recheck/run.py" if recheck else "nullif_620/run.py"
    output.mkdir()
    with legacy_runner(name) as runner:
        args = ["preflight", "nullif", output / "ipc", 1, "-", NULLIF]
        deadline = time.monotonic() + 300
        result = runner["call"](binary, args, output / "preflight", deadline)
        rows = result if recheck else result[1]
        assert len(rows) == 3 and {row["configuration"] for row in rows} == {"native", "spark-off", "spark-on"}
        assert all(row["id"] == NULLIF and row["formula_match"] and row["rows"] == 262144 for row in rows)
        native = next(row for row in rows if row["configuration"] == "native")
        for row in rows:
            assert row["executed_types"] == native["executed_types"]
            with Path(row["ipc_path"]).open("rb") as data:
                assert data.read(6) == b"ARROW1"
        receipt = output / "eligibility.json"
        cells.write(receipt, {"excluded": []})
        return rows


def pending(binary, output, *, isolated):
    output.mkdir()
    if isolated:
        with legacy_runner("pending_range_1099_isolated/run.py") as runner:
            status, rows = runner["invoke"](binary, ["ordinary-trace", runner["NATIVE_CONTROLS"]],
                                            output, "control", time.monotonic() + 300)
        assert status == "ok" and len(rows) == 3
        return rows
    with legacy_runner("pending_range_1099/run.py") as runner:
        original = cells.read(runner["MANIFEST"])
        shape, = [shape for shape in original["shapes"] if shape["id"] == "constant"]
        manifest = output / "single-shape.json"
        cells.write(manifest, {**original, "shapes": [shape], "row_counts": original["row_counts"][:1],
                               "batch_sizes": [original["batch_sizes"][1]]})
        rows = runner["invoke"]({"binary": binary}, ["trace", manifest], output, "trace")
    assert rows and all(row["observation"]["error"] is None for row in rows)
    grouped = {}
    for row in rows:
        case, config = row["id"].rsplit(":", 1)
        grouped.setdefault(case, {})[config] = row["observation"]
    assert all(len(results) == 3 and results["native"] == results["spark-off"] == results["spark-on"]
               for key, results in grouped.items() if key.startswith("constant:"))
    return {"statements": len(grouped), "mode_records": len(rows)}


def footprint(binary, destination, snapshot):
    with legacy_runner("footprint/native.py") as runner:
        case, = [row for row in runner["MANIFEST"]["cases"] if row["id"] == "trivial"]
        runner["MANIFEST"]["snapshot"] = str(snapshot)
        inputs = destination / "inputs"
        destination.mkdir()
        runner["write_query_inputs"](inputs)
        values = destination / "values.json"
        args = runner["command"](binary, case, inputs, values)
        result = runner["invoke"](args, destination / "result", 120)
        comparison = runner["compare"](values, values)
    assert comparison["status"] == "equal" and comparison["rows"] == 1 and comparison["executed_schema"]
    return {"exit_code": result["exit_code"], "rows": comparison["rows"]}


def main():
    cli = argparse.ArgumentParser(description=__doc__)
    for name in ("binaries", "output", "fixture", "cli"):
        cli.add_argument(name, type=lambda path: Path(path).resolve())
    args = cli.parse_args()
    prefer_performance_cores()
    release = source_receipt(args.binaries)
    if args.cli.is_dir():
        receipt = cells.read(args.cli / "receipt.json")
        args.cli = args.cli / "dar"
        assert digest(args.cli) == receipt["binary_sha256"]
    proof = cells.read(args.fixture / "fixture-proof.json")
    assert proof["snapshot_matches"] or proof.get("content_matches")
    snapshot = args.fixture / "delta-snapshot"
    args.output.mkdir()
    b, w = args.binaries, args.output
    delta = w / "planning-delta"
    cells.prepare_planning(b, delta)

    def reduced():
        with legacy_runner("functions_1406/run.py") as runner:
            case = runner["cases"]()[0]["id"]
            rows = runner["probe"](b / "functions_1406_driver", "reference", ["preflight", cells.FUNCTIONS,
                                   case, w / "functions_1406-plans"], w / "functions_1406.jsonl")
        row, = rows
        assert row["id"] == case and row["status"] == "ok" and len(row["results"]) >= 2
        return row

    def planning():
        with legacy_runner("planning/run.py") as runner:
            runner["self_check"]()
        rows = checked(b / "planning", ["preflight", delta, "stress/wide_400"], w / "planning")
        assert len(rows) == 4 and all(row["matches"] and not row["extra_delta_io"] for row in rows)
        return rows

    def attribution():
        output = w / "planning_attribution"
        output.mkdir()
        with legacy_runner("planning_attribution/run.py") as runner:
            rows = runner["invoke"](b / "planning_attribution", ["trace", delta, "spark-off", "check", "-", "stress/wide_400"],
                                    output, "wide")
        row, = rows
        assert row["id"] == "stress/wide_400" and row["physical"]
        return row

    checks = {
        "execution": lambda: execution(b / "execution", snapshot, w / "execution", "execution", "projection_five"),
        "functions": lambda: function(b / "functions", "scalar.string.left", w / "functions"),
        "functions_1406": reduced,
        "nullif_620": lambda: nullif(b / "nullif_620", w / "nullif_620", recheck=False),
        "nullif_620_recheck": lambda: nullif(b / "nullif_620", w / "nullif_620_recheck", recheck=True),
        "pending_range_1099": lambda: pending(b / "pending_range_isolated", w / "pending_range_1099", isolated=False),
        "pending_range_1099_isolated": lambda: pending(b / "pending_range_isolated", w / "pending_range_1099_isolated", isolated=True),
        "planning": planning,
        "planning_attribution": attribution,
        "regression_windows_1512": lambda: function(b / "functions", "window.window.regr_count", w / "regression_windows_1512"),
        "strings": lambda: execution(b / "execution", snapshot, w / "strings", "strings", "substring_only"),
        "footprint": lambda: footprint(args.cli, w / "footprint", snapshot),
    }
    records = {"source": release["source"], "checks": {}}
    bounds = ((w, "EVIDENCE"), (b, "RELEASE"), (args.fixture, "FIXTURE"), (BENCHES, "BENCHES"), (Path.home(), "HOME_ROOT"))
    for name, check in checks.items():
        try:
            row = safe_record(check(), bounds)
        except (AssertionError, KeyError, OSError, RuntimeError, ValueError) as error:
            records["checks"][name] = {"status": "failed", "reason": safe_record(str(error), bounds)}
        else:
            records["checks"][name] = {"status": "passed", "result": row}
        print(name, records["checks"][name]["status"], flush=True)
        cells.write(w / "self-check.json", records)
    assert all(row["status"] == "passed" for row in records["checks"].values())


if __name__ == "__main__":
    main()
