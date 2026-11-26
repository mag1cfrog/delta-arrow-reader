"""Translate macOS cell selections to the original benchmark entry points."""

import json
from pathlib import Path
from statistics import median
import subprocess

from build import digest
from native import BENCHES, bounded_timeout, call
import ports

FUNCTIONS = BENCHES / "functions/manifest.json"
CONFIGURATIONS = {
    "functions": ("native", "spark-off", "spark-on"),
    "execution": ("native", "spark-off", "spark-on"),
    "strings": ("native", "spark-off", "spark-on"),
    "parsers": ("native", "spark-off", "spark-on"),
    "planning": ("native-configured", "spark-off", "spark-on", "native-plain"),
    "planning_attribution": ("native-configured", "spark-off", "spark-on", "native-plain"),
    "nullif_620": ("native", "spark-off", "spark-on"),
    "nullif_620_recheck": ("native", "spark-off", "spark-on"),
    "functions_1406": ("native", "spark-off", "spark-on"),
    "regression_windows_1512": ("native", "spark-off", "spark-on"),
    "pending_range_1099": ("native", "spark-off", "spark-on"),
    "pending_range_1099_isolated": ("native", "spark-off", "spark-on"),
    "footprint": ("native",),
}
PREFIX = {"execution": (), "strings": ("--strings-1388",), "parsers": ("--parsers-1389",),
          "casts-date": ("--casts-1385-date",)}
CONFIGURATIONS["casts-date"] = CONFIGURATIONS["parsers"]


def read(path):
    return json.loads(path.read_text())


def write(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def process(binary_dir, cell, sample_count, destination, resources, deadline, block):
    suite, case, config = (cell[k] for k in ("suite", "id", "configuration"))
    assert config in CONFIGURATIONS[suite]
    timeout = bounded_timeout(deadline, cell.get("process_timeout_seconds", 300))
    prefix = ["--macos-samples", sample_count]
    if suite == "functions":
        manifest = BENCHES / Path(cell.get("function_manifest", "functions/manifest.json"))
        args = [*prefix, "measure", manifest, case, config]
    elif suite in PREFIX:
        args = [*prefix, *PREFIX[suite], "measure", resources["snapshot"], destination, config, case]
    elif suite == "planning":
        args = [*prefix, "measure", resources["planning_delta"], config, str(block), "main",
                BENCHES / "planning/eligibility.jsonl", case]
    elif suite == "planning_attribution":
        args = [*prefix, "measure", resources["planning_delta"], config, str(block), "-", case]
    elif suite == "nullif_620":
        family = case.split("-")[0]
        args = [*prefix, "measure", family, config, block, resources["nullif_eligible"], case]
    else:
        return ports.process(binary_dir, cell, sample_count, destination, resources, timeout, block)
    binary = binary_dir / ("execution" if suite in PREFIX else suite)
    status, rows = call(binary, args, destination, timeout)
    return status, rows


def function_set(binary, ids, config, sample_count, destination, deadline, schedule, runner):
    id_file = destination.with_suffix(".ids.json")
    write(id_file, ids)
    return runner["invoke"](binary, ["--macos-samples", sample_count, "measure-set",
                                     FUNCTIONS, id_file, config], destination, schedule, deadline)


def function_preflight(binary, case, destination, deadline, schedule, runner, manifest=FUNCTIONS):
    if case["exclusion"]:
        return {"id": case["id"], **runner["eligibility"](case, {})}
    plans = destination / "plans"
    result = runner["invoke"](binary, ["preflight", manifest, case["id"], plans],
                              destination, schedule, deadline, True)
    if result["status"] == "ok":
        record, = result["records"]
        record = {**record, "status": "ok"}
    else:
        record = result
    verdict = runner["eligibility"](case, record)
    return {"id": case["id"], **verdict, "results": record.get("results", []),
            "plan_hashes": {path.name: digest(path) for path in sorted(plans.glob("*.plan"))}}


def function_samples(suite, rows, cell, count):
    case, config = cell["id"], cell["configuration"]
    complete = [row for row in rows if "samples" in row and row.get("id") == case]
    row, = complete
    assert row["configuration"] == config and row.get("status", "ok") == "ok"
    events = [event for event in rows if event.get("kind") == "retained_sample" and event["id"] == case]
    if events:
        assert row["samples"] == [event["sample"] for event in events]
        assert all(event["types"] == row["types"] and event["configuration"] == config for event in events)
    samples = row["samples"]
    assert len(samples) == count
    if suite == "functions" or suite == "regression_windows_1512":
        assert [sample["iteration"] for sample in samples] == list(range(1, count + 1))
        assert all(sample["input_rows"] == 1_000_000 for sample in samples)
    return row, samples


def metric(cell, rows, count):
    suite, case, config = (cell[k] for k in ("suite", "id", "configuration"))
    if suite in ("functions", "regression_windows_1512", "functions_1406"):
        _, samples = function_samples(suite, rows, cell, count)
        values = [s["increment_ns"] / s["input_rows"] for s in samples]
    elif suite.startswith("planning"):
        row, = rows
        assert (row["id"], row["configuration"]) == (case, config)
        values = row[cell.get("metric", "total_ns")]
    elif suite in PREFIX:
        row, = rows
        assert (row["id"], row["configuration"]) == (case, config)
        assert row["process_peak_rss_bytes"] is None
        samples = row["samples"]
        if expected := cell.get("expected_rows"):
            assert all(sample["rows"] == expected for sample in samples)
        assert all(sample["source_io"]["counters_complete"] for sample in samples)
        values = [sample[cell.get("metric", "query_complete_ns")] for sample in samples]
    elif suite.startswith("pending_range_"):
        completed, = [row for row in rows if row["id"] == case and "samples_ns" in row]
        assert completed["configuration"] == config
        values = completed["samples_ns"]
        retained = [row["elapsed_ns"] for row in rows if row.get("kind") == "retained_sample" and row["id"] == case]
        assert not retained or retained == values
    elif suite == "footprint":
        row, = rows
        assert row["id"] == case and row["exit_code"] == 0
        values = [row["elapsed_ns"]]
    else:
        assert all((row["id"], row["configuration"]) == (case, config) for row in rows)
        assert sorted(row["sample"] for row in rows) == list(range(count))
        assert all(row["planning_calls"] == row["execution_calls"] == 1 for row in rows)
        values = [row[cell.get("metric", "execution_ns")] for row in rows]
    assert len(values) == count
    return median(values)


def prepare_planning(binary_dir, destination):
    result = subprocess.run([binary_dir / "planning", "prepare", destination],
                            capture_output=True, timeout=60, check=True)
    row = json.loads(result.stdout)
    assert row["rows"] == 1024 and row["version"] == 0
