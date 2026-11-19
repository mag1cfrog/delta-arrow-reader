#!/usr/bin/env python3
"""Run the two affected #1388 SUBSTRING shapes through the frozen string driver."""

import gzip
import importlib.util
import json
import pathlib
import platform
import subprocess
import sys
import time

SUITE = pathlib.Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("string_attribution", SUITE.parent / "run.py")
strings = importlib.util.module_from_spec(spec)
spec.loader.exec_module(strings)
execution = strings.execution
WORKSPACE = strings.WORKSPACE
REPOSITORY = WORKSPACE.parents[1]
REPORTS = strings.REPORTS
PRODUCT_BASELINE = "7cbb9dbf0ca0e625f0fe4a50f12292e0720788b7"
QUERY_MANIFEST_ORIGIN = strings.BASELINE
IDS = ("string_functions", "substring_only")
PREFIX = "strings-1388-substring"
PRODUCT_PATHS = (
    "Cargo.toml", "Cargo.lock", "crates/delta-arrow-reader-core",
    "crates/delta-arrow-reader-cli", "crates/delta-arrow-reader-python",
    "crates/delta-arrow-reader-spark-sql/Cargo.toml",
    "crates/delta-arrow-reader-spark-sql/Cargo.lock",
    "crates/delta-arrow-reader-spark-sql/src",
    "crates/delta-arrow-reader-spark-sql/vendor",
)


def product_source_objects():
    subprocess.run(["git", "diff", "--exit-code", "HEAD", "--", *PRODUCT_PATHS], cwd=REPOSITORY, check=True, capture_output=True)
    return {path: subprocess.check_output(["git", "rev-parse", "HEAD:" + path], cwd=REPOSITORY, text=True).strip()
            for path in PRODUCT_PATHS}


def load_and_validate_workload():
    manifest, schedule = (execution.read(SUITE / name) for name in ("queries.json", "schedule.json"))
    original_manifest = execution.read(SUITE.parent / "queries.json")
    original_schedule = execution.read(SUITE.parent / "schedule.json")
    assert manifest == dict(original_manifest, queries=[query for query in original_manifest["queries"] if query["id"] in IDS])
    assert [query["id"] for query in manifest["queries"]] == list(IDS)
    assert schedule == dict(original_schedule, runs=[run for run in original_schedule["runs"] if run["query"] in IDS], profile_max_cases=2)
    return manifest, schedule


original_bound_hashes = strings.bound_hashes


def bound_hashes():
    hashes = original_bound_hashes()
    for path in (
        SUITE.parent / "run.py", SUITE.parent / "queries.json", SUITE.parent / "schedule.json",
        REPORTS / "strings-1388-preflight.json.gz",
        WORKSPACE / "vendor/sail/crates/sail-function/src/scalar/string/spark_substring.rs",
        WORKSPACE / "vendor/sail/crates/sail-function/tests/fixtures/substring-row-cost-1388-inputs.json",
        WORKSPACE / "tests/substring_row_cost.rs", WORKSPACE / "UPSTREAM.md",
    ):
        hashes[str(path.relative_to(WORKSPACE))] = execution.digest(path)
    return hashes


def preflight(binary, snapshot, destination, manifest, schedule):
    # Reuse the full export, native comparison and fail-closed telemetry checks.
    strings.preflight(binary, snapshot, destination, manifest, schedule)
    full = execution.read(destination / "preflight.json")
    with gzip.open(REPORTS / "strings-1388-preflight.json.gz", "rt") as source:
        baseline = {(result["id"], result["configuration"]): result for result in json.load(source)["results"]}
    deadline = time.monotonic() + schedule["preflight_timeout_seconds"]
    comparisons = []
    for result in full["results"]:
        old = baseline[result["id"], result["configuration"]]
        assert execution.digest(pathlib.Path(old["ipc_path"])) == old["ipc_sha256"], "baseline IPC changed"
        comparison = strings.compare_results(binary, old["ipc_path"], result["ipc_path"],
                                             destination / result["id"] / ("baseline-" + result["configuration"]),
                                             deadline, schedule["preflight_address_space_bytes"])
        assert comparison["equal"], "existing result differs; stop before timing"
        assert comparison["rows_compared"] == old["rows"] == result["rows"]
        assert old["sql_schema"] == result["sql_schema"]
        assert old["arrow_types"] == result["arrow_types"]
        strings.validate_telemetry(result, schedule)
        execution.require_same_source_io(result["source_io"], old["source_io"])
        comparisons.append(dict(comparison, id=result["id"], configuration=result["configuration"]))
    full["baseline_comparisons"] = comparisons
    strings.write_json(destination / "preflight.json", full)
    with gzip.GzipFile(filename=str(REPORTS / (PREFIX + "-preflight.json.gz")), mode="wb", mtime=0) as output:
        output.write((json.dumps(full, indent=2) + "\n").encode())
    receipt = execution.read(SUITE / "preflight-receipt.json")
    receipt.update(baseline_commit=PRODUCT_BASELINE, query_manifest_origin=QUERY_MANIFEST_ORIGIN,
                   source_head_commit=subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=REPOSITORY, text=True).strip(),
                   product_source_objects=product_source_objects(), baseline_comparison_count=len(comparisons),
                   file_hashes=bound_hashes())
    strings.write_json(SUITE / "preflight-receipt.json", receipt)


def validate_receipt(binary, snapshot):
    receipt = execution.read(SUITE / "preflight-receipt.json")
    assert receipt["status"] == "complete" and receipt["baseline_commit"] == PRODUCT_BASELINE
    assert receipt["query_manifest_origin"] == QUERY_MANIFEST_ORIGIN
    assert receipt["product_source_objects"] == product_source_objects()
    assert receipt["file_hashes"] == bound_hashes(), "frozen source or evidence changed"
    assert receipt["binary_sha256"] == execution.digest(binary), "binary differs from untimed preflight"
    assert receipt["snapshot_root"] == str(snapshot)
    fixture = strings.check_fixture(snapshot)
    assert receipt["snapshot_log_sha256"] == fixture["derived_delta_log_sha256"]
    assert receipt["preflight_result_count"] == receipt["baseline_comparison_count"] == 6
    assert receipt["exact_comparison_count"] >= 2 * len(receipt["eligible_shapes"])
    assert set(receipt["eligible_shapes"]) | set(receipt["excluded_shapes"]) == set(IDS)
    assert not set(receipt["eligible_shapes"]) & set(receipt["excluded_shapes"])
    return receipt


def self_check():
    _, schedule = load_and_validate_workload()
    execution.source_io_self_check()
    strings.aggregation.self_check()
    assert len(schedule["runs"]) == len({run["process"] for run in schedule["runs"]}) == 30
    for query in IDS:
        assert sum(run["query"] == query for run in schedule["runs"]) == 15
    assert not strings.completion_gap_exceeds_noise(100, 110, 10)
    assert strings.completion_gap_exceeds_noise(100, 111, 10)
    assert strings.command(pathlib.Path("execution"), "check") == ["execution", "--strings-1388", "check"]
    child = subprocess.run([sys.executable, "-O", str(pathlib.Path(__file__).resolve()), "--check"], capture_output=True, text=True, timeout=10)
    assert child.returncode != 0 and child.stdout == "" and child.stderr.strip() == "validation requires Python assertions"


# These explicit bindings restrict the unchanged execution, validation and summary
# functions to this slice's manifests and evidence. The Rust selector stays frozen.
strings.SUITE = SUITE
strings.IDS = IDS
strings.PREFIX = PREFIX
strings.load_and_validate_workload = load_and_validate_workload
strings.bound_hashes = bound_hashes
strings.validate_receipt = validate_receipt


if __name__ == "__main__":
    if sys.flags.optimize:
        raise SystemExit("validation requires Python assertions")
    self_check()
    if sys.argv[1:] == ["--check"]:
        print("two affected shapes, fixed schedule, source I/O and noise checks passed")
    elif len(sys.argv) == 4 and sys.argv[1] == "summarize":
        strings.summarize(pathlib.Path(sys.argv[2]).resolve(), pathlib.Path(sys.argv[3]).resolve())
    elif len(sys.argv) == 5 and sys.argv[1] in ("preflight", "measure"):
        assert platform.system() == "Linux", "execution/RSS runs require Linux"
        manifest, schedule = load_and_validate_workload()
        binary, snapshot, destination = map(lambda path: pathlib.Path(path).resolve(), sys.argv[2:])
        assert not destination.exists(), "retain existing evidence; no unchanged reruns"
        destination.mkdir(parents=True)
        metadata = execution.metadata(binary, snapshot, manifest, schedule)
        metadata.update(baseline_commit=PRODUCT_BASELINE, query_manifest_origin=QUERY_MANIFEST_ORIGIN,
                        product_source_objects=product_source_objects(),
                        queries_sha256=execution.digest(SUITE / "queries.json"), schedule_sha256=execution.digest(SUITE / "schedule.json"),
                        controller_sha256=execution.digest(pathlib.Path(__file__)))
        strings.write_json(destination / "metadata.json", metadata)
        (preflight if sys.argv[1] == "preflight" else strings.measure)(binary, snapshot, destination, manifest, schedule)
    else:
        raise SystemExit("usage: run.py --check | preflight/measure BINARY SNAPSHOT NEW_DIRECTORY | summarize RUN_DIRECTORY OUTPUT_DIRECTORY")
