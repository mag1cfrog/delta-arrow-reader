#!/usr/bin/env python3
"""Attribute #1389 row parsers using the existing SF1 execution helpers."""

import difflib
import gzip
import importlib.util
import json
import pathlib
import platform
import subprocess
import sys
import time

SUITE = pathlib.Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("string_attribution", SUITE.parent / "strings/run.py")
strings = importlib.util.module_from_spec(spec)
spec.loader.exec_module(strings)
execution, aggregation = strings.execution, strings.aggregation
WORKSPACE, REPORTS = strings.WORKSPACE, strings.REPORTS
REPOSITORY = WORKSPACE.parents[1]
BASELINE = "298015f9def26cc97a271bdbc0316e64083d7874"
PREFIX = "parsers-1389"
IDS = ("date_fallback", "timestamp_ltz", "timestamp_ntz", "interval_year_month",
       "interval_day_time", "interval_calendar", "date_format_string", "interval_day_time_input")
PROJECTION = ("interval_day_time", "interval_day_time_input")
INTERVALS = ("interval_year_month", "interval_day_time", "interval_calendar", "interval_day_time_input")
MODES = ("native", "spark-off", "spark-on")
PRODUCT_PATHS = ("Cargo.toml", "Cargo.lock", "crates/delta-arrow-reader-core",
                 "crates/delta-arrow-reader-cli", "crates/delta-arrow-reader-python",
                 "crates/delta-arrow-reader-spark-sql/Cargo.toml",
                 "crates/delta-arrow-reader-spark-sql/Cargo.lock",
                 "crates/delta-arrow-reader-spark-sql/src", "crates/delta-arrow-reader-spark-sql/vendor")


def product_source_objects():
    subprocess.run(["git", "diff", "--exit-code", "HEAD", "--", *PRODUCT_PATHS],
                   cwd=REPOSITORY, check=True, capture_output=True)
    return {path: subprocess.check_output(["git", "rev-parse", "HEAD:" + path],
                                         cwd=REPOSITORY, text=True).strip() for path in PRODUCT_PATHS}


def modes(query):
    return MODES[1:] if query in PROJECTION else MODES


def expected_runs():
    runs = []
    for replica in range(1, 4):
        for index, query in enumerate(IDS):
            for mode in (MODES[1:] if query in PROJECTION else MODES[:1]):
                for arm in (("a", "b") if (replica + index) % 2 else ("b", "a")):
                    runs.append(dict(series="noise", configuration=mode, query=query, replica=replica,
                                     arm=arm, process=f"noise-{replica:02}-{query}-{mode}-{arm}"))
    for replica in range(1, 4):
        for index, query in enumerate(IDS):
            configurations = modes(query)
            offset = (replica - 1 + index) % len(configurations)
            for mode in configurations[offset:] + configurations[:offset]:
                runs.append(dict(series="main", configuration=mode, query=query, replica=replica,
                                 process=f"main-{replica:02}-{query}-{mode}"))
    return runs


def load_and_validate_workload():
    manifest, schedule = (execution.read(SUITE / name) for name in ("queries.json", "schedule.json"))
    original = execution.read(SUITE.parent / "casts/queries.json")
    assert manifest["source_commit"] == BASELINE
    assert [query["id"] for query in manifest["queries"]] == list(IDS)
    for key in ("dimension", "source_scale_factor", "stored_columns", "snapshot_columns"):
        assert manifest[key] == original[key], key
    additions = [f"CASE WHEN l_orderkey%97=0 THEN CAST(NULL AS VARCHAR) ELSE {expression} END AS {name}"
                 for name, expression in manifest["input_columns"].items()]
    assert manifest["common_view_sql"] == original["common_view_sql"].replace(
        " FROM raw_lineitem", "," + ",".join(additions) + " FROM raw_lineitem")
    assert manifest["queries"][4]["input_column"] == manifest["queries"][-1]["input_column"]
    assert all(query["ordered"] is False and query["related_issues"] == [] for query in manifest["queries"])
    counts = execution.read(SUITE / "interval-input-count.json")
    assert counts["source_rows"] == 6_001_215 and counts["interval_rows"] == 8_184
    assert counts["null_rows"] == 90 and counts["keys_unique"] and counts["line_numbers_in_1_to_7"]
    assert counts["predicate"] == "l_orderkey BETWEEN 1 AND 8192"
    for query in manifest["queries"]:
        predicate = counts["predicate"] if query["id"] in INTERVALS else None
        assert query["input_predicate"] == predicate
        assert query["expected_rows"] == counts["interval_rows" if predicate else "source_rows"]
        suffix = " FROM bench" + (" WHERE " + predicate if predicate else "")
        assert query["native_sql"].endswith(suffix) and query["spark_sql"].endswith(suffix)
    original_schedule = execution.read(SUITE.parent / "casts/schedule.json")
    for key, value in original_schedule.items():
        if key not in ("runs", "schedule_timeout_seconds", "preflight_timeout_seconds"):
            assert schedule[key] == value, key
    assert schedule["schedule_timeout_seconds"] == 3600 and schedule["preflight_timeout_seconds"] == 1800
    assert schedule["runs"] == expected_runs()
    return manifest, schedule


def command(binary, *args):
    return [str(binary), "--parsers-1389", *map(str, args)]


def bound_hashes():
    paths = [SUITE / name for name in ("queries.json", "schedule.json", "run.py", "interval-input-count.json")]
    paths += [WORKSPACE / path for path in (
        "benches/execution.rs", "benches/execution/run.py", "benches/execution/summarize.py",
        "benches/strings/run.py", "tests/support/delta_reads.rs", "tests/support/spark_results.rs",
        "Cargo.toml", "Cargo.lock", "benches/reports/execution-1380-fixture.json",
        "benches/reports/parsers-1389-protocol.md", "benches/reports/parsers-1389-source-inventory.md",
        "benches/reports/parsers-1389-preflight.json.gz")]
    return {str(path.relative_to(WORKSPACE)): execution.digest(path) for path in paths}


def comparison_failure(binary, left, right, work, deadline, schedule):
    proof = strings.compare_results(binary, left["ipc_path"], right["ipc_path"], work, deadline,
                                    schedule["preflight_address_space_bytes"])
    reason = proof.get("mismatch")
    if proof["equal"]:
        assert proof["rows_compared"] == left["rows"] == right["rows"], "incomplete decoded comparison"
        if left["sql_schema"] != right["sql_schema"]:
            reason = "executed SQL types differ"
        else:
            try:
                execution.require_same_source_io(left["source_io"], right["source_io"])
            except AssertionError as error:
                reason = str(error)
    return proof, reason


def expected_type(query, result):
    actual = result["arrow_types"]
    assert actual[:2] == ["Int64", "Int32"] and len(actual) == 3, "unexpected keys or result width"
    actual_type = actual[-1].replace("\N{MICRO SIGN}", "u")
    expected = query["expected_arrow_type"]
    return actual_type in ("Utf8", "Utf8View", "LargeUtf8") if expected in ("Utf8", "string") else actual_type == expected


def preflight(binary, snapshot, destination, manifest, schedule):
    fixture = strings.check_fixture(snapshot)
    counts = execution.read(SUITE / "interval-input-count.json")
    assert counts["source_rows"] == fixture["rows"]
    deadline = time.monotonic() + schedule["preflight_timeout_seconds"]
    check = execution.invoke(command(binary, "check"), destination / "self-check",
                             execution.remaining_timeout(deadline, 30), schedule["preflight_address_space_bytes"])
    assert check == dict(kind="self_check", status="passed")
    results, comparisons, excluded, eligible = [], [], {}, []
    for query in manifest["queries"]:
        name, outputs = query["id"], {}
        for mode in modes(name):
            print("preflight", name, mode, flush=True)
            result = strings.invoke(binary, snapshot, destination / name / mode, schedule, deadline,
                                    "preflight", mode, name)
            assert result["id"] == name and result["configuration"] == mode
            strings.validate_telemetry(result, schedule)
            result["ipc_sha256"] = execution.digest(pathlib.Path(result["ipc_path"]))
            outputs[mode] = result
            results.append(result)
        failures = []
        pairs = [("spark-off", "spark-on")] if name in PROJECTION else [("native", mode) for mode in MODES[1:]]
        for left_mode, right_mode in pairs:
            left, right = outputs[left_mode], outputs[right_mode]
            difference = "".join(difflib.unified_diff(left["physical_plan"].splitlines(keepends=True),
                                                     right["physical_plan"].splitlines(keepends=True),
                                                     fromfile=left_mode, tofile=right_mode))
            (destination / name / (left_mode + "-" + right_mode + ".plan.diff")).write_text(difference)
            proof, reason = comparison_failure(binary, left, right, destination / name / ("compare-" + right_mode), deadline, schedule)
            if proof["equal"] and left["rows"] != query["expected_rows"]:
                reason = "row count differs from the complete declared input"
            if not all(expected_type(query, output) for output in (left, right)):
                reason = "executed Arrow type differs from the declared shape"
            comparisons.append(dict(proof, id=name, left=left_mode, right=right_mode))
            if name in PROJECTION:
                assert reason is None, f"Spark-only ANSI outcomes differ: {name}: {reason}"
            elif reason:
                failures.append(dict(configuration=right_mode, reason=reason))
        assert len({result["rows"] for result in outputs.values()}) != 1 or next(iter(outputs.values()))["rows"] == query["expected_rows"], "all outputs have the same incorrect declared input row count"
        if failures:
            excluded[name] = failures
        else:
            eligible.append(name)
    # The control projects the same text and must observe exactly the same source traffic.
    indexed = {(result["id"], result["configuration"]): result for result in results}
    for mode in MODES[1:]:
        execution.require_same_source_io(indexed[PROJECTION[0], mode]["source_io"], indexed[PROJECTION[1], mode]["source_io"])
    full = dict(status="complete", results=results, comparisons=comparisons, input_count_reference=counts,
                eligible_shapes=eligible, excluded_shapes=excluded)
    strings.write_json(destination / "preflight.json", full)
    with (REPORTS / (PREFIX + "-preflight.json.gz")).open("xb") as output:
        with gzip.GzipFile(fileobj=output, mode="wb", mtime=0) as compressed:
            compressed.write((json.dumps(full, indent=2) + "\n").encode())
    receipt = dict(status="complete", baseline_commit=BASELINE, product_source_objects=product_source_objects(),
                   input_count_reference=counts,
                   binary_sha256=execution.digest(binary), snapshot_root=str(snapshot),
                   snapshot_log_sha256=fixture["derived_delta_log_sha256"], file_hashes=bound_hashes(),
                   eligible_shapes=eligible, excluded_shapes=excluded,
                   preflight_result_count=len(results), exact_comparison_count=len(comparisons),
                   expected_rows={q: {mode: indexed[q, mode]["rows"] for mode in modes(q)} for q in eligible},
                   expected_source_io={q: {mode: {key: indexed[q, mode]["source_io"][key] for key in (
                       "distinct_source_files", "source_file_count", "source_bytes_received", "recorded_range_bytes"
                   )} for mode in modes(q)} for q in eligible})
    strings.write_json(SUITE / "preflight-receipt.json", receipt)


def validate_receipt(binary, snapshot):
    receipt = execution.read(SUITE / "preflight-receipt.json")
    assert receipt["status"] == "complete" and receipt["baseline_commit"] == BASELINE
    assert receipt["product_source_objects"] == product_source_objects()
    assert receipt["file_hashes"] == bound_hashes(), "frozen source or evidence changed"
    assert receipt["binary_sha256"] == execution.digest(binary), "binary differs from untimed preflight"
    assert receipt["snapshot_root"] == str(snapshot)
    assert receipt["snapshot_log_sha256"] == strings.check_fixture(snapshot)["derived_delta_log_sha256"]
    assert receipt["preflight_result_count"] == 22 and receipt["exact_comparison_count"] == 14
    assert set(receipt["eligible_shapes"]) | set(receipt["excluded_shapes"]) == set(IDS)
    assert not set(receipt["eligible_shapes"]) & set(receipt["excluded_shapes"])
    assert set(PROJECTION) <= set(receipt["eligible_shapes"])
    assert receipt["input_count_reference"] == execution.read(SUITE / "interval-input-count.json")
    for query in execution.read(SUITE / "queries.json")["queries"]:
        if query["id"] in receipt["eligible_shapes"]:
            assert receipt["expected_rows"][query["id"]] == {mode: query["expected_rows"] for mode in modes(query["id"])}
    return receipt


def summarize(destination, output):
    _, schedule = load_and_validate_workload()
    complete = execution.read(destination / "complete.json")
    receipt = execution.read(SUITE / "preflight-receipt.json")
    assert complete["eligible_shapes"] == receipt["eligible_shapes"]
    assert complete["excluded_shapes"] == receipt["excluded_shapes"] and complete["concurrency_processes"] == 0
    with gzip.open(destination / "samples.jsonl.gz", "rt") as source:
        records = [json.loads(line) for line in source]
    runs = [run for run in schedule["runs"] if run["query"] in complete["eligible_shapes"]]
    assert len(records) == len(runs) == complete["query_processes"]
    for record, run in zip(records, runs, strict=True):
        assert {key: record[key] for key in run} == run
        assert record["kind"] == "query_samples" and record["source_commit"] == BASELINE
        strings.validate_samples(record, run["query"], run["configuration"], receipt, schedule)
    data = aggregation.aggregate(records)
    shapes = summarize_shapes(data, complete["excluded_shapes"])
    output.mkdir(parents=True, exist_ok=True)
    summary = dict(shapes=shapes, process_records=len(records), excluded_shapes=complete["excluded_shapes"],
                   samples_sha256=execution.digest(destination / "samples.jsonl.gz"), profile_cases=[])
    strings.write_json(output / (PREFIX + "-summary.json"), summary)
    strings.write_json(output / (PREFIX + "-metrics.json"), [dict(series=k[0], arm=k[1], configuration=k[2], id=k[3], metric=k[4], **v) for k, v in data.items()])
    return summary


def summarize_shapes(data, excluded):
    shapes = []
    def timing(query, mode, series="main", arm=None):
        return data[series, arm, mode, query, "query_complete_ns"]
    for query in IDS[:-1]:
        if query in excluded:
            shapes.append(dict(id=query, excluded=excluded[query]))
            continue
        configurations = {}
        for mode in modes(query):
            measured = timing(query, mode)
            rows = data["main", None, mode, query, "rows"]["median"]
            if query == PROJECTION[0]:
                reference = timing(PROJECTION[1], mode)
                arms = [timing(query, mode, "noise", arm)["median"] - timing(PROJECTION[1], mode, "noise", arm)["median"] for arm in ("a", "b")]
                noise = abs(arms[0] - arms[1])
            else:
                reference = timing(query, "native")
                noise = abs(timing(query, "native", "noise", "a")["median"] - timing(query, "native", "noise", "b")["median"])
            delta = measured["median"] - reference["median"]
            configurations[mode] = dict(query_complete_ns=measured, query_complete_ms=measured["median"] / 1e6,
                                        ns_per_row=measured["median"] / rows, rows=rows,
                                        reference_query_complete_ns=reference, delta_ns=delta, delta_ns_per_row=delta / rows,
                                        noise_ns=noise, noise_ms=noise / 1e6, noise_ns_per_row=noise / rows,
                                        completion_gap_exceeds_noise=None if query in PROJECTION else delta > noise)
        shapes.append(dict(id=query, reference="same-mode input projection" if query in PROJECTION else "native",
                           configurations=configurations))
    return shapes


def self_check():
    manifest, schedule = load_and_validate_workload()
    execution.source_io_self_check()
    aggregation.self_check()
    assert len(schedule["runs"]) == len({run["process"] for run in schedule["runs"]}) == 126
    assert sum(run["series"] == "noise" for run in schedule["runs"]) == 60
    assert sum(len(modes(query)) for query in IDS) == 22
    assert command("execution", "check") == ["execution", "--parsers-1389", "check"]
    timestamp = manifest["queries"][1]
    assert expected_type(timestamp, dict(arrow_types=["Int64", "Int32", 'Timestamp(\N{MICRO SIGN}s, "UTC")']))
    for wrong in ('Timestamp(ns, "UTC")', "Timestamp(us)", "Date32"):
        assert not expected_type(timestamp, dict(arrow_types=["Int64", "Int32", wrong]))
    summary_self_check(schedule)
    child = subprocess.run([sys.executable, "-O", str(pathlib.Path(__file__).resolve()), "--check"], capture_output=True, text=True, timeout=10)
    assert child.returncode != 0 and child.stderr.strip() == "validation requires Python assertions"


def summary_self_check(schedule):
    io = dict(distinct_source_files=["one.parquet"], source_file_count=1,
              source_bytes_received=10, recorded_range_bytes=10, counters_complete=True)
    records = []
    for run in schedule["runs"]:
        q, mode = run["query"], run["configuration"]
        duration = 1000 if q == PROJECTION[0] else 100 if q == PROJECTION[1] else 500 if mode == "native" else 600
        if run.get("arm") == "b":
            duration += 10 if q == PROJECTION[0] else 3 if q == PROJECTION[1] else 1
        sample = {key: 0 for key in aggregation.TIMINGS + aggregation.COUNTERS}
        sample.update(query_complete_ns=duration, rows=2, output_partitions=2,
                      source_io=dict(io, **{key: io.get(key, 0) for key in aggregation.IO}))
        records.append(dict(run, kind="query_samples", source_commit=BASELINE, id=q,
                            process_peak_rss_bytes=100, setup_rss_bytes=100, samples=[sample] * 3))
    shapes = summarize_shapes(aggregation.aggregate(records), {})
    native = shapes[0]["configurations"]["spark-off"]
    projection = shapes[4]["configurations"]["spark-on"]
    assert (native["delta_ns"], native["noise_ns"], native["completion_gap_exceeds_noise"]) == (100, 1, True)
    assert (projection["delta_ns"], projection["noise_ns"], projection["delta_ns_per_row"]) == (900, 7, 450)
    assert projection["completion_gap_exceeds_noise"] is None
    excluded = summarize_shapes(aggregation.aggregate(records), {IDS[0]: "mismatch"})
    assert excluded[0] == dict(id=IDS[0], excluded="mismatch")


# Bind only this controller's imported helpers; historical files are unchanged.
strings.SUITE = execution.SUITE = SUITE
strings.IDS, strings.PREFIX, strings.BASELINE = IDS, PREFIX, BASELINE
strings.command, strings.validate_receipt = command, validate_receipt

if __name__ == "__main__":
    if sys.flags.optimize:
        raise SystemExit("validation requires Python assertions")
    self_check()
    if sys.argv[1:] == ["--check"]:
        print("seven parser kernels, one projection, fixed schedule and helper checks passed")
    elif len(sys.argv) == 4 and sys.argv[1] == "summarize":
        summarize(pathlib.Path(sys.argv[2]).resolve(), pathlib.Path(sys.argv[3]).resolve())
    elif len(sys.argv) == 5 and sys.argv[1] in ("preflight", "measure"):
        assert platform.system() == "Linux", "execution/RSS runs require Linux"
        manifest, schedule = load_and_validate_workload()
        binary, snapshot, destination = map(lambda path: pathlib.Path(path).resolve(), sys.argv[2:])
        assert not destination.exists(), "retain existing evidence; no unchanged reruns"
        destination.mkdir(parents=True)
        metadata = execution.metadata(binary, snapshot, manifest, schedule)
        metadata.update(product_source_objects=product_source_objects(), controller_sha256=execution.digest(pathlib.Path(__file__)))
        strings.write_json(destination / "metadata.json", metadata)
        (preflight if sys.argv[1] == "preflight" else strings.measure)(binary, snapshot, destination, manifest, schedule)
    else:
        raise SystemExit("usage: run.py --check | preflight/measure BINARY SNAPSHOT NEW_DIRECTORY | summarize RUN_DIRECTORY OUTPUT_DIRECTORY")
