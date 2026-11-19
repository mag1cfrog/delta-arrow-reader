#!/usr/bin/env python3
"""Compare and measure only the four #1388 string shapes on the frozen SF1 input."""

import ctypes
import difflib
import gzip
import importlib.util
import json
import os
import pathlib
import platform
import re
import signal
import subprocess
import sys
import tempfile
import time

SUITE = pathlib.Path(__file__).resolve().parent
WORKSPACE = SUITE.parents[1]
REPORTS = WORKSPACE / "benches/reports"
BASELINE = "8260c58ba48e0a65ca7cb7668dd2d8c64b1434f1"
IDS = ("string_functions", "substring_only", "concat_only", "upper_only")
CONFIGURATIONS = ("native", "spark-off", "spark-on")
PREFIX = "strings-1388"

def load_execution_driver(module_name):
    spec = importlib.util.spec_from_file_location(module_name, SUITE.parent / "execution" / (module_name + ".py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


execution = load_execution_driver("run")
aggregation = load_execution_driver("summarize")

def write_json(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")

def command(binary, *args):
    return [str(binary), "--strings-1388", *map(str, args)]

def expected_runs():
    runs = []
    for replica in range(1, 4):
        for index, query in enumerate(IDS):
            for arm in (("a", "b") if (replica + index) % 2 else ("b", "a")):
                runs.append(dict(series="noise", configuration="native", query=query, replica=replica,
                                 arm=arm, process=f"noise-{replica:02}-{query}-{arm}"))
    for replica in range(1, 4):
        for index, query in enumerate(IDS):
            offset = (replica - 1 + index) % 3
            for configuration in CONFIGURATIONS[offset:] + CONFIGURATIONS[:offset]:
                runs.append(dict(series="main", configuration=configuration, query=query, replica=replica,
                                 process=f"main-{replica:02}-{query}-{configuration}"))
    return runs

def load_and_validate_workload():
    manifest, schedule = (execution.read(SUITE / name) for name in ("queries.json", "schedule.json"))
    original = execution.read(execution.EXECUTION / "queries.json")
    assert manifest["source_commit"] == BASELINE
    assert [query["id"] for query in manifest["queries"]] == list(IDS)
    for key in ("common_view_sql", "dimension", "source_scale_factor", "stored_columns", "snapshot_columns"):
        assert manifest[key] == original[key], key
    assert manifest["queries"][0] == next(query for query in original["queries"] if query["id"] == IDS[0])
    for query, expression in zip(manifest["queries"][1:], (
        "SUBSTRING(l_comment,1,12) AS prefix",
        "CONCAT(l_shipmode,':',l_shipinstruct) AS description",
        "UPPER(l_comment) AS uppercase",
    ), strict=True):
        assert query["native_sql"] == query["spark_sql"] == f"SELECT l_orderkey,l_linenumber,{expression} FROM bench"
        assert query["ordered"] is False and query["related_issues"] == []
    original_schedule = execution.read(execution.EXECUTION / "schedule.json")
    for key in ("cpus", "target_partitions", "batch_rows", "runtime_workers", "warmups", "samples",
                "process_timeout_seconds", "single_pool_bytes", "preflight_pool_bytes", "single_address_space_bytes",
                "preflight_address_space_bytes", "spill_limit_bytes", "preflight_process_timeout_seconds",
                "profile_iterations", "profile_frequency_hz", "profile_stack_bytes"):
        assert schedule[key] == original_schedule[key], key
    assert schedule["schedule_timeout_seconds"] == 3600 and schedule["preflight_timeout_seconds"] == 1800
    assert schedule["runs"] == expected_runs() and schedule["concurrency"] == []
    assert schedule["profile_max_cases"] == 4 and schedule["profile_timeout_seconds"] == 180
    assert schedule["profile_total_timeout_seconds"] == 900 and schedule["profile_max_bytes"] == 512 * 1024**2
    return manifest, schedule

def bound_hashes():
    paths = [SUITE / name for name in ("queries.json", "schedule.json", "run.py")]
    paths += [WORKSPACE / "benches/execution.rs", WORKSPACE / "Cargo.toml", WORKSPACE / "Cargo.lock",
              execution.EXECUTION / "run.py", execution.EXECUTION / "summarize.py",
              REPORTS / "execution-1380-fixture.json", REPORTS / (PREFIX + "-protocol.md"),
              REPORTS / (PREFIX + "-preflight.json.gz")]
    return {str(path.relative_to(WORKSPACE)): execution.digest(path) for path in paths}

def check_fixture(snapshot):
    fixture = execution.read(REPORTS / "execution-1380-fixture.json")
    assert str(snapshot) == fixture["snapshot_root"]
    assert execution.digest(snapshot / "_delta_log/00000000000000000000.json") == fixture["derived_delta_log_sha256"]
    base = pathlib.Path(fixture["base_root"])
    assert execution.digest(base / "manifest.json") == fixture["source_manifest_sha256"]
    source = execution.read(base / "manifest.json")
    table = next(table for table in source["tables"] if table.get("id") == "wide.clustered")
    assert execution.digest(base / table["path"] / "_delta_log/00000000000000000000.json") == fixture["base_delta_log_sha256"]
    for file in fixture["files"]:
        assert (snapshot / file["path"]).stat().st_size == file["bytes"]
        assert execution.digest(snapshot / file["path"]) == file["sha256"], file["path"]
    return fixture

def invoke(binary, snapshot, destination, schedule, deadline, mode, configuration, query):
    cpu = ",".join(map(str, schedule["cpus"]))
    preflight = mode == "preflight"
    return execution.invoke(["taskset", "-c", cpu] + command(binary, mode, snapshot, destination, configuration, query),
                            destination, execution.remaining_timeout(deadline, schedule["preflight_process_timeout_seconds"] if preflight else schedule["process_timeout_seconds"]),
                            schedule["preflight_address_space_bytes"] if preflight else schedule["single_address_space_bytes"])


def validate_telemetry(result, schedule):
    assert result["output_partitions"] == schedule["target_partitions"], "output partitions differ"
    assert result["source_io"]["counters_complete"], "source counters incomplete"
    execution.require_same_source_io(result["source_io"], result["source_io"])

def compare_results(binary, left, right, work, deadline, address_bytes):
    try:
        return execution.invoke(command(binary, "compare", left, right), work,
                                execution.remaining_timeout(deadline, 180), address_bytes)
    except RuntimeError as error:
        # Legacy main emits these exact errors with exit 1 for confirmed decoded mismatches.
        stderr = (work / "stderr.log").read_text()
        mismatch = re.fullmatch(r'Error: "(executed SQL schemas differ|result values/types differ at or after row [0-9]+|result row count/multiplicity differs after [0-9]+ rows)"\n?', stderr)
        if str(error) != f"command failed (1); evidence: {work}" or mismatch is None:
            raise
        return dict(equal=False, mismatch=mismatch.group(1), stderr_path=str(work / "stderr.log"))

def preflight(binary, snapshot, destination, manifest, schedule):
    fixture = check_fixture(snapshot)
    deadline = time.monotonic() + schedule["preflight_timeout_seconds"]
    check = execution.invoke(command(binary, "check"), destination / "self-check",
                             execution.remaining_timeout(deadline, 30), schedule["preflight_address_space_bytes"])
    assert check == dict(kind="self_check", status="passed")
    results, comparisons, excluded, eligible = [], [], {}, []
    for query in IDS:
        outputs = {}
        for configuration in CONFIGURATIONS:
            print("preflight", query, configuration, flush=True)
            result = invoke(binary, snapshot, destination / query / configuration, schedule, deadline,
                            "preflight", configuration, query)
            assert result["id"] == query and result["configuration"] == configuration
            validate_telemetry(result, schedule)
            result["ipc_sha256"] = execution.digest(pathlib.Path(result["ipc_path"]))
            outputs[configuration] = result
            results.append(result)
        failures = []
        native = outputs["native"]
        for configuration in CONFIGURATIONS[1:]:
            spark = outputs[configuration]
            difference = "".join(difflib.unified_diff(native["physical_plan"].splitlines(keepends=True),
                                                     spark["physical_plan"].splitlines(keepends=True),
                                                     fromfile="native", tofile=configuration))
            (destination / query / (configuration + ".plan.diff")).write_text(difference)
            comparison = compare_results(binary, native["ipc_path"], spark["ipc_path"],
                                         destination / query / ("compare-" + configuration),
                                         deadline, schedule["preflight_address_space_bytes"])
            reason = None
            if not comparison["equal"]:
                reason = comparison["mismatch"]
            elif comparison["rows_compared"] != native["rows"] or native["rows"] != spark["rows"] or native["rows"] != fixture["rows"]:
                reason = "row count differs from the native reference or full SF1 input"
            elif native["sql_schema"] != spark["sql_schema"]:
                reason = "executed SQL types differ"
            else:
                try:
                    execution.require_same_source_io(spark["source_io"], native["source_io"])
                except AssertionError as error:
                    reason = str(error)
            if reason:
                failures.append(dict(configuration=configuration, reason=reason))
            else:
                comparisons.append(dict(comparison, id=query, configuration=configuration))
        if failures:
            excluded[query] = failures
        else:
            eligible.append(query)
    full = dict(status="complete", results=results, comparisons=comparisons,
                eligible_shapes=eligible, excluded_shapes=excluded)
    write_json(destination / "preflight.json", full)
    with (REPORTS / (PREFIX + "-preflight.json.gz")).open("xb") as output:
        with gzip.GzipFile(fileobj=output, mode="wb", mtime=0) as compressed:
            compressed.write((json.dumps(full, indent=2) + "\n").encode())
    receipt = dict(status="complete", baseline_commit=BASELINE, binary_sha256=execution.digest(binary),
                   snapshot_root=str(snapshot), snapshot_log_sha256=fixture["derived_delta_log_sha256"],
                   file_hashes=bound_hashes(), eligible_shapes=eligible, excluded_shapes=excluded,
                   preflight_result_count=len(results), exact_comparison_count=len(comparisons),
                   expected_rows={q: {r["configuration"]: r["rows"] for r in results if r["id"] == q} for q in eligible},
                   expected_source_io={q: {r["configuration"]: r["source_io"] for r in results if r["id"] == q} for q in eligible})
    write_json(SUITE / "preflight-receipt.json", receipt)
    print(json.dumps({key: full[key] for key in ("status", "eligible_shapes", "excluded_shapes")}))

def validate_receipt(binary, snapshot):
    receipt = execution.read(SUITE / "preflight-receipt.json")
    assert receipt["status"] == "complete" and receipt["baseline_commit"] == BASELINE
    assert receipt["file_hashes"] == bound_hashes(), "frozen source or evidence changed"
    assert receipt["binary_sha256"] == execution.digest(binary), "binary differs from untimed preflight"
    assert receipt["snapshot_root"] == str(snapshot)
    fixture = check_fixture(snapshot)
    assert receipt["snapshot_log_sha256"] == fixture["derived_delta_log_sha256"]
    assert receipt["preflight_result_count"] == 12
    assert receipt["exact_comparison_count"] >= 2 * len(receipt["eligible_shapes"])
    assert set(receipt["eligible_shapes"]) | set(receipt["excluded_shapes"]) == set(IDS)
    assert not set(receipt["eligible_shapes"]) & set(receipt["excluded_shapes"])
    return receipt


def validate_samples(result, query, configuration, receipt, schedule):
    assert result["id"] == query and result["configuration"] == configuration
    assert len(result["samples"]) == schedule["samples"]
    for sample in result["samples"]:
        assert sample["rows"] == receipt["expected_rows"][query][configuration], "rows differ from preflight"
        validate_telemetry(sample, schedule)
        execution.require_same_source_io(sample["source_io"], receipt["expected_source_io"][query][configuration])

def measure(binary, snapshot, destination, manifest, schedule):
    assert not subprocess.check_output(["git", "status", "--porcelain"], cwd=WORKSPACE, text=True).strip(), "commit the reviewed freeze before timing"
    receipt = validate_receipt(binary, snapshot)
    deadline = time.monotonic() + schedule["schedule_timeout_seconds"]
    records = []
    for run in schedule["runs"]:
        if run["query"] not in receipt["eligible_shapes"]:
            continue
        print(run["process"], flush=True)
        result = invoke(binary, snapshot, destination / run["process"], schedule, deadline,
                        "measure", run["configuration"], run["query"])
        validate_samples(result, run["query"], run["configuration"], receipt, schedule)
        assert result["source_commit"] == BASELINE
        assert isinstance(result["process_peak_rss_bytes"], int) and result["process_peak_rss_bytes"] > 0
        records.append(dict(result, **run))
    with (destination / "samples.jsonl.gz").open("xb") as output:
        with gzip.GzipFile(fileobj=output, mode="wb", mtime=0) as compressed:
            for record in records:
                compressed.write((json.dumps(record, separators=(",", ":")) + "\n").encode())
    write_json(destination / "complete.json", dict(query_processes=len(records), concurrency_processes=0,
                                                  eligible_shapes=receipt["eligible_shapes"], excluded_shapes=receipt["excluded_shapes"]))

def completion_gap_exceeds_noise(native_ns, spark_ns, noise_ns):
    return spark_ns - native_ns > noise_ns


def select_profiles(shapes):
    cases = []
    for shape in shapes:
        if "excluded" in shape:
            continue
        configurations = shape["configurations"]
        native = configurations["native"]["query_complete_ns"]["median"]
        slower = [c for c in CONFIGURATIONS[1:] if completion_gap_exceeds_noise(native, configurations[c]["query_complete_ns"]["median"], shape["noise_ns"])]
        if slower:
            selected = max(slower, key=lambda c: configurations[c]["query_complete_ns"]["median"])
            cases.append(dict(id=shape["id"], configuration=selected))
    return cases

def summarize(destination, output):
    _, schedule = load_and_validate_workload()
    complete = execution.read(destination / "complete.json")
    receipt = execution.read(SUITE / "preflight-receipt.json")
    assert complete["eligible_shapes"] == receipt["eligible_shapes"]
    assert complete["excluded_shapes"] == receipt["excluded_shapes"]
    assert complete["concurrency_processes"] == 0
    with gzip.open(destination / "samples.jsonl.gz", "rt") as source:
        records = [json.loads(line) for line in source]
    runs = [run for run in schedule["runs"] if run["query"] in complete["eligible_shapes"]]
    assert len(records) == len(runs) == complete["query_processes"]
    for record, run in zip(records, runs, strict=True):
        assert {key: record[key] for key in run} == run
        assert record["kind"] == "query_samples" and record["source_commit"] == BASELINE
        validate_samples(record, run["query"], run["configuration"], receipt, schedule)
    data = aggregation.aggregate(records)
    rows = []
    for query in IDS:
        if query not in complete["eligible_shapes"]:
            rows.append(dict(id=query, excluded=complete["excluded_shapes"][query]))
            continue
        def values(series, arm, configuration):
            return data[(series, arm, configuration, query, "query_complete_ns")]
        native = values("main", None, "native")["median"]
        noise = abs(values("noise", "a", "native")["median"] - values("noise", "b", "native")["median"])
        configurations = {}
        for configuration in CONFIGURATIONS:
            timing = values("main", None, configuration)
            row_count = data[("main", None, configuration, query, "rows")]["median"]
            configurations[configuration] = dict(query_complete_ns=timing, query_complete_ms=timing["median"] / 1e6,
                                                 ns_per_row=timing["median"] / row_count, rows=row_count,
                                                 delta_ns=timing["median"] - native,
                                                 completion_gap_exceeds_noise=completion_gap_exceeds_noise(native, timing["median"], noise))
        rows.append(dict(id=query, noise_ns=noise, noise_ms=noise / 1e6, noise_ns_per_row=noise / row_count,
                         configurations=configurations))
    profiles = select_profiles(rows)
    assert len(profiles) <= schedule["profile_max_cases"]
    output.mkdir(parents=True, exist_ok=True)
    summary = dict(shapes=rows, profile_cases=profiles, process_records=len(records),
                   excluded_shapes=complete["excluded_shapes"], samples_sha256=execution.digest(destination / "samples.jsonl.gz"))
    write_json(output / (PREFIX + "-summary.json"), summary)
    write_json(output / (PREFIX + "-metrics.json"), [dict(series=k[0], arm=k[1], configuration=k[2], id=k[3], metric=k[4], **v) for k, v in data.items()])
    return summary


def profile(binary, snapshot, run_directory, destination, schedule):
    receipt = validate_receipt(binary, snapshot)
    # Reparent orphaned profiler children so this controller can reap the whole group.
    pr_set_child_subreaper = 36
    if ctypes.CDLL(None, use_errno=True).prctl(pr_set_child_subreaper, 1, 0, 0, 0):
        raise OSError(ctypes.get_errno(), "could not enable profiler child reaping")
    metadata = execution.read(run_directory / "metadata.json")
    assert metadata["binary_sha256"] == execution.digest(binary)
    for key, path in (("queries_sha256", SUITE / "queries.json"), ("schedule_sha256", SUITE / "schedule.json"),
                      ("controller_sha256", pathlib.Path(__file__))):
        assert metadata[key] == execution.digest(path), key
    summary = summarize(run_directory, destination)
    cases = summary["profile_cases"]
    assert all(case["id"] in receipt["eligible_shapes"] for case in cases)
    assert len(cases) <= schedule["profile_max_cases"]
    assert len({case["id"] for case in cases}) == len(cases)
    deadline = time.monotonic() + schedule["profile_total_timeout_seconds"]
    for case in cases:
        work = destination / case["id"]
        work.mkdir()
        for name in ("perf-control", "perf-ack"):
            os.mkfifo(work / name)
        cpu = ",".join(map(str, schedule["cpus"]))
        perf_command = ["perf", "record", "-e", "cpu-clock:u", "--user-callchains", "--call-graph",
                        f"dwarf,{schedule['profile_stack_bytes']}", "-F", str(schedule["profile_frequency_hz"]),
                        "--delay=-1", "--control", f"fifo:{work / 'perf-control'},{work / 'perf-ack'}",
                        "--max-size", f"{schedule['profile_max_bytes'] // 1024**2}M", "-o", str(work / "perf.data"), "--",
                        "taskset", "-c", cpu] + command(binary, "profile", snapshot, work, case["configuration"], case["id"])
        write_json(work / "command.json", perf_command)
        with (work / "stdout.jsonl").open("x") as output, (work / "stderr.log").open("x") as errors:
            process = subprocess.Popen(perf_command, stdout=output, stderr=errors, start_new_session=True,
                                       preexec_fn=execution.address_limit(schedule["single_address_space_bytes"]))
            try:
                process.wait(timeout=execution.remaining_timeout(deadline, schedule["profile_timeout_seconds"]))
                assert process.returncode == 0, "profile failed; retain partial evidence and do not retry"
            finally:
                # perf may exit while its child is blocked on a FIFO acknowledgment.
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                process.wait()
                while True:
                    try:
                        os.waitpid(-process.pid, 0)
                    except ChildProcessError:
                        break
        records = [json.loads(line) for line in (work / "stdout.jsonl").read_text().splitlines()]
        assert len(records) == 1
        result = records[0]
        assert result["kind"] == "profile" and result["warmups"] == 1
        assert result["executions"] == schedule["profile_iterations"]
        validate_samples(result, case["id"], case["configuration"], receipt, schedule)
        assert (work / "perf.data").stat().st_size <= schedule["profile_max_bytes"]
        for name, arguments in (
            ("stacks.txt", ["script", "-F", "+srcline,+srccode", "--inline", "--full-source-path"]),
            ("report.txt", ["report", "--stdio", "--no-children", "--sort=symbol,srcline"]),
        ):
            with (work / name).open("x") as output, (work / (name + ".stderr")).open("x") as errors:
                subprocess.run(["perf", *arguments, "-i", str(work / "perf.data")], stdout=output, stderr=errors,
                               check=True, timeout=execution.remaining_timeout(deadline, 180))
    write_json(destination / "complete.json", dict(profile_count=len(cases), cases=cases))

def self_check():
    load_and_validate_workload()
    execution.source_io_self_check()
    aggregation.self_check()
    assert command(pathlib.Path("execution"), "check") == ["execution", "--strings-1388", "check"]
    assert not completion_gap_exceeds_noise(100, 110, 10)
    assert completion_gap_exceeds_noise(100, 111, 10)
    assert not completion_gap_exceeds_noise(100, 99, 0)
    assert completion_gap_exceeds_noise(100, 101, 0)
    runs = expected_runs()
    assert len(runs) == len({run["process"] for run in runs}) == 60
    for query in IDS:
        assert sum(run["query"] == query for run in runs) == 15

    io = dict(distinct_source_files=["one.parquet"], source_file_count=1, source_bytes_received=10,
              recorded_range_bytes=10, counters_complete=True)
    receipt = dict(expected_rows={"q": {"native": 2}}, expected_source_io={"q": {"native": io}})
    schedule = dict(samples=3, target_partitions=2)
    sample = dict(rows=2, output_partitions=2, source_io=io)
    result = dict(id="q", configuration="native", samples=[sample] * 3)
    validate_samples(result, "q", "native", receipt, schedule)
    for changed in (dict(sample, rows=1), dict(sample, output_partitions=1),
                    dict(sample, source_io=dict(io, counters_complete=False)),
                    dict(sample, source_io=dict(io, source_bytes_received=11))):
        try:
            validate_samples(dict(result, samples=[changed] * 3), "q", "native", receipt, schedule)
        except AssertionError:
            pass
        else:
            raise AssertionError("changed timed evidence accepted")
    def shape(off, on):
        return dict(id="q", noise_ns=10, configurations={c: dict(query_complete_ns=dict(median=n))
                    for c, n in zip(CONFIGURATIONS, (100, off, on), strict=True)})
    assert select_profiles([shape(110, 110)]) == []
    assert select_profiles([shape(111, 110)]) == [dict(id="q", configuration="spark-off")]
    assert select_profiles([shape(111, 112)]) == [dict(id="q", configuration="spark-on")]
    assert select_profiles([shape(112, 112)]) == [dict(id="q", configuration="spark-off")]
    assert select_profiles([dict(id="q", excluded="type mismatch")]) == []

    original_invoke = execution.invoke
    with tempfile.TemporaryDirectory(prefix="strings-1388-check-") as temporary:
        work = pathlib.Path(temporary)
        exit_status = 1
        def comparator_failure(*args):
            raise RuntimeError(f"command failed ({exit_status}); evidence: {work}")
        execution.invoke = comparator_failure
        try:
            for message in ("executed SQL schemas differ", "result values/types differ at or after row 7",
                            "result row count/multiplicity differs after 2 rows"):
                (work / "stderr.log").write_text(f'Error: "{message}"\n')
                assert compare_results("binary", "left", "right", work, time.monotonic() + 5, 1024)["equal"] is False
            for message, exit_status in (("out of memory", 1), ("No such file or directory", 1),
                                         ("panic", 1), ("executed SQL schemas differ; unexpected suffix", 1),
                                         ("executed SQL schemas differ", 137), ("executed SQL schemas differ", -9)):
                (work / "stderr.log").write_text(f'Error: "{message}"\n')
                try:
                    compare_results("binary", "left", "right", work, time.monotonic() + 5, 1024)
                except RuntimeError:
                    pass
                else:
                    raise AssertionError("unexpected comparator failure was excluded")
        finally:
            execution.invoke = original_invoke

    for optimize_arguments, optimize_environment in ((["-O"], {}), ([], {"PYTHONOPTIMIZE": "1"})):
        environment = dict(os.environ)
        environment.pop("PYTHONOPTIMIZE", None)
        environment.update(optimize_environment)
        child = subprocess.run([sys.executable, *optimize_arguments, str(pathlib.Path(__file__).resolve()), "--check"],
                               env=environment, capture_output=True, text=True, timeout=10)
        assert child.returncode != 0 and child.stdout == ""
        assert child.stderr.strip() == "validation requires Python assertions"


if __name__ == "__main__":
    if sys.flags.optimize:
        raise SystemExit("validation requires Python assertions")
    self_check()
    if sys.argv[1:] == ["--check"]:
        print("string inventory, source I/O, aggregation and noise-boundary checks passed")
    elif len(sys.argv) == 4 and sys.argv[1] == "summarize":
        summarize(pathlib.Path(sys.argv[2]).resolve(), pathlib.Path(sys.argv[3]).resolve())
    elif len(sys.argv) == 6 and sys.argv[1] == "profile":
        assert platform.system() == "Linux", "profiles require Linux"
        _, schedule = load_and_validate_workload()
        binary, snapshot, run_directory, destination = map(lambda path: pathlib.Path(path).resolve(), sys.argv[2:])
        assert not destination.exists(), "retain existing evidence; no unchanged reruns"
        destination.mkdir(parents=True)
        profile(binary, snapshot, run_directory, destination, schedule)
    elif len(sys.argv) == 5 and sys.argv[1] in ("preflight", "measure"):
        assert platform.system() == "Linux", "execution/RSS runs require Linux"
        manifest, schedule = load_and_validate_workload()
        binary, snapshot, destination = map(lambda path: pathlib.Path(path).resolve(), sys.argv[2:])
        assert not destination.exists(), "retain existing evidence; no unchanged reruns"
        destination.mkdir(parents=True)
        metadata = execution.metadata(binary, snapshot, manifest, schedule)
        metadata.update(queries_sha256=execution.digest(SUITE / "queries.json"), schedule_sha256=execution.digest(SUITE / "schedule.json"),
                        controller_sha256=execution.digest(pathlib.Path(__file__)))
        write_json(destination / "metadata.json", metadata)
        (preflight if sys.argv[1] == "preflight" else measure)(binary, snapshot, destination, manifest, schedule)
    else:
        raise SystemExit("usage: run.py --check | preflight/measure BINARY SNAPSHOT NEW_DIRECTORY | summarize RUN_DIRECTORY OUTPUT_DIRECTORY | profile BINARY SNAPSHOT RUN_DIRECTORY NEW_DIRECTORY")
