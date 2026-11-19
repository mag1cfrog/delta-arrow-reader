#!/usr/bin/env python3
"""Run the fixed execution preflight or the once-only committed timing schedule."""

import collections
import difflib
import gzip
import hashlib
import json
import pathlib
import platform
import resource
import subprocess
import sys
import time

EXECUTION = pathlib.Path(__file__).resolve().parent
WORKSPACE = EXECUTION.parents[1]
REPORTS = WORKSPACE / "benches/reports"
SUITE = EXECUTION
CAST_SUITE_SELECTED = False


def read(path):
    return json.loads(path.read_text())


def digest(path):
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def check_inventory():
    manifest, schedule = read(SUITE / "queries.json"), read(SUITE / "schedule.json")
    ids = [query["id"] for query in manifest["queries"]]
    if CAST_SUITE_SELECTED:
        assert ids == ["cast_strings", "cast_date", "cast_decimal"]
        original = read(EXECUTION / "queries.json")
        assert manifest["common_view_sql"] == original["common_view_sql"]
        assert manifest["dimension"] == original["dimension"]
        assert manifest["queries"][0] == next(query for query in original["queries"] if query["id"] == "cast_strings")
        for query, expression in zip(manifest["queries"][1:], ("CAST(date_text AS DATE) AS ship_date", "CAST(decimal_text AS DECIMAL(15,2)) AS price"), strict=True):
            assert query["native_sql"] == query["spark_sql"] == f"SELECT l_orderkey,l_linenumber,{expression} FROM bench"
            assert not query["ordered"]
        original_schedule = read(EXECUTION / "schedule.json")
        for key in ("cpus", "target_partitions", "batch_rows", "runtime_workers", "warmups", "samples", "process_timeout_seconds", "single_pool_bytes", "preflight_pool_bytes", "single_address_space_bytes", "preflight_address_space_bytes", "spill_limit_bytes", "preflight_process_timeout_seconds"):
            assert schedule[key] == original_schedule[key], key
        assert schedule["concurrency"] == [] and schedule["profile_max_cases"] == 0
        assert len(schedule["runs"]) == len({run["process"] for run in schedule["runs"]}) == 45
        assert [run["series"] for run in schedule["runs"]] == ["noise"] * 18 + ["main"] * 27
        assert all(run["replica"] in (1, 2, 3) for run in schedule["runs"])
        assert collections.Counter((run["query"],run["configuration"]) for run in schedule["runs"] if run["series"] == "main") == {(query,configuration):3 for query in ids for configuration in ("native","spark-off","spark-on")}
        assert collections.Counter((run["query"],run["arm"]) for run in schedule["runs"] if run["series"] == "noise") == {(query,arm):3 for query in ids for arm in ("a","b")}
        assert all(run["configuration"] == "native" for run in schedule["runs"] if run["series"] == "noise")
        return manifest, schedule
    assert len(ids) == len(set(ids)) == 18
    assert set(ids) == {"projection_five", "partition_filter", "statistics_filter", "empty_filter", "ordered_limit", "group_low", "group_high", "count_distinct", "row_number", "dimension_join", "self_join", "decimal_division_round", "cast_strings", "string_functions", "nullif_nullable", "in_10", "in_1000", "mixed_boolean"}
    queries = {query["id"]:query for query in manifest["queries"]}
    for count in (10, 1000):
        assert len(queries[f"in_{count}"]["spark_sql"].rsplit("(",1)[1].rstrip(")").split(",")) == count
    assert len(schedule["runs"]) == len({run["process"] for run in schedule["runs"]}) == 270
    main = collections.Counter((run["query"],run["configuration"]) for run in schedule["runs"] if run["series"] == "main")
    noise = collections.Counter((run["query"],run["arm"]) for run in schedule["runs"] if run["series"] == "noise")
    assert main == {(query,configuration):3 for query in ids for configuration in ("native","spark-off","spark-on")}
    assert noise == {(query,arm):3 for query in ids for arm in ("a","b")}
    assert all(run["configuration"] == "native" for run in schedule["runs"] if run["series"] == "noise")
    assert len(schedule["concurrency"]) == 6 and all(case["executions"] == 4 for case in schedule["concurrency"])
    return manifest, schedule


def binary_command(binary, *args):
    return [str(binary)] + (["--casts-1385"] if CAST_SUITE_SELECTED else []) + list(args)


def require_same_source_io(actual, expected):
    for evidence in (actual, expected):
        assert evidence["source_bytes_received"] == evidence["recorded_range_bytes"], "received bytes differ from recorded range sum"
    for key in ("distinct_source_files", "source_file_count", "source_bytes_received", "recorded_range_bytes"):
        assert actual[key] == expected[key], f"source I/O differs: {key}"


def source_io_self_check():
    expected = dict(distinct_source_files=["one.parquet"], source_file_count=1, source_bytes_received=10, recorded_range_bytes=10)
    require_same_source_io(dict(expected), expected)
    for key, changed in (("distinct_source_files", ["other.parquet"]), ("source_file_count", 2), ("source_bytes_received", 9), ("recorded_range_bytes", 11)):
        try:
            require_same_source_io(dict(expected, **{key: changed}), expected)
        except AssertionError:
            pass
        else:
            raise AssertionError(f"changed source I/O accepted: {key}")
    inconsistent = dict(expected, recorded_range_bytes=11)
    try:
        require_same_source_io(inconsistent, inconsistent)
    except AssertionError:
        pass
    else:
        raise AssertionError("matching but internally inconsistent source I/O accepted")


def remaining_timeout(deadline, per_process):
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("the fixed overall deadline has expired")
    return min(remaining, per_process)


def address_limit(bound):
    def apply():
        soft, hard = resource.getrlimit(resource.RLIMIT_AS)
        ceiling = bound if hard == resource.RLIM_INFINITY else min(bound, hard)
        resource.setrlimit(resource.RLIMIT_AS, (ceiling if soft == resource.RLIM_INFINITY else min(soft,ceiling), ceiling))
    return apply


def invoke(command, work, timeout, address_bytes):
    work.mkdir(parents=True)
    with (work / "stdout.jsonl").open("x") as output, (work / "stderr.log").open("x") as errors:
        result = subprocess.run(command, stdout=output, stderr=errors, timeout=timeout, preexec_fn=address_limit(address_bytes))
    records = [json.loads(line) for line in (work / "stdout.jsonl").read_text().splitlines()]
    if result.returncode:
        raise RuntimeError(f"command failed ({result.returncode}); evidence: {work}")
    assert len(records) == 1, work
    return records[0]


def metadata(binary, snapshot, manifest, schedule):
    fixture = REPORTS / "execution-1380-fixture.json"
    return {"baseline_commit":manifest["source_commit"],"head_commit":subprocess.check_output(["git","rev-parse","HEAD"],cwd=WORKSPACE,text=True).strip(),"binary_sha256":digest(binary),"queries_sha256":digest(SUITE/"queries.json"),"schedule_sha256":digest(SUITE/"schedule.json"),"fixture_receipt_sha256":digest(fixture),"fixture":read(fixture),"snapshot":str(snapshot),"rustc":subprocess.check_output(["rustc","+1.96.0","--version","--verbose"],text=True),"uname":platform.uname()._asdict(),"cpuinfo":pathlib.Path("/proc/cpuinfo").read_text(),"governors":{str(cpu):pathlib.Path(f"/sys/devices/system/cpu/cpu{cpu}/cpufreq/scaling_governor").read_text().strip() for cpu in schedule["cpus"]},"schedule":schedule}


def preflight(binary, snapshot, destination, manifest, schedule):
    deadline = time.monotonic() + schedule["preflight_timeout_seconds"]
    cpu = ",".join(map(str,schedule["cpus"]))
    check = invoke(binary_command(binary,"check"), destination/"self-check", remaining_timeout(deadline,30), schedule["preflight_address_space_bytes"])
    assert check == {"kind":"self_check","status":"passed"}
    outcomes, comparisons = [], []
    for query in manifest["queries"]:
        results = {}
        for configuration in ("native","spark-off","spark-on"):
            work = destination / query["id"] / configuration
            command = ["taskset","-c",cpu] + binary_command(binary,"preflight",str(snapshot),str(work),configuration,query["id"])
            print("preflight",query["id"],configuration,flush=True)
            result = invoke(command, work, remaining_timeout(deadline,schedule["preflight_process_timeout_seconds"]), schedule["preflight_address_space_bytes"])
            result["ipc_sha256"] = digest(pathlib.Path(result["ipc_path"]))
            outcomes.append(result)
            results[configuration] = result
            if query["id"] in ("partition_filter","statistics_filter"):
                source_files = result["source_io"]["source_file_count"]
                assert 0 < source_files < len(read(REPORTS/"execution-1380-fixture.json")["files"]), "required filter did not read a nonempty proper subset"
                assert result["rows"] > 0
        native = results["native"]
        for configuration in ("spark-off","spark-on"):
            spark = results[configuration]
            work = destination / query["id"] / ("compare-"+configuration)
            command = binary_command(binary,"compare",native["ipc_path"],spark["ipc_path"])
            result = invoke(command, work, remaining_timeout(deadline,180), schedule["preflight_address_space_bytes"])
            result.update(id=query["id"],configuration=configuration,native_rows=native["rows"],spark_rows=spark["rows"])
            assert result["rows_compared"] == native["rows"] == spark["rows"]
            result["extra_source_files"] = spark["source_io"]["source_file_count"] > native["source_io"]["source_file_count"]
            result["extra_source_bytes"] = spark["source_io"]["source_bytes_received"] > native["source_io"]["source_bytes_received"]
            if CAST_SUITE_SELECTED:
                require_same_source_io(spark["source_io"], native["source_io"])
            comparisons.append(result)
            difference = "".join(difflib.unified_diff(native["physical_plan"].splitlines(keepends=True),spark["physical_plan"].splitlines(keepends=True),fromfile="native",tofile=configuration))
            (destination/query["id"]/(configuration+".plan.diff")).write_text(difference)
            if result["extra_source_files"] or result["extra_source_bytes"]:
                (destination/"finding.json").write_text(json.dumps({"kind":"extra_source_io","comparison":result,"native":native["source_io"],"spark":spark["source_io"]},indent=2)+"\n")
                raise RuntimeError("extra source I/O; stop before freeze for coordinator handling")
    receipt = {"status":"passed","result_count":len(outcomes),"comparison_count":len(comparisons),"results":outcomes,"comparisons":comparisons}
    (destination/"preflight.json").write_text(json.dumps(receipt,indent=2)+"\n")
    print(json.dumps({"status":"passed","results":len(outcomes),"comparisons":len(comparisons)}))


def measure(binary, snapshot, destination, manifest, schedule):
    if subprocess.check_output(["git","status","--porcelain"],cwd=WORKSPACE,text=True).strip():
        raise RuntimeError("commit protocol, fixture receipt, query list and harness before timing")
    eligibility = read(SUITE/"preflight-receipt.json")
    assert eligibility["status"] == "passed"
    for key, path in [("queries_sha256",SUITE/"queries.json"),("schedule_sha256",SUITE/"schedule.json"),("harness_sha256",WORKSPACE/"benches/execution.rs"),("timing_driver_sha256",EXECUTION/"run.py"),("fixture_receipt_sha256",REPORTS/"execution-1380-fixture.json")]:
        assert eligibility[key] == digest(path), key
    assert eligibility["binary_sha256"] == digest(binary)
    assert eligibility["baseline_commit"] == manifest["source_commit"]
    assert eligibility["snapshot_root"] == str(snapshot)
    assert eligibility["snapshot_log_sha256"] == digest(snapshot/"_delta_log/00000000000000000000.json")
    for file in read(REPORTS/"execution-1380-fixture.json")["files"]:
        assert file["sha256"] == digest(snapshot/file["path"]), file["path"]
    if CAST_SUITE_SELECTED:
        for key, path in (("protocol_sha256", REPORTS/"casts-1385-protocol.md"), ("summarizer_sha256", EXECUTION/"summarize.py")):
            assert eligibility[key] == digest(path), key
        assert eligibility["timing_driver_sha256"] == digest(EXECUTION/"run.py")
        assert eligibility["full_receipt_sha256"] == digest(REPORTS/"casts-1385-preflight.json.gz")
        assert eligibility["preflight_result_count"] == 9 and eligibility["exact_comparison_count"] == 6
    expected_rows = eligibility["expected_rows"]
    deadline = time.monotonic() + schedule["schedule_timeout_seconds"]
    cpu = ",".join(map(str,schedule["cpus"]))
    records = []
    for run in schedule["runs"]:
        print(run["process"],flush=True)
        work = destination / run["process"]
        command = ["taskset","-c",cpu] + binary_command(binary,"measure",str(snapshot),str(work),run["configuration"],run["query"])
        result = invoke(command, work, remaining_timeout(deadline,schedule["process_timeout_seconds"]),schedule["single_address_space_bytes"])
        assert result["id"] == run["query"] and result["configuration"] == run["configuration"]
        assert len(result["samples"]) == schedule["samples"]
        assert isinstance(result["process_peak_rss_bytes"],int) and result["process_peak_rss_bytes"] > 0
        for sample in result["samples"]:
            if CAST_SUITE_SELECTED:
                require_same_source_io(sample["source_io"], eligibility["expected_source_io"][run["query"]][run["configuration"]])
            if sample["rows"] != expected_rows[run["query"]][run["configuration"]]:
                (destination/"finding.json").write_text(json.dumps({"kind":"timed_row_count","run":run,"sample":sample,"expected_rows":expected_rows[run["query"]][run["configuration"]]},indent=2)+"\n")
                raise RuntimeError("timed row count differs from exact preflight; coordinator handling required")
        result.update(run)
        records.append(result)
        if run["series"] == "main":
            matched = {record["configuration"]:record for record in records if record["series"]=="main" and record["id"]==run["query"] and record["replica"]==run["replica"]}
            if set(matched) == {"native","spark-off","spark-on"}:
                for configuration in ("spark-off","spark-on"):
                    for index, (native, spark) in enumerate(zip(matched["native"]["samples"],matched[configuration]["samples"],strict=True)):
                        if spark["source_io"]["source_file_count"] > native["source_io"]["source_file_count"] or spark["source_io"]["source_bytes_received"] > native["source_io"]["source_bytes_received"]:
                            (destination/"finding.json").write_text(json.dumps({"kind":"timed_extra_source_io","id":run["query"],"replica":run["replica"],"configuration":configuration,"sample":index,"native":native["source_io"],"spark":spark["source_io"]},indent=2)+"\n")
                            raise RuntimeError("newly observed source I/O increase; coordinator handling required")
    concurrency = []
    for case in schedule["concurrency"]:
        print(case["process"],flush=True)
        work = destination / case["process"]
        command = ["taskset","-c",cpu,str(binary),"concurrency",str(snapshot),str(work),case["configuration"],case["query"]]
        result = invoke(command,work,remaining_timeout(deadline,schedule["process_timeout_seconds"]),schedule["concurrency_address_space_bytes"])
        assert result["executions"] == 4 and len(result["results"]) == 4
        if any(output["rows"] != expected_rows[case["query"]][case["configuration"]] for output in result["results"]):
            (destination/"finding.json").write_text(json.dumps({"kind":"concurrency_row_count","case":case,"result":result},indent=2)+"\n")
            raise RuntimeError("concurrency row count differs from exact preflight; coordinator handling required")
        result.update(case)
        concurrency.append(result)
    with (destination/"samples.jsonl.gz").open("xb") as output:
        with gzip.GzipFile(fileobj=output,mode="wb",mtime=0) as compressed:
            for record in records+concurrency:
                compressed.write((json.dumps(record,separators=(",",":"))+"\n").encode())
    (destination/"complete.json").write_text(json.dumps({"query_processes":len(records),"concurrency_processes":len(concurrency)})+"\n")


if __name__ == "__main__":
    if sys.argv[1:2] == ["--casts-1385"]:
        CAST_SUITE_SELECTED = True
        SUITE = EXECUTION.parent / "casts"
        del sys.argv[1]
    source_io_self_check()
    manifest, schedule = check_inventory()
    assert binary_command(pathlib.Path("execution"), "check") == (["execution", "--casts-1385", "check"] if CAST_SUITE_SELECTED else ["execution", "check"])
    if sys.argv[1:] == ["--check"]:
        print("execution inventory/schedule checks passed")
    elif len(sys.argv) == 5 and sys.argv[1] in ("preflight","measure"):
        if platform.system() != "Linux":
            raise SystemExit("execution/RSS runs require Linux")
        mode = sys.argv[1]
        binary, snapshot, destination = map(lambda value:pathlib.Path(value).resolve(),sys.argv[2:])
        if destination.exists():
            raise SystemExit("destination exists; retain it and do not repeat an unchanged schedule")
        destination.mkdir(parents=True)
        (destination/"metadata.json").write_text(json.dumps(metadata(binary,snapshot,manifest,schedule),indent=2)+"\n")
        (preflight if mode == "preflight" else measure)(binary,snapshot,destination,manifest,schedule)
    else:
        raise SystemExit("usage: run.py [--casts-1385] --check | preflight/measure BINARY SNAPSHOT NEW_DIRECTORY")
