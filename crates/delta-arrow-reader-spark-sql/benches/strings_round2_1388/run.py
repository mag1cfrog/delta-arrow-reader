"""Run one bounded SF1 comparison of the three isolated string functions."""

import importlib.util
import itertools
import pathlib
import platform
import shutil
import statistics
import subprocess
import sys
import time

SUITE = pathlib.Path(__file__).resolve().parent
LEGACY = SUITE.parent / "strings"
spec = importlib.util.spec_from_file_location("strings", LEGACY / "run.py")
strings = importlib.util.module_from_spec(spec)
spec.loader.exec_module(strings)
execution = strings.execution
MODES = strings.CONFIGURATIONS
STATS = strings.aggregation.spread


def run_order(queries, replicas):
    orders = list(itertools.permutations(MODES))
    for replica in range(replicas):
        offset = replica % len(queries)
        for query in queries[offset:] + queries[:offset]:
            index = queries.index(query)
            arms = ("a", "b") if (replica + index) % 2 else ("b", "a")
            for mode in orders[(replica + index) % len(orders)]:
                yield replica, query, "main", mode
            for arm in arms:
                yield replica, query, "noise", arm


def preflight(binary, snapshot, work, shapes, schedule):
    fixture = strings.check_fixture(snapshot)
    deadline = time.monotonic() + schedule["preflight_timeout_seconds"]
    check = execution.invoke(strings.command(binary, "check"), work / "self-check",
                             execution.remaining_timeout(deadline, 30),
                             schedule["preflight_address_space_bytes"])
    assert check == {"kind": "self_check", "status": "passed"}
    receipt = dict(eligible_shapes=[], excluded_shapes={}, expected_rows={},
                   expected_source_io={})
    for shape in shapes:
        results = {}
        for mode in MODES:
            result = strings.invoke(binary, snapshot, work / shape / mode, schedule,
                                    deadline, "preflight", mode, shape)
            assert result["id"] == shape and result["configuration"] == mode
            strings.validate_telemetry(result, schedule)
            result["ipc_sha256"] = execution.digest(pathlib.Path(result["ipc_path"]))
            results[mode] = result
        native = results["native"]
        failures = []
        for mode in MODES[1:]:
            spark = results[mode]
            comparison = strings.compare_results(
                binary, native["ipc_path"], spark["ipc_path"], work / shape / ("compare-" + mode),
                deadline, schedule["preflight_address_space_bytes"])
            failure = None
            if not comparison["equal"]:
                failure = comparison["mismatch"]
            elif not comparison["rows_compared"] == native["rows"] == spark["rows"] == fixture["rows"]:
                failure = "complete SF1 row count differs"
            elif native["sql_schema"] != spark["sql_schema"] or native["arrow_types"] != spark["arrow_types"]:
                failure = "executed result types differ"
            else:
                try:
                    execution.require_same_source_io(spark["source_io"], native["source_io"])
                except AssertionError as error:
                    failure = str(error)
            if failure:
                failures.append(dict(configuration=mode, reason=failure))
        strings.write_json(work / shape / "exports.json", results)
        if failures:
            receipt["excluded_shapes"][shape] = failures
        else:
            receipt["eligible_shapes"].append(shape)
            receipt["expected_rows"][shape] = {mode: result["rows"] for mode, result in results.items()}
            receipt["expected_source_io"][shape] = {mode: result["source_io"] for mode, result in results.items()}
    return receipt


def measure(binary, snapshot, destination, schedule, receipt, ordered_runs):
    deadline = time.monotonic() + schedule["schedule_timeout_seconds"]
    timings = {}
    for replica, shape, series, path in ordered_runs:
        if shape not in receipt["eligible_shapes"]:
            continue
        mode = path if series == "main" else "native"
        process = f"{series}-{replica + 1:02}-{shape}-{path}"
        print(process, flush=True)
        result = strings.invoke(binary, snapshot, destination / process, schedule,
                                deadline, "measure", mode, shape)
        assert result["kind"] == "query_samples" and result["source_commit"] == strings.BASELINE
        strings.validate_samples(result, shape, mode, receipt, schedule)
        values = timings.setdefault((shape, series, path), [])
        assert len(values) == replica
        values.append(statistics.median(s["query_complete_ns"] for s in result["samples"]))
    return timings


def summarize(timings, receipt):
    shapes = []
    for shape in receipt["eligible_shapes"]:
        paths = {mode: STATS(timings[shape, "main", mode]) for mode in MODES}
        controls = {arm: STATS(timings[shape, "noise", arm]) for arm in ("a", "b")}
        floor = abs(controls["a"]["median"] - controls["b"]["median"])
        on_minus_off = [on - off for on, off in zip(timings[shape, "main", "spark-on"],
                                                    timings[shape, "main", "spark-off"], strict=True)]
        paths = {mode: {"query_complete_ns": estimate} for mode, estimate in paths.items()}
        shapes.append(dict(id=shape, configurations=paths, native_controls_ns=controls,
                           noise_ns=floor, paired_on_minus_off_ns=on_minus_off))
    return dict(shapes=shapes, excluded_shapes=receipt["excluded_shapes"],
                profile_cases=strings.select_profiles(shapes))


def profile(binary, snapshot, destination, schedule, receipt, summary):
    # The frozen profiler uses these two hooks and a pair of on-disk bindings.
    work = destination / "profiles"
    work.mkdir()
    strings.SUITE = destination
    strings.validate_receipt = lambda _binary, _snapshot: receipt
    strings.summarize = lambda _run, _output: summary
    strings.profile(binary, snapshot, destination, work, schedule)


def main():
    if sys.flags.optimize:
        raise SystemExit("validation requires Python assertions")
    if len(sys.argv) != 4 or platform.system() != "Linux":
        raise SystemExit("usage on Linux: run.py BINARY SNAPSHOT NEW_EVIDENCE_DIRECTORY")
    binary, snapshot, destination = map(lambda arg: pathlib.Path(arg).resolve(), sys.argv[1:])
    assert not subprocess.check_output(["git", "status", "--porcelain"], cwd=SUITE, text=True).strip()
    protocol_commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=SUITE, text=True).strip()
    protocol = execution.read(SUITE / "protocol.json")
    source = (binary.parent / "source.txt").read_text().strip()
    assert source == protocol["staging_source"], "binary source differs from protocol"
    expected_digest = (binary.parent / "executable-sha256.txt").read_text().split()[0]
    assert execution.digest(binary) == expected_digest, "binary differs from build receipt"
    changed = subprocess.check_output(["git", "diff", "--name-only", source], cwd=SUITE, text=True).splitlines()
    repo = pathlib.Path(subprocess.check_output(["git", "rev-parse", "--show-toplevel"], cwd=SUITE, text=True).strip())
    assert all((repo / name).is_relative_to(SUITE) for name in changed), "protocol-only diff required"
    manifest = execution.read(SUITE / "queries.json")
    original = execution.read(LEGACY / "queries.json")
    original["queries"] = [query for query in original["queries"] if query["id"] != "string_functions"]
    assert manifest == original, "must use the frozen isolated statements"
    shapes = [query["id"] for query in manifest["queries"]]
    schedule = execution.read(LEGACY / "schedule.json")
    schedule.update(schedule_timeout_seconds=protocol["timing_timeout_seconds"],
                    profile_max_cases=protocol["maximum_profile_cases"])
    ordered_runs = list(run_order(shapes, protocol["process_replicas"]))
    assert len(ordered_runs) == protocol["maximum_processes"] == len(set(ordered_runs))
    for shape in shapes:
        path_orders = {tuple(path for index, name, series, path in ordered_runs
                             if index == replica and name == shape and series == "main")
                       for replica in range(protocol["process_replicas"])}
        assert path_orders == set(itertools.permutations(MODES))
    schedule["runs"] = [dict(replica=replica + 1, query=shape, series=series, path=path)
                        for replica, shape, series, path in ordered_runs]

    destination.mkdir()
    shutil.copyfile(SUITE / "queries.json", destination / "queries.json")
    strings.write_json(destination / "schedule.json", schedule)
    metadata = execution.metadata(binary, snapshot, manifest, schedule)
    metadata.update(protocol_commit=protocol_commit, staging_source=protocol["staging_source"],
                    queries_sha256=execution.digest(destination / "queries.json"),
                    schedule_sha256=execution.digest(destination / "schedule.json"),
                    controller_sha256=execution.digest(LEGACY / "run.py"),
                    round2_controller_sha256=execution.digest(pathlib.Path(__file__)))
    strings.write_json(destination / "metadata.json", metadata)

    receipt = preflight(binary, snapshot, destination / "preflight", shapes, schedule)
    strings.write_json(destination / "preflight-receipt.json", receipt)
    assert metadata["binary_sha256"] == execution.digest(binary)
    started = time.strftime("%FT%TZ", time.gmtime())
    timings = measure(binary, snapshot, destination / "timings", schedule, receipt, ordered_runs)
    summary = summarize(timings, receipt)
    summary.update(started=started, finished=time.strftime("%FT%TZ", time.gmtime()),
                   processes=sum(map(len, timings.values())))
    strings.write_json(destination / "summary.json", summary)
    profile(binary, snapshot, destination, schedule, receipt, summary)


if __name__ == "__main__":
    main()
