"""Compare uninstrumented #1389 builds in one paired session."""

import argparse
import gzip
import json
from pathlib import Path
import runpy
import statistics
import subprocess
import time

if not __debug__:
    raise RuntimeError("validation requires Python assertions")

HERE = Path(__file__).resolve().parent
PARSERS = HERE.parent / "parsers"
parsers = runpy.run_path(str(PARSERS / "run.py"))
execution, strings = (parsers[name] for name in ("execution", "strings"))
modes = parsers["modes"]
compare_results = parsers["comparison_failure"]
expected_type = parsers["expected_type"]


def comparison_failure(binary, left, right, output, deadline, schedule):
    if left["arrow_types"] != right["arrow_types"]:
        return f"executed Arrow types differ: {left['arrow_types']} versus {right['arrow_types']}"
    _, failure = compare_results(binary, left, right, output, deadline, schedule)
    return failure


def validate_preflight(binaries, snapshot, output, schedule, deadline, queries):
    outputs, excluded = {}, {}
    for query in queries:
        name = query["id"]
        for mode in modes(name):
            for role, binary in binaries.items():
                result = strings.invoke(binary, snapshot, output / role / name / mode, schedule, deadline,
                                        mode="preflight", configuration=mode, query=name)
                assert result["id"] == name and result["configuration"] == mode
                if result["rows"] != query["expected_rows"]:
                    excluded.setdefault(name, []).append(
                        {"mode": mode, "reason": f"unexpected row count {result['rows']}", "pair": role}
                    )
                strings.validate_telemetry(result, schedule)
                if not expected_type(query, result):
                    excluded.setdefault(name, []).append(
                        {"mode": mode, "reason": f"unexpected executed Arrow types {result['arrow_types']}", "pair": role}
                    )
                outputs[role, name, mode] = result
            left, right = [outputs[role, name, mode] for role in binaries]
            failure = comparison_failure(
                binaries["candidate"], left, right, output / "across-builds" / name / mode, deadline, schedule,
            )
            if failure:
                excluded.setdefault(name, []).append({"mode": mode, "reason": failure, "pair": "across builds"})
        reference_mode = "native" if "native" in modes(name) else "spark-off"
        for role, binary in binaries.items():
            for mode in ("spark-off", "spark-on"):
                if mode == reference_mode:
                    continue
                failure = comparison_failure(
                    binary, outputs[role, name, reference_mode], outputs[role, name, mode],
                    output / "reference" / role / name / mode, deadline, schedule,
                )
                if failure:
                    excluded.setdefault(name, []).append({"mode": mode, "reason": failure, "pair": role})
    if any(query["id"] == "interval_day_time" for query in queries):
        for role in binaries:
            for mode in ("spark-off", "spark-on"):
                cast = outputs[role, "interval_day_time", mode]
                projection = outputs[role, "interval_day_time_input", mode]
                try:
                    execution.require_same_source_io(cast["source_io"], projection["source_io"])
                except AssertionError as error:
                    excluded.setdefault("interval_day_time", []).append(
                        {"mode": mode, "reason": str(error), "pair": f"{role} projection"}
                    )
    strings.write_json(output / "observations.json", [
        dict(result, build=role, ipc_sha256=execution.digest(Path(result["ipc_path"])))
        for (role, _, _), result in outputs.items()
    ])
    strings.write_json(output / "excluded.json", excluded)
    return outputs, excluded


def statistics_by_shape(records, queries, excluded):
    def timing(build, name, mode, series="main", arm=None):
        process_medians = [
            statistics.median(sample["query_complete_ns"] for sample in record["samples"])
            for record in records
            if (record["build"], record["query"], record["configuration"], record["series"], record.get("arm"))
            == (build, name, mode, series, arm)
        ]
        assert len(process_medians) == 3
        return statistics.median(process_medians)

    summary = {}
    for query in queries:
        name, rows = query["id"], query["expected_rows"]
        if name.endswith("_input") or name in excluded:
            continue
        entries = []
        for mode in ("spark-off", "spark-on"):
            for build in ("staging", "candidate"):
                value = timing(build, name, mode)
                if "native" in modes(name):
                    reference = timing(build, name, "native")
                    arms = [timing(build, name, "native", "noise", arm) for arm in "ab"]
                else:
                    projection = "interval_day_time_input"
                    reference = timing(build, projection, mode)
                    arms = [timing(build, name, mode, "noise", arm)
                            - timing(build, projection, mode, "noise", arm) for arm in "ab"]
                noise = abs(arms[0] - arms[1])
                entries.append(dict(build=build, mode=mode, rows=rows, query_ms=value / 1e6,
                                    reference_ms=reference / 1e6, gap_ns_per_row=(value - reference) / rows,
                                    noise_ns_per_row=noise / rows,
                                    gap_above_native_noise=value - reference > noise if "native" in modes(name) else None))
        summary[name] = entries
    return summary


def main():
    cli = argparse.ArgumentParser(description=__doc__)
    for name in ("staging", "candidate", "snapshot", "output"):
        cli.add_argument(name, type=Path)
    cli.add_argument("--manifest", type=Path, default=HERE / "manifest.json")
    args = cli.parse_args()
    binaries = {name: getattr(args, name).resolve() for name in ("staging", "candidate")}
    snapshot, output = args.snapshot.resolve(), args.output.resolve()
    assert not subprocess.check_output(["git", "status", "--porcelain"], cwd=HERE.parents[3], text=True).strip()
    manifest = execution.read(args.manifest)
    assert execution.digest(PARSERS / "queries.json") == manifest["queries_sha256"]
    assert execution.digest(PARSERS / "schedule.json") == manifest["schedule_sha256"]
    workload, schedule = parsers["load_and_validate_workload"]()
    names = manifest["statements"]
    queries = [query for query in workload["queries"] if query["id"] in names]
    assert [query["id"] for query in queries] == names
    fixture = strings.check_fixture(snapshot)
    receipts = {role: {"path": str(binary), "sha256": execution.digest(binary)} for role, binary in binaries.items()}
    assert receipts["staging"]["sha256"] != receipts["candidate"]["sha256"], "build executables are identical"
    output.mkdir()
    strings.write_json(output / "session.json", {
        "protocol_git_head": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=HERE, text=True).strip(),
        "protocol": manifest,
        "snapshot": fixture,
        "binaries": receipts,
    })
    preflight, excluded = validate_preflight(
        binaries, snapshot, output / "preflight", schedule,
        time.monotonic() + schedule["preflight_timeout_seconds"], queries,
    )
    if "interval_day_time_input" in excluded:
        excluded.setdefault("interval_day_time", []).append(
            {"reason": "same-mode input projection failed its control"}
        )
    eligible = set(names) - excluded.keys()
    deadline = time.monotonic() + manifest["session_limit_seconds"]
    records = []
    for scheduled in schedule["runs"]:
        name, mode = scheduled["query"], scheduled["configuration"]
        if name not in eligible:
            continue
        offset = names.index(name) + modes(name).index(mode) + (scheduled.get("arm") == "b")
        order = tuple(binaries) if (scheduled["replica"] + offset) % 2 else tuple(reversed(binaries))
        for role in order:
            print(role, scheduled["process"], flush=True)
            result = strings.invoke(binaries[role], snapshot, output / role / scheduled["process"], schedule,
                                    deadline, mode="measure", configuration=mode, query=name)
            assert result["id"] == name and result["configuration"] == mode
            assert len(result["samples"]) == schedule["samples"]
            expected = preflight[role, name, mode]
            for sample in result["samples"]:
                assert sample["rows"] == expected["rows"]
                strings.validate_telemetry(sample, schedule)
                execution.require_same_source_io(sample["source_io"], expected["source_io"])
            records.append(dict(result, **scheduled, build=role))
    assert len(records) == 2 * sum(run["query"] in eligible for run in schedule["runs"])
    with gzip.GzipFile(output / "samples.jsonl.gz", "xb", mtime=0) as samples:
        for record in records:
            samples.write((json.dumps(record, separators=(",", ":")) + "\n").encode())
    strings.write_json(output / "summary.json", {
        "sample_count": sum(len(record["samples"]) for record in records),
        "query_processes": len(records), "excluded": excluded,
        "shapes": statistics_by_shape(records, queries, excluded),
    })


if __name__ == "__main__":
    main()
