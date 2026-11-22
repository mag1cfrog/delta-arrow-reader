"""Compare the #1389 interval cases with a quoted year-month case."""
import gzip
import json
from pathlib import Path
import runpy
from statistics import median
import subprocess
import sys
import time

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
BENCHES = HERE.parent
ORIGINAL = runpy.run_path(str(BENCHES / "interval_1389/compare.py"))
execution, strings, parsers, modes = (ORIGINAL[key] for key in ("execution", "strings", "parsers", "modes"))
IDS = ("interval_year_month", "interval_day_time", "interval_calendar",
       "interval_day_time_input", "interval_year_month_quoted")


def workload():
    manifest, schedule = parsers["load_and_validate_workload"]()
    queries = {query["id"]: query for query in manifest["queries"]}
    q = queries["interval_year_month"] | execution.read(HERE / "quoted-query.json")
    assert q["id"] == IDS[-1] and q["spark_sql"].endswith(" FROM bench WHERE " + q["input_predicate"])
    queries[q["id"]] = q
    runs = []
    for run in schedule["runs"]:
        if run["query"] in IDS:
            runs.append(run)
        if run["query"] == "interval_year_month":
            runs.append(run | {"query": q["id"], "process": run["process"].replace(run["query"], q["id"])})
    return manifest | {"queries": [queries[name] for name in IDS]}, schedule | {"runs": runs}


def main(staging, candidate, snapshot, output, staging_source):
    assert not subprocess.check_output(["git", "status", "--porcelain"], cwd=BENCHES, text=True).strip()
    head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=BENCHES, text=True).strip()
    manifest, schedule = workload()
    binaries, receipts = {}, {}
    for role, directory in (("staging", staging), ("candidate", candidate)):
        receipt = execution.read(directory / "receipt.json")
        binary = directory / "execution"
        assert receipt["sha256"] == execution.digest(binary)
        assert receipt["queries"] == manifest
        binaries[role], receipts[role] = binary, receipt
    assert receipts["staging"]["toolchain"] == receipts["candidate"]["toolchain"]
    assert receipts["staging"]["harness_sha256"] == receipts["candidate"]["harness_sha256"]
    assert receipts["staging"]["source_commit"] == staging_source
    same_build = receipts["staging"]["sha256"] == receipts["candidate"]["sha256"]
    if same_build:
        assert receipts["staging"] == receipts["candidate"]
        repository = BENCHES.parents[2]
        scripts = (HERE / "run.py", BENCHES / "reports/interval-1389-round3-protocol.md")
        subprocess.run(["git", "diff", "--exit-code", staging_source, head, "--", ".",
                        *(f":!{path.relative_to(repository)}" for path in scripts)],
                       cwd=repository, check=True, capture_output=True)
    else:
        assert receipts["candidate"]["source_commit"] == head
        subprocess.run(["git", "merge-base", "--is-ancestor", staging_source, head],
                       cwd=BENCHES, check=True)
    fixture = strings.check_fixture(snapshot)
    output.mkdir()
    queries = manifest["queries"]
    preflight, excluded = ORIGINAL["validate_preflight"](
        binaries, snapshot, output / "preflight", schedule,
        time.monotonic() + schedule["preflight_timeout_seconds"], queries,
    )
    if "interval_day_time_input" in excluded:
        excluded.setdefault("interval_day_time", []).append({"reason": "projection control excluded"})
    records = []
    deadline = time.monotonic() + 1800
    with gzip.GzipFile(output / "samples.jsonl.gz", "xb", mtime=0) as stream:
        def emit(value):
            stream.write((json.dumps(value, separators=(",", ":")) + "\n").encode())
            stream.flush()
        emit({"kind": "session", "protocol_commit": head, "builds": receipts, "same_build": same_build,
              "snapshot_log_sha256": fixture["derived_delta_log_sha256"], "excluded": excluded})
        for scheduled in schedule["runs"]:
            name, mode = scheduled["query"], scheduled["configuration"]
            if name in excluded:
                continue
            offset = IDS.index(name) + modes(name).index(mode) + (scheduled.get("arm") == "b")
            order = tuple(binaries) if (scheduled["replica"] + offset) % 2 else tuple(reversed(binaries))
            for role in order:
                result = strings.invoke(binaries[role], snapshot, output / role / scheduled["process"],
                                        schedule, deadline, "measure", mode, name)
                record = dict(result, **scheduled, build=role,
                              binary_source_commit=receipts[role]["source_commit"])
                emit(record)
                assert result["id"] == name and result["configuration"] == mode
                assert len(result["samples"]) == schedule["samples"]
                expected = preflight[role, name, mode]
                for sample in result["samples"]:
                    assert sample["rows"] == expected["rows"]
                    strings.validate_telemetry(sample, schedule)
                    execution.require_same_source_io(sample["source_io"], expected["source_io"])
                records.append(record)
                print(role, scheduled["process"], flush=True)
        assert len(records) == 2 * sum(run["query"] not in excluded for run in schedule["runs"])
        trends = paired_changes(records)
        cast_failures, control_flags = timing_flags(trends)
        summary = {
            "sample_count": sum(len(record["samples"]) for record in records),
            "processes": len(records), "excluded": excluded, "same_build": same_build,
            "gaps": ORIGINAL["statistics_by_shape"](records, queries, excluded), "paired": trends,
            "cast_failures": [] if same_build else cast_failures,
            "controls_requiring_comparison": [] if same_build else control_flags,
        }
        emit({"kind": "summary", **summary})
    strings.write_json(output / "summary.json", summary)
    return bool(excluded or (not same_build and (cast_failures or control_flags)))


def timing_flags(trends):
    cast_failures, control_flags = [], []
    for key, outcome in trends.items():
        query, mode = key.split("/")
        if mode == "native" or query == "interval_day_time_input":
            if outcome["status"] == "regression":
                control_flags.append(key)
        elif outcome["status"] != "faster":
            cast_failures.append(key)
    return cast_failures, control_flags


def paired_changes(records):
    groups = {}
    for record in records:
        if record["series"] != "main":
            continue
        key = record["query"] + "/" + record["configuration"]
        pair = groups.setdefault(key, {}).setdefault(record["replica"], {})
        pair[record["build"]] = median(s["query_complete_ns"] for s in record["samples"])
    result = {}
    for key, data in groups.items():
        assert set(data) == {1, 2, 3} and all(set(pair) == {"staging", "candidate"} for pair in data.values())
        before = [p["staging"] for p in data.values()]
        after = [p["candidate"] for p in data.values()]
        delta = median(b - a for a, b in zip(before, after))
        noise = max(before) - min(before)
        result[key] = {"staging_ms": median(before) / 1e6, "candidate_ms": median(after) / 1e6,
                       "paired_delta_ms": delta / 1e6, "staging_spread_ms": noise / 1e6,
                       "status": "faster" if delta < -noise else "regression" if delta > noise else "within spread"}
    return result


if __name__ == "__main__":
    *paths, source = sys.argv[1:]
    sys.exit(main(*(Path(arg).resolve() for arg in paths), source))
