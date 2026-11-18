#!/usr/bin/env python3
"""Run the committed planning schedule once; retain every process and raw sample."""

import gzip
import hashlib
import json
import pathlib
import platform
import subprocess
import sys

HERE = pathlib.Path(__file__).resolve().parent
WORKSPACE = HERE.parents[1]


def read(path):
    return json.loads(path.read_text())


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def self_check():
    queries = read(HERE / "queries.json")
    schedule = read(HERE / "schedule.json")
    ids = [query["id"] for query in queries["queries"]]
    assert len(ids) == len(set(ids)) == 309
    assert sum(query["group"] == "paired" for query in queries["queries"]) == 25
    assert sum(query["group"] == "stress" for query in queries["queries"]) == 5
    stress = {query["id"]: query["sql"] for query in queries["queries"] if query["group"] == "stress"}
    assert stress["stress/case_200"].count("WHEN ") == 200
    assert len(stress["stress/in_1000"].rsplit("(", 1)[1].rstrip(")").split(",")) == 1000
    assert stress["stress/join_six"].count("JOIN ") == 5
    assert stress["stress/cte_eight"].count("WITH ") == 8
    assert len(schedule["runs"]) == 30
    assert len({run["process"] for run in schedule["runs"]}) == 30
    assert {run["configuration"] for run in schedule["runs"]} == {
        "native-plain", "native-configured", "spark-off", "spark-on"
    }
    expected_statements = set()
    for name, expected in queries["sources"].items():
        path = WORKSPACE / "tests/fixtures" / name
        assert sha256(path) == expected, name
        source = read(path)
        definitions = source.get("cases", source.get("corpora", [{}])[0].get("cases", []))
        for definition in definitions:
            for ansi in [definition["ansi"]] if "ansi" in definition else [False, True]:
                expected_statements.add((name, definition["id"], definition["sql"], ansi))
    frozen_statements = {(query["source_manifest"], query["source_id"], query["sql"], ansi)
                         for query in queries["queries"] if query["group"] == "sweep"
                         for ansi in query["ansi"]}
    assert frozen_statements == expected_statements, "ordinary source inventory differs"
    eligibility = HERE / "eligibility.jsonl"
    if eligibility.exists():
        statuses = [json.loads(line) for line in eligibility.read_text().splitlines()]
        expected = {(query["id"], "spark-on" if ansi else "spark-off")
                    for query in queries["queries"] if query["group"] == "sweep"
                    for ansi in query["ansi"]}
        actual = {(row["id"], row["configuration"]) for row in statuses}
        assert len(statuses) == len(actual) == len(expected) and actual == expected
        assert all(row["status"] in ("included", "excluded") for row in statuses)
        assert all(isinstance(row.get("error"), str) for row in statuses if row["status"] == "excluded")
    print("planning manifest and schedule checks passed")


def run(binary, destination, delta_root):
    if platform.system() != "Linux":
        raise SystemExit("release measurements require Linux")
    binary, destination, delta_root = map(lambda value: pathlib.Path(value).resolve(), (binary, destination, delta_root))
    schedule = read(HERE / "schedule.json")
    eligibility = HERE / "eligibility.jsonl"
    if not eligibility.exists():
        raise SystemExit("freeze preflight eligibility before timing")
    if subprocess.check_output(["git", "status", "--porcelain"], cwd=WORKSPACE, text=True).strip():
        raise SystemExit("commit protocol, queries, eligibility and harness before timing")
    if destination.exists():
        raise SystemExit("schedule destination exists; unchanged schedules must not be repeated")
    destination.mkdir(parents=True)
    metadata = {
        "protocol_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=WORKSPACE, text=True).strip(),
        "binary_sha256": sha256(binary),
        "queries_sha256": sha256(HERE / "queries.json"),
        "schedule_sha256": sha256(HERE / "schedule.json"),
        "eligibility_sha256": sha256(eligibility),
        "rustc": subprocess.check_output(["rustc", "+1.96.0", "--version", "--verbose"], text=True),
        "uname": platform.uname()._asdict(),
        "cpu": pathlib.Path("/proc/cpuinfo").read_text(),
        "cpu_governor": pathlib.Path(f"/sys/devices/system/cpu/cpu{schedule['cpu']}/cpufreq/scaling_governor").read_text().strip(),
        "delta_files": {str(path.relative_to(delta_root)): sha256(path) for path in sorted(delta_root.rglob("*")) if path.is_file()},
        "schedule": schedule,
    }
    (destination / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
    records = []
    for item in schedule["runs"]:
        print(item["process"], flush=True)
        command = ["taskset", "-c", str(schedule["cpu"]), str(binary), "measure", str(delta_root), item["configuration"], item["process"], item["series"], str(eligibility)]
        with (destination / (item["process"] + ".jsonl")).open("x") as output, (destination / (item["process"] + ".stderr")).open("x") as errors:
            subprocess.run(command, stdout=output, stderr=errors, timeout=schedule["process_timeout_seconds"], check=True)
        samples = [json.loads(line) for line in (destination / (item["process"] + ".jsonl")).read_text().splitlines()]
        assert samples and all(len(row[phase + "_ns"]) == schedule["samples"] for row in samples for phase in ("logical", "physical", "total"))
        for row in samples:
            row.update(series=item["series"], arm=item.get("arm"))
            records.append(row)
    with (destination / "samples.jsonl.gz").open("xb") as output:
        with gzip.GzipFile(fileobj=output, mode="wb", mtime=0) as compressed:
            for record in records:
                compressed.write((json.dumps(record, separators=(",", ":")) + "\n").encode())
    (destination / "complete.json").write_text(json.dumps({"processes":len(schedule["runs"]),"records":len(records)}) + "\n")


if __name__ == "__main__":
    self_check()
    if sys.argv[1:] == ["--check"]:
        pass
    elif len(sys.argv) == 4:
        run(*sys.argv[1:])
    else:
        raise SystemExit("usage: run.py --check | BINARY NEW_DESTINATION DELTA_ROOT")
