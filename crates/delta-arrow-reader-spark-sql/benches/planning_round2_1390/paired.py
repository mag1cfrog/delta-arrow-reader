"""Compare the four frozen planning configurations with both builds interleaved."""

import itertools
import pathlib
import platform
import statistics
import subprocess
import sys
import time

HERE = pathlib.Path(__file__).resolve().parent
sys.dont_write_bytecode = True
sys.path.insert(0, str(HERE.parent / "planning_attribution"))
import assemble
import run
import summarize

ARMS = ("staging", "candidate")


def verify_build(receipt_path, expected_source):
    assemble.BASELINE = expected_source
    receipt = run.verify_build(receipt_path)
    assert receipt["mode"] == "canonical"
    assert receipt["baseline"] == expected_source
    return receipt


def capture_traces(builds, delta, evidence):
    evidence.mkdir()
    for mode in run.CONFIGURATIONS:
        expected_count = 309 if mode.startswith("spark-") else 30
        for arm in ARMS:
            process = f"{arm}-{mode}"
            reference = evidence / ("staging-" + mode + ".jsonl") if arm == "candidate" else "-"
            rows = run.invoke(builds[arm]["binary"], ["trace", str(delta), mode, process, str(reference)],
                              evidence, process)
            assert len(rows) == expected_count and len({row["id"] for row in rows}) == expected_count
            assert all(row["kind"] == "trace" and row["configuration"] == mode for row in rows)
            assert not (evidence / (process + ".stderr")).stat().st_size


def process_order():
    frozen = run.read(assemble.HERE / "schedule.json")["runs"]
    canonical = [item for item in frozen if item["mode"] == "canonical"]
    assert len(canonical) == 30
    for replica in range(1, 6):
        for item in canonical:
            if item["pair"] != replica:
                continue
            if item["series"] == "noise":
                yield item, "candidate"
            else:
                for arm in (ARMS if replica % 2 else ARMS[::-1]):
                    yield item, arm


def capture_times(builds, delta, evidence, ordered, expected_ids, timeout):
    evidence.mkdir()
    deadline = time.monotonic() + timeout
    records = []
    for item, arm in ordered:
        process = item["process"] + "-" + arm
        mode = item["configuration"]
        assert time.monotonic() < deadline, "bounded planning schedule expired"
        print(process, flush=True)
        rows = run.invoke(builds[arm]["binary"], ["measure", str(delta), mode, process, "-"],
                          evidence, process)
        assert time.monotonic() <= deadline, "bounded planning schedule expired"
        assert len(rows) == 30 and {row["id"] for row in rows} == expected_ids
        assert not (evidence / (process + ".stderr")).stat().st_size
        for row in rows:
            assert row["process"] == process and row["configuration"] == mode
            assert row["events"] == [[]] * 9
            assert all(len(row[phase + "_ns"]) == 9 for phase in summarize.PHASES)
            assert row["total_ns"] == [a + b for a, b in zip(row["logical_ns"], row["physical_ns"], strict=True)]
            records.append(dict(item, **{name: value for name, value in row.items() if name != "process"},
                                process=process, build=arm))
    return records


def aggregate(records, expected_ids):
    values = {}
    for row in records:
        role = row["arm"] if row["series"] == "noise" else row["build"]
        for phase in summarize.PHASES:
            key = (row["id"], row["configuration"], row["series"], role, phase)
            values.setdefault(key, {})[row["pair"]] = statistics.median(row[phase + "_ns"])
    assert all(sorted(group) == list(range(1, 6)) for group in values.values())

    def series(query, configuration, metric, arm):
        return list(values[query, configuration, "main", arm, metric].values())

    def noise(query, metric):
        return abs(statistics.median(values[query, "native-configured", "noise", "a", metric].values())
                   - statistics.median(values[query, "native-configured", "noise", "b", metric].values()))

    summary = []
    for query, mode, phase in itertools.product(sorted(expected_ids), run.CONFIGURATIONS, summarize.PHASES):
        before, after = (series(query, mode, phase, arm) for arm in ARMS)
        before_native, after_native = (statistics.median(series(query, "native-configured", phase, arm))
                                       for arm in ARMS)
        summary.append(dict(id=query, configuration=mode, phase=phase,
                            staging_median_ns=statistics.median(before), candidate_median_ns=statistics.median(after),
                            staging_minimum_ns=min(before), staging_maximum_ns=max(before),
                            candidate_minimum_ns=min(after), candidate_maximum_ns=max(after),
                            staging_vs_configured_ns=statistics.median(before)-before_native,
                            candidate_vs_configured_ns=statistics.median(after)-after_native,
                            candidate_minus_staging_ns=[a-b for b, a in zip(before, after, strict=True)],
                            native_floor_ns=noise(query, phase)))
    return summary


def main():
    if sys.flags.optimize:
        raise SystemExit("validation requires Python assertions")
    if len(sys.argv) != 6 or platform.system() != "Linux":
        raise SystemExit("usage on Linux: paired.py SETTINGS STAGING_RECEIPT CANDIDATE_RECEIPT DELTA NEW_EVIDENCE")
    settings, before, after, delta, output = (pathlib.Path(p).resolve() for p in sys.argv[1:])
    commit = run.protocol_commit()
    config = run.read(settings)
    assert subprocess.check_output(
        ["git", "show", f"{commit}:{settings.relative_to(assemble.REPO)}"], cwd=assemble.REPO
    ) == settings.read_bytes()
    builds = {arm: verify_build(path, config[arm + "_source"])
              for arm, path in zip(ARMS, (before, after), strict=True)}
    assert builds["candidate"]["protocol_commit"] == commit
    assert builds["staging"]["assembly"]["assembly"] == builds["candidate"]["assembly"]["assembly"]
    assert builds["staging"]["rustc"] == builds["candidate"]["rustc"]
    assert builds["staging"]["command"] == builds["candidate"]["command"]
    for key in ("CARGO_BUILD_JOBS", "CARGO_PROFILE_RELEASE_DEBUG"):
        assert builds["staging"]["build_environment"][key] == builds["candidate"]["build_environment"][key]
    expected_delta = run.read(assemble.WORKSPACE / "benches/reports/planning-1379-metadata.json")["delta_files"]
    assert run.delta_hashes(delta) == expected_delta
    manifest = assemble.WORKSPACE / "benches/planning/queries.json"
    assert assemble.digest(manifest) == config["query_manifest_sha256"]
    expected_ids = {q["id"] for q in run.read(manifest)["queries"] if q["group"] in ("paired", "stress")}
    assert len(expected_ids) == 30
    ordered = list(process_order())
    assert len(ordered) == config["maximum_processes"] == 50
    output.mkdir()
    run.write(output / "metadata.json", dict(protocol_commit=commit, builds=builds, order=ordered,
              settings_sha256=assemble.digest(settings), delta_files=expected_delta))
    capture_traces(builds, delta, output / "traces")
    started = time.strftime("%FT%TZ", time.gmtime())
    records = capture_times(builds, delta, output / "timings", ordered, expected_ids, config["timeout_seconds"])
    finished = time.strftime("%FT%TZ", time.gmtime())
    assert len(records) == 30 * len(ordered)
    assert run.delta_hashes(delta) == expected_delta
    rows = aggregate(records, expected_ids)
    run.compressed(output / "samples.jsonl.gz", records)
    run.write(output / "summary.json", dict(started=started, finished=finished, process_count=len(ordered), rows=rows))


if __name__ == "__main__":
    main()
