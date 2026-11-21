"""Compare two pinned SF1 release builds in one serial session."""

import pathlib
import statistics
import subprocess
import sys
import time

import run as sf1

ARMS = ("staging", "candidate")


def compare_outputs(binaries, evidence, schedule, receipts, shapes):
    work = evidence / "before-after"
    deadline = time.monotonic() + schedule["preflight_timeout_seconds"]
    eligible = []
    for shape in shapes:
        if shape not in receipts["staging"]["eligible_shapes"]:
            continue
        assert shape in receipts["candidate"]["eligible_shapes"], (shape, receipts["candidate"]["excluded_shapes"])
        exports = {arm: sf1.execution.read(evidence / arm / shape / "exports.json") for arm in ARMS}
        for mode in sf1.MODES:
            before, after = (exports[arm][mode] for arm in ARMS)
            comparison = sf1.strings.compare_results(
                binaries["candidate"], before["ipc_path"], after["ipc_path"], work / shape / mode,
                deadline, schedule["preflight_address_space_bytes"])
            assert comparison["equal"] and comparison["rows_compared"] == before["rows"] == after["rows"]
            assert before["arrow_types"] == after["arrow_types"] and before["sql_schema"] == after["sql_schema"]
            sf1.execution.require_same_source_io(after["source_io"], before["source_io"])
        eligible.append(shape)
    return eligible


def measure(binaries, snapshot, work, schedule, receipts, order, eligible):
    deadline = time.monotonic() + schedule["schedule_timeout_seconds"]
    values = {}
    for replica, shape, series, path in order:
        if shape not in eligible:
            continue
        mode = path if series == "main" else "native"
        arms = ARMS[::(-1 if replica % 2 else 1)] if series == "main" else ("candidate",)
        for arm in arms:
            name = f"{series}-{replica + 1:02}-{shape}-{path}-{arm}"
            print(name, flush=True)
            result = sf1.strings.invoke(binaries[arm], snapshot, work / name, schedule,
                                        deadline, "measure", mode, shape)
            assert result["kind"] == "query_samples" and result["source_commit"] == sf1.strings.BASELINE
            sf1.strings.validate_samples(result, shape, mode, receipts[arm], schedule)
            medians = values.setdefault((shape, series, path, arm), [])
            assert len(medians) == replica
            medians.append(statistics.median(row["query_complete_ns"] for row in result["samples"]))
    return values


def summarize(shapes, values):
    result = {}
    for shape in shapes:
        controls = {arm: sf1.STATS(values[shape, "noise", arm, "candidate"]) for arm in ("a", "b")}
        floor = abs(controls["a"]["median"] - controls["b"]["median"])
        main = {}
        for mode in sf1.MODES:
            timing = {arm: values[shape, "main", mode, arm] for arm in ARMS}
            delta = [after - before for before, after in zip(*timing.values(), strict=True)]
            main[mode] = dict(staging=sf1.STATS(timing["staging"]), candidate=sf1.STATS(timing["candidate"]),
                              candidate_minus_staging_ns=delta)
        result[shape] = dict(paths=main, native_controls_ns=controls, native_floor_ns=floor)
    return result


def main():
    if sys.flags.optimize:
        raise SystemExit("validation requires Python assertions")
    if len(sys.argv) != 6 or sf1.platform.system() != "Linux":
        raise SystemExit("usage on Linux: compare.py SETTINGS STAGING_BINARY CANDIDATE_BINARY SNAPSHOT NEW_EVIDENCE_DIR")
    settings, staging, candidate, snapshot, evidence = map(lambda arg: pathlib.Path(arg).resolve(), sys.argv[1:])
    clean = subprocess.check_output(["git", "status", "--porcelain"], cwd=sf1.SUITE, text=True)
    assert not clean.strip(), "commit the comparison protocol before clocks"
    protocol_commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=sf1.SUITE, text=True).strip()
    config = sf1.execution.read(settings)
    assert sf1.execution.digest(sf1.LEGACY / "queries.json") == config["query_manifest_sha256"]
    binaries = dict(staging=staging, candidate=candidate)
    receipts = {}
    for arm, binary in binaries.items():
        source = (binary.parent / "source.txt").read_text().strip()
        sha256 = sf1.execution.digest(binary)
        assert source == config[arm + "_source"]
        assert sha256 == (binary.parent / "executable-sha256.txt").read_text().split()[0]
        receipts[arm] = dict(source=source, sha256=sha256)
    for name in ("toolchain.txt", "sources-sha256.txt"):
        assert (staging.parent / name).read_bytes() == (candidate.parent / name).read_bytes()

    schedule = sf1.execution.read(sf1.LEGACY / "schedule.json")
    schedule["schedule_timeout_seconds"] = config["timing_timeout_seconds"]
    shapes = config["shapes"]
    assert set(shapes) <= set(sf1.strings.IDS)
    order = list(sf1.run_order(shapes, config["process_replicas"]))
    assert len(set(order)) == len(order)
    process_count = sum(2 if series == "main" else 1 for _, _, series, _ in order)
    assert process_count == config["maximum_processes"]
    evidence.mkdir()
    sf1.strings.write_json(evidence / "metadata.json", dict(protocol_commit=protocol_commit,
                           settings_sha256=sf1.execution.digest(settings), builds=receipts, order=order,
                           schedule={key: value for key, value in schedule.items() if key != "runs"}))
    preflights = {arm: sf1.preflight(binary, snapshot, evidence / arm, shapes, schedule)
                  for arm, binary in binaries.items()}
    eligible = compare_outputs(binaries, evidence, schedule, preflights, shapes)
    sf1.strings.write_json(evidence / "preflight-receipts.json", preflights)
    for arm, binary in binaries.items():
        assert receipts[arm]["sha256"] == sf1.execution.digest(binary)
    started = time.strftime("%FT%TZ", time.gmtime())
    values = measure(binaries, snapshot, evidence / "timings", schedule, preflights, order, eligible)
    sf1.strings.write_json(evidence / "summary.json", dict(shapes=summarize(eligible, values),
                           excluded_shapes=preflights["staging"]["excluded_shapes"],
                           started=started, finished=time.strftime("%FT%TZ", time.gmtime()),
                           processes=sum(map(len, values.values()))))


if __name__ == "__main__":
    main()
