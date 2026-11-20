#!/usr/bin/env python3
"""Compare like-sized XPath runs and validate the full-input candidate."""

import argparse
import json
from pathlib import Path
import statistics
import sys
import time

sys.dont_write_bytecode = True
from run import FROZEN, HERE, MANIFEST, MODES, build, manifest, runner, write

ARMS = ("staging", "candidate")
PROTOCOL = HERE / "reuse.json"


def fixture_value(row):
    mask = (1 << 64) - 1
    value = (row + 0x13915EED20261119) & mask
    value = ((value ^ (value >> 30)) * 0xBF58476D1CE4E5B9) & mask
    value = ((value ^ (value >> 27)) * 0x94D049BB133111EB) & mask
    return (value ^ (value >> 31)) % 31


def observe(binary, name, work, rows, schedule, deadline):
    observed = runner.invoke(binary, ["preflight", MANIFEST, name, work / "plans"],
                             work, schedule, deadline, preflight=True)
    if observed["status"] != "ok":
        raise ValueError(f"preflight {observed['status']}")
    result = observed["records"][-1]
    outcomes = result["results"]
    baselines = result["baselines"]
    expression = f'name: "{name.rsplit(".", 1)[1]}",'
    if (
        [r["configuration"] for r in outcomes] != list(MODES)
        or any(r["status"] != "ok" or r["rows"] != rows or r["source_guard_rows"] != rows
               or not any(expression in e for e in r["function_evidence"]["expressions"])
               for r in outcomes)
        or len(baselines) != 3 or any(r["rows"] != rows for r in baselines)
    ):
        raise ValueError("source, physical function or result guard failed")
    return {r["configuration"]: (r["types"], work / "plans" / f"{r['configuration']}.rows.json")
            for r in outcomes}


def row_reference(path, rows):
    known = {}
    seen = set()
    for row in runner.read(path):
        index, value = json.loads(row)
        index = int(index)
        if index in seen or not 0 <= index < rows:
            raise ValueError("duplicate or out-of-bounds staging row ID")
        seen.add(index)
        key = fixture_value(index) if index % 16 else None
        if known.setdefault(key, value) != value:
            raise ValueError("same XML input had different staging results")
    if len(seen) != rows or set(known) != {None, *range(31)} or known[None] is not None:
        raise ValueError("staging reference does not cover every seeded XML payload and NULL")
    return known


def verify_full(path, reference, rows):
    seen = bytearray(rows)
    count = 0
    for row in runner.read(path):
        index, value = json.loads(row)
        index = int(index)
        if not 0 <= index < rows or seen[index]:
            raise ValueError("duplicate or out-of-bounds candidate row ID")
        key = fixture_value(index) if index % 16 else None
        if reference[key] != value:
            raise ValueError(f"full-input candidate result differs at row {index}")
        seen[index] = 1
        count += 1
    if count != rows:
        raise ValueError("missing full-input candidate row ID")


def select(builds, ids, output, schedule, deadline, protocol):
    eligible, excluded = {}, {}
    for name in ids:
        try:
            short = protocol["paired_rows"]
            reference = observe(builds["staging"], name, output / name / "staging",
                                short, schedule, deadline)
            candidate = observe(builds["candidate"], name, output / name / "candidate",
                                short, schedule, deadline)
            full = observe(builds["full"], name, output / name / "full",
                           protocol["full_rows"], schedule, deadline)
            for mode in MODES:
                types, rows = reference[mode]
                other_types, other_rows = candidate[mode]
                full_types, full_rows = full[mode]
                if types != other_types or types != full_types:
                    raise ValueError(f"{mode}: executed types differ")
                if rows.read_bytes() != other_rows.read_bytes():
                    raise ValueError(f"{mode}: paired rows differ")
                verify_full(full_rows, row_reference(rows, short), protocol["full_rows"])
            eligible[name] = {mode: reference[mode][0] for mode in MODES}
        except ValueError as error:
            excluded[name] = str(error)
    return eligible, excluded


def samples(binary, name, mode, work, rows, types, schedule, deadline):
    work.parent.mkdir(parents=True, exist_ok=True)
    id_file = work.with_suffix(".ids.json")
    write(id_file, [name])
    execution = runner.invoke(binary, ["measure-set", MANIFEST, id_file, mode],
                              work, schedule, deadline)
    result, = runner.assemble_cases(execution, [name], mode)
    if execution["status"] != "ok":
        return result | {"status": execution["status"]}
    if result["status"] == "ok":
        assert result["types"] == types, (name, mode, result)
        assert all(s["output_rows"] == s["input_rows"] == rows for s in result["samples"])
    return result


def summarized(records):
    if any(record["status"] != "ok" for record in records):
        return {"statuses": [record["status"] for record in records]}
    fields = ("execution_ns", "projection_ns", "increment_ns")
    process_medians = {key: [statistics.median(s[key] / s["input_rows"] for s in result["samples"])
                             for result in records] for key in fields}
    return {key: [statistics.median(values), min(values), max(values)]
            for key, values in process_medians.items()}


def measure(directories, output, protocol):
    assert not runner.git("status", "--porcelain", "--", str(HERE))
    edge_receipt = runner.read(output.parent / "edge-differential.json")
    assert edge_receipt["staging"] == protocol["staging_source"]
    assert edge_receipt["candidate"] == protocol["candidate_source"]
    assert edge_receipt["fixture_sha256"] == protocol["edge_fixture_sha256"]
    assert edge_receipt["test_sha256"] == protocol["edge_test_sha256"]
    assert edge_receipt["test_results"] == {"staging": 2, "candidate": 2}
    tests = HERE.parents[1] / "tests"
    assert runner.digest(tests / "xpath_batch_reuse.rs") == protocol["edge_test_sha256"]
    assert runner.digest(tests / "fixtures/xpath-batch-reuse/staging.json") == protocol["edge_fixture_sha256"]
    output.mkdir()
    binaries, receipts = {}, {}
    for arm, path in directories.items():
        receipt = runner.read(path / "receipt.json")
        source = protocol["staging_source" if arm == "staging" else "candidate_source"]
        binary = path / "functions"
        assert receipt["source"] == source and receipt["variant"] == "plain"
        assert runner.digest(binary) == receipt["sha256"]
        patch = runner.read(path / "patch.json")
        assert runner.digest(path / "patch.json") == receipt["patch_sha256"]
        rows = protocol["full_rows" if arm == "full" else "paired_rows"]
        assert list(patch) == ["benches/functions.rs"]
        assert f"const ROWS: usize = {rows};" in patch["benches/functions.rs"]
        binaries[arm], receipts[arm] = binary, receipt

    frozen = runner.read(FROZEN / "schedule.json")
    schedule = {key: frozen[key] for key in (
        "cpus", "address_space_bytes", "case_timeout_seconds", "preflight_process_timeout_seconds",
    )}
    schedule["process_timeout_seconds"] = protocol["per_process_seconds"]
    deadline = time.monotonic() + protocol["session_seconds"]
    ids = sorted(f["id"] for f in manifest()["functions"])
    eligible, excluded = select(binaries, ids, output / "preflight", schedule, deadline, protocol)
    write(output / "session.json", {"protocol_commit": runner.git("rev-parse", "HEAD"),
          "protocol": protocol, "schedule": schedule, "binaries": receipts,
          "edge_differential": edge_receipt,
          "eligible": eligible, "excluded": excluded})
    ids = sorted(eligible)
    records = {name: {mode: {arm: [] for arm in (*ARMS, "full")} for mode in MODES} for name in ids}
    for arm_group in (ARMS, ("full",)):
        rows = protocol["paired_rows"] if arm_group == ARMS else protocol["full_rows"]
        for replica in range(protocol["replicas"]):
            offset = replica * 3 % max(1, len(ids))
            order = -1 if replica == 1 else 1
            for name in ids[offset:] + ids[:offset]:
                for mode in MODES[::order]:
                    for arm in arm_group[::order]:
                        observations = records[name][mode][arm]
                        if observations and observations[-1]["status"] != "ok":
                            continue
                        work = output / str(replica) / name / mode / arm
                        print(replica, name, mode, arm, flush=True)
                        observations.append(samples(binaries[arm], name, mode, work, rows,
                                                    eligible[name][mode], schedule, deadline))
    write(output / "summary.json", {
        name: {mode: {arm: summarized(observations) for arm, observations in by_arm.items()}
               for mode, by_arm in modes.items()} for name, modes in records.items()
    })


def main():
    assert __debug__, "Do not disable validation"
    parser = argparse.ArgumentParser(description=__doc__)
    actions = parser.add_subparsers(dest="action", required=True)
    prepare = actions.add_parser("build")
    prepare.add_argument("arm", choices=(*ARMS, "full"))
    for name in ("repository", "target", "output"):
        prepare.add_argument(name, type=Path)
    collect = actions.add_parser("measure")
    for name in (*ARMS, "full", "output"):
        collect.add_argument(name, type=Path)
    actions.add_parser("check")
    args = parser.parse_args()
    protocol = runner.read(PROTOCOL)
    if args.action == "build":
        if args.arm != "staging":
            assert protocol.get("edge_fixture_sha256"), "freeze the staging oracle first"
        source = protocol["staging_source" if args.arm == "staging" else "candidate_source"]
        rows = protocol["full_rows" if args.arm == "full" else "paired_rows"]
        build(args.repository.resolve(), args.target.resolve(), args.output.resolve(), "plain",
              {"baseline_source": source, "reduced_rows": rows})
    elif args.action == "measure":
        measure({name: getattr(args, name).resolve() for name in (*ARMS, "full")},
                args.output.resolve(), protocol)
    else:
        manifest()
        fixture_source = (HERE.parent / "functions.rs").read_text()
        assert '"xml_s",\n                string_column(&|v| format!("<r><x>{}</x></r>", v % 31))' in fixture_source
        short = protocol["paired_rows"]
        assert {fixture_value(row) for row in range(short) if row % 16} == set(range(31))
        assert protocol["full_rows"] == runner.read(FROZEN / "schedule.json")["input_rows"]
        print("All 31 XML payloads present in paired prefix; frozen cases verified")


if __name__ == "__main__":
    main()
