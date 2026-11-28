"""Verify full inputs and time a paired #1516 change on the restricted cases."""

import argparse
from collections import defaultdict
from pathlib import Path
import sys
import time
from types import SimpleNamespace

sys.dont_write_bytecode = True
import restricted_1516 as frozen

FULL = frozen.BENCHES / "functions/manifest.json"
ARMS = ("before", "after", "a", "b")


def release_receipts(args, protocol):
    return {
        role: frozen.verify_release(SimpleNamespace(release=path), {"source": protocol[role]})
        for role, path in (("staging", args.staging), ("candidate", args.candidate))
    }


def verify_results(binary, manifest, case, scope, path, deadline, protocol):
    work = path / scope / case
    status, records = frozen.call(
        binary, ["preflight", manifest, case, work / "plans"], work,
        frozen.bounded_timeout(deadline, protocol["preflight_process_seconds"]),
    )
    if status != "ok" or len(records) != 1:
        return {"reason": f"preflight {status}"}
    record, = records
    input_rows = record["input_rows"]
    output_rows = 1 if case.startswith("aggregate.") else input_rows
    if scope == "full" and input_rows != 1_000_000:
        return {"reason": "full fixture row count differs"}
    modes = {row["configuration"]: row for row in record["results"]}
    eligible = (
        tuple(modes) == frozen.MODES
        and 0 < input_rows <= 1_000_000
        and len(record["baselines"]) == 3
        and all(row["rows"] == input_rows for row in record["baselines"])
        and all(row["status"] == "ok" and row["rows"] == output_rows
                and row["source_guard_rows"] == input_rows for row in modes.values())
    )
    if not eligible:
        return {"reason": "failed mode or input/output row guard", "results": record["results"]}
    normalized = {}
    for mode, row in modes.items():
        if scope == "restricted" and mode != "native" and not row.get("comparison", {}).get("equal"):
            return {"reason": "restricted native values or types differ", "results": record["results"]}
        normalized[mode] = {
            "types": row["types"],
            "rows_sha256": frozen.mac_build.digest(work / "plans" / f"{mode}.rows.json"),
            "plan_sha256": frozen.mac_build.digest(work / "plans" / f"{mode}.plan"),
        }
    if scope == "restricted" and any(row["types"] != modes["native"]["types"] for row in modes.values()):
        return {"reason": "restricted executed types differ", "results": record["results"]}
    return {"input_rows": input_rows, "output_rows": output_rows, "modes": normalized}


def preflight(args, protocol, releases):
    args.output.mkdir()
    deadline = time.monotonic() + protocol["preflight_seconds"]
    observations = {}
    excluded = {}
    scopes = (("full", FULL), ("restricted", frozen.MANIFEST))
    roles = (("staging", args.staging), ("candidate", args.candidate))
    for case in protocol["case_ids"]:
        detail = {}
        for scope, manifest in scopes:
            outcomes = {role: verify_results(path / "functions", manifest, case, scope,
                                             args.output / role, deadline, protocol)
                        for role, path in roles}
            detail[scope] = outcomes
            if any("reason" in row for row in outcomes.values()):
                excluded[case] = f"{scope}: preflight failed"
                continue
            left, right = (outcomes[key] for key in ("staging", "candidate"))
            stable = (
                left["input_rows"] == right["input_rows"] and left["output_rows"] == right["output_rows"]
                and all(left["modes"][mode][key] == right["modes"][mode][key]
                        for mode in frozen.MODES for key in ("types", "rows_sha256"))
            )
            if not stable:
                excluded[case] = f"{scope}: cross-release values or executed types differ"
        observations[case] = detail
    proof = {"protocol_sha256": frozen.mac_build.digest(args.protocol), "releases": releases,
             "observations": observations, "excluded": excluded}
    frozen.cells.write(args.output / "proof.json", frozen.safe_record(proof, roots(args)))


def roots(args):
    return ((args.output, "EVIDENCE"), (args.staging, "STAGING"), (args.candidate, "CANDIDATE"),
            (frozen.REPO, "REPOSITORY"), (Path.home(), "HOME_ROOT"))


def measure(args, protocol, releases):
    proof = frozen.cells.read(args.proof)
    assert proof["protocol_sha256"] == frozen.mac_build.digest(args.protocol) and proof["releases"] == releases
    cases = [case for case in protocol["case_ids"] if case not in proof["excluded"]]
    assert cases
    blocks, samples = protocol["blocks"], protocol["samples"]
    tracks = defaultdict(lambda: defaultdict(list))
    failures = {}
    with frozen.session(args.protocol) as binding:
        args.output.mkdir()
        deadline = time.monotonic() + protocol["max_seconds"]
        with frozen.archive(args.output / "samples.jsonl.gz") as stream:
            frozen.emit(stream, {"kind": "session", **binding, "releases": releases,
                                 "protocol_sha256": frozen.mac_build.digest(args.protocol),
                                 "proof_sha256": frozen.mac_build.digest(args.proof),
                                 "excluded": proof["excluded"]})
            for block in range(1, blocks + 1):
                order = 1 if block % 2 else -1
                for case in cases[::order]:
                    observed = proof["observations"][case]["restricted"]["staging"]
                    for mode in frozen.MODES[::order]:
                        key = f"{case}/{mode}"
                        if key in failures:
                            continue
                        expected = {"input_rows": observed["input_rows"], "output_rows": observed["output_rows"],
                                    "types": observed["modes"][mode]["types"]}
                        for arm in ARMS[::order]:
                            binary = (args.candidate if arm == "after" else args.staging) / "functions"
                            path = args.output / f"{block:02}" / case / mode / arm
                            frozen.check_host()
                            try:
                                status, value, records = frozen.process(
                                    binary, case, mode, path, expected, samples, deadline, protocol["process_seconds"],
                                )
                            except TimeoutError:
                                status, value, records = "schedule_timeout", None, []
                            frozen.emit(stream, {"kind": "process", "block": block, "case": case,
                                                 "configuration": mode, "arm": arm, "status": status,
                                                 "records": records}, roots(args))
                            if status != "ok":
                                failures[key] = {"status": "untimable", "reason": status, "block": block, "arm": arm}
                                break
                            tracks[key][arm].append(value)
                print(f"paired restricted functions: block {block}/{blocks}", flush=True)
    result = {f"{case}/{mode}": (failures[f"{case}/{mode}"] if f"{case}/{mode}" in failures else {
        "status": "ok", **frozen.paired(tracks[f"{case}/{mode}"], blocks),
    }) for case in cases for mode in frozen.MODES}
    frozen.cells.write(args.output / "comparisons.json", result)


def main():
    cli = argparse.ArgumentParser(description=__doc__)
    sub = cli.add_subparsers(dest="action", required=True)
    for action, names in (("preflight", ("staging", "candidate")), ("measure", ("staging", "candidate", "proof"))):
        parser = sub.add_parser(action)
        for name in ("protocol", *names, "output"):
            parser.add_argument(name, type=lambda value: Path(value).resolve())
    args = cli.parse_args()
    assert not args.output.is_relative_to(frozen.REPO)
    assert frozen.mac_build.git(frozen.REPO, "show", f"HEAD:{args.protocol.relative_to(frozen.REPO)}") == args.protocol.read_text().strip()
    protocol = frozen.cells.read(args.protocol)
    assert protocol["restricted_manifest_sha256"] == frozen.mac_build.digest(frozen.MANIFEST)
    assert protocol["full_manifest_sha256"] == frozen.mac_build.digest(FULL)
    assert protocol["schedule_sha256"] == frozen.mac_build.digest(frozen.SCHEDULE)
    assert protocol["blocks"] == 9 and protocol["samples"] == frozen.cells.read(frozen.SCHEDULE)["samples"]["functions"]
    assert len(set(protocol["case_ids"])) == len(protocol["case_ids"])
    releases = release_receipts(args, protocol)
    assert releases["staging"]["overlays"] == releases["candidate"]["overlays"]
    (preflight if args.action == "preflight" else measure)(args, protocol, releases)


if __name__ == "__main__":
    main()
