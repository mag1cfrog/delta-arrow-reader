"""Run the committed Apple Silicon baseline sessions and retain raw samples."""

import argparse
import json
from pathlib import Path
import sys
import time

sys.dont_write_bytecode = True
import cells
from build import digest
from native import BENCHES, HERE, REPO, archive, check_host, emit, legacy_runner, paired, safe_record, session

PROTOCOL = HERE / "protocol.json"
REPORTS = BENCHES / "reports"
ARMS = ("before", "after", "a", "b")


def source_receipt(binaries):
    receipt = cells.read(binaries / "receipt.json")
    assert receipt["profile"] == "release" and receipt["rustc"] == "1.96.0"
    assert receipt["recipe_sha256"] == digest(HERE / "build.py")
    for name, expected in receipt["binaries"].items():
        assert digest(binaries / name) == expected, f"release executable changed: {name}"
    return receipt


def roots(args):
    paths = ((args.output, "EVIDENCE"), (args.binaries, "RELEASE"), (args.candidate, "CANDIDATE"),
             (args.fixture, "FIXTURE"), (REPO, "REPOSITORY"), (Path.home(), "HOME_ROOT"))
    return tuple((path, label) for path, label in paths if path is not None)


def resources(args):
    available = {}
    if args.fixture:
        proof = cells.read(args.fixture / "fixture-proof.json")
        assert proof["snapshot_matches"] or proof.get("content_matches"), "SF1 equality is required before use"
        snapshot = args.fixture / "delta-snapshot"
        for row in proof["checks"]["files"]:
            assert digest(snapshot / row["path"]) == row["macos"]
        derived_log = snapshot / "_delta_log/00000000000000000000.json"
        assert digest(derived_log) == proof["checks"]["derived_delta_log_sha256"]["macos"]
        available["snapshot"] = snapshot
    if args.needs_planning:
        path = args.output / "planning-delta"
        cells.prepare_planning(args.binaries, path)
        available["planning_delta"] = path
    if args.nullif_eligible:
        available["nullif_eligible"] = args.nullif_eligible
    if args.range_eligible:
        available["range_eligible"] = args.range_eligible
    if args.stock_cli:
        receipt = cells.read(args.stock_cli / "receipt.json")
        binary = args.stock_cli / "dar"
        assert digest(binary) == receipt["binary_sha256"]
        available["stock_cli"] = binary
    return available


def trial(cases, args, settings, counts, stream, data, phase, *, aa_only=False):
    blocks = settings["blocks"]
    assert blocks >= 9
    deadline = time.monotonic() + settings["cell_session_seconds"]
    arm_names = ("a", "b") if aa_only else ARMS
    metrics = {case["label"]: {arm: [] for arm in arm_names} for case in cases}
    failures = {case["label"]: [] for case in cases}
    assert len(cases) == len(metrics)
    bounds = roots(args)
    for block in range(blocks):
        order = 1 if block % 2 == 0 else -1
        for index, case in enumerate(cases[::order]):
            label = case["label"]
            if failures[label]:
                continue
            samples = counts[case["suite"]]
            for arm in arm_names[::order]:
                reference = {**case, "configuration": case.get("reference", case["configuration"]),
                             "id": case.get("reference_id", case["id"])}
                controls = bool(case.get("reference_id")) and arm in ("a", "b") and not aa_only
                participants = (("before", reference), ("after", case)) if controls else (
                    (arm, case if (arm == "after" or aa_only) else reference),)
                values = {}
                work = args.output / phase / f"{block + 1:02}" / f"{index:03}" / arm
                for side, picked in participants[::order]:
                    binaries = args.candidate if arm == "after" and args.candidate else args.binaries
                    check_host()
                    status, rows = "ok", []
                    try:
                        status, rows = cells.process(binaries, picked, samples, work / side, data, deadline, block + 1)
                        if status == "ok":
                            values[side] = cells.metric(picked, rows, samples)
                    except (AssertionError, KeyError, RuntimeError, ValueError, TimeoutError) as error:
                        status = f"{type(error).__name__}: {error}"
                    if status != "ok":
                        failures[label].append(f"block {block + 1} {arm} {side}: {status}")
                    emit(stream, {"kind": "process", "phase": phase, "label": label, "block": block + 1,
                                  "arm": arm, "side": side, "cell": picked, "samples_per_process": samples,
                                  "status": status, "records": rows}, bounds)
                if len(values) == len(participants):
                    ns = values["after"] - values["before"] if controls else values[arm]
                    metrics[label][arm].append(ns)
    summary = {}
    for label, series in metrics.items():
        if failures[label]:
            summary[label] = {"status": "untimable", "reason": failures[label]}
        elif aa_only:
            arms = {**series, "before": series["a"], "after": series["b"]}
            first_three = {name: values[:3] for name, values in arms.items()}
            full = paired(arms, blocks)
            cost = abs((full["before"] + full["after"]) / 2)
            summary[label] = {"status": "ok", **full, "first_three_blocks": paired(first_three, 3),
                              "floor_fraction": full["floor"] / cost if cost else None}
        else:
            summary[label] = {"status": "ok", **paired(series, blocks)}
    return safe_record(summary, bounds)


def noise(args, settings, stream):
    assert not args.candidate
    data = resources(args)
    cases = settings["noise_cells"]
    counts = dict(settings["initial_samples"])
    eligibility = cells.read(args.eligibility)
    assert eligibility["binary_sha256"] == digest(args.binaries / "functions")
    assert eligibility["manifest_sha256"] == digest(cells.FUNCTIONS)
    functions = {row["id"]: row for row in eligibility["results"]}
    exclusions = {}
    for case in cases:
        if case["suite"] == "functions":
            row = functions[case["id"]]
            if row["status"] not in ("eligible", "same_verified"):
                exclusions[case["label"]] = row["reason"]
        elif args.cell_eligibility:
            excluded = cells.read(args.cell_eligibility)["excluded"]
            family = "planning" if case["suite"] == "planning_attribution" else case["suite"]
            cell_id = f"{family}/{case['id']}/{case['configuration']}"
            if cell_id in excluded:
                exclusions[case["label"]] = excluded[cell_id]
    selected = [case for case in cases if case["label"] not in exclusions]
    first = trial(selected, args, settings, counts, stream, data, "noise-03", aa_only=True)
    first.update({label: {"status": "untimable", "reason": reason} for label, reason in exclusions.items()})
    raised = {case["suite"] for case in cases if (row := first[case["label"]])["status"] == "ok"
              and (row["floor_fraction"] is None or row["floor_fraction"] > 0.03)}
    counts.update({suite: settings["maximum_samples"] for suite in raised})
    selected = [case for case in cases if case["suite"] in raised and first[case["label"]]["status"] == "ok"]
    rerun = trial(selected, args, settings, counts, stream, data, "noise-09", aa_only=True) if selected else {}
    return {"first": first, "rerun": rerun, "samples": counts, "blocks": settings["blocks"]}


def preflight_functions(args, settings, *, noise_only=False):
    args.output.mkdir()
    deadline = time.monotonic() + settings["function_preflight_seconds"]
    schedule = cells.read(BENCHES / "functions/schedule.json")
    cases = cells.read(cells.FUNCTIONS)["functions"]
    assert len(cases) == 633
    if noise_only:
        ids = {row["id"] for row in settings["noise_cells"] if row["suite"] == "functions"}
        cases = [case for case in cases if case["id"] in ids]
        assert len(cases) == len(ids)
    binary = args.binaries / "functions"
    with legacy_runner("functions/run.py") as runner:
        runner["self_check"]()
        registry = runner["invoke"](binary, ["registry"], args.output / "registry", schedule, deadline, True)
        fixture = runner["invoke"](binary, ["fixture"], args.output / "fixture", schedule, deadline, True)
        assert registry["status"] == fixture["status"] == "ok"
        assert registry["records"] == [cells.read(BENCHES / "functions/native-registry.json")], "registry drift"
        check, = fixture["records"]
        assert check["rows"] == 1_000_000
        assert all(c["nulls"] == (0 if c["name"] in ("id", "grp", "high_key") else 62500) for c in check["columns"])
        eligibility = []
        for index, case in enumerate(cases, 1):
            result = cells.function_preflight(binary, case, args.output / f"{index:03}", deadline, schedule, runner)
            eligibility.append(safe_record(result, roots(args)))
            if index % 25 == 0 or index == len(cases):
                print(f"Function preflight {index}/{len(cases)}", flush=True)
    cells.write(args.output / "eligibility.json", {"binary_sha256": digest(binary),
                "manifest_sha256": digest(cells.FUNCTIONS), "fixture": check, "results": eligibility})


def sharded(args, settings, stream, *, repeat):
    schedule = cells.read(BENCHES / "functions/schedule.json")
    preflight = cells.read(args.eligibility)
    binary = args.binaries / "functions"
    assert preflight["binary_sha256"] == digest(binary)
    assert preflight["manifest_sha256"] == digest(cells.FUNCTIONS)
    work = {row["id"]: row for row in preflight["results"] if row["status"] == "eligible"}
    pairs = set(work) if not repeat else {
        key.rsplit("/", 1)[0] for key, row in cells.read(args.first_sweep).items()
        if row["status"] == "ok" and row["decision"] == "slower"
    }
    ids = sorted(work.keys() & pairs)
    count = cells.read(args.frozen_noise)["samples"]["functions"] if repeat else schedule["samples_per_case"]
    deadline = time.monotonic() + settings["function_sweep_seconds"]
    metrics = {case: {} for case in ids}
    failure = {}
    bounds = roots(args)
    blocks = settings["blocks"]
    assert blocks >= 9 and args.candidate is None
    with legacy_runner("functions/run.py") as runner:
        for block in range(blocks):
            main = [(config, config) for config in schedule["main_orders"][block % 3]]
            aa = [(arm, "native") for arm in schedule["noise_orders"][block % 3]]
            groups = runner["shards"](ids, block % 3, schedule["shard_size"])
            if block % 2:
                groups = [shard[::-1] for shard in groups[::-1]]
            for shard, group in enumerate(groups):
                for arm, config in (aa + main if block % 2 == 0 else main + aa):
                    selected = [case for case in group if case not in failure and
                                (arm != "native" or work[case]["class"] == "paired")]
                    if not selected:
                        continue
                    check_host()
                    path = args.output / f"{block + 1:02}-{shard + 1:03}-{arm}"
                    result = cells.function_set(binary, selected, config, count, path, deadline, schedule, runner)
                    emit(stream, {"kind": "process", "block": block + 1, "arm": arm, "ids": selected,
                                  "sample_count": count, **result}, bounds)
                    for case in selected:
                        cell = {"suite": "functions", "configuration": config, "id": case}
                        try:
                            records = result["records"]
                            assert any(row.get("status") == "ok" and row.get("id") == case for row in records)
                            ns = cells.metric(cell, records, count)
                        except (AssertionError, KeyError, ValueError):
                            row = next((r for r in result["records"] if r.get("id") == case and "status" in r), {})
                            failure[case] = row.get("error") or result.get("error") or f"{result['status']}: incomplete or invalid timed samples"
                        else:
                            metrics[case].setdefault(arm, []).append(ns)
            print(f"Function {'repeat' if repeat else 'sweep'} block {block + 1}/{blocks}", flush=True)
    outcome = {}
    for case in ids:
        series = metrics[case]
        for config in ("spark-off", "spark-on"):
            key = f"{case}/{config}"
            if case in failure:
                outcome[key] = {"status": "untimable", "reason": failure[case]}
            elif work[case]["class"] == "paired":
                arms = {"before": series["native"], "after": series[config], "a": series["a"], "b": series["b"]}
                outcome[key] = {"status": "ok", **paired(arms, blocks)}
            else:
                arms = {"before": series["a"], "after": series[config], "a": series["a"], "b": series["b"]}
                row = paired(arms, blocks)
                outcome[key] = {"status": "spark-only", "after": row["after"], "floor": row["floor"]}
    return safe_record(outcome, bounds)


def main():
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("operation", choices=("preflight-noise", "preflight-functions", "noise", "sweep", "repeat", "cells"))
    for name in ("binaries", "output"):
        cli.add_argument(name, type=lambda path: Path(path).resolve())
    for name in ("fixture", "nullif-eligible", "eligibility", "cell-eligibility", "frozen-noise", "first-sweep",
                 "candidate", "manifest", "range-eligible", "stock-cli"):
        cli.add_argument(f"--{name}", type=lambda path: Path(path).resolve())
    args = cli.parse_args()
    settings = cells.read(PROTOCOL)
    assert not args.output.is_relative_to(REPO)
    receipt = source_receipt(args.binaries)
    assert receipt["source"] == settings["staging_source"]
    assert not args.output.exists()
    spec = cells.read(args.manifest)["cells"] if args.manifest else settings["noise_cells"]
    args.needs_planning = any(row["suite"].startswith("planning") for row in spec)
    if args.operation.startswith("preflight-"):
        preflight_functions(args, settings, noise_only=args.operation == "preflight-noise")
        return
    with session(PROTOCOL) as binding:
        if args.manifest:
            from build import git
            assert git(REPO, "show", f"HEAD:{args.manifest.relative_to(REPO)}") == args.manifest.read_text().strip()
        args.output.mkdir()
        with archive(args.output / "samples.jsonl.gz") as stream:
            uses = {key: digest(path) for key in ("eligibility", "frozen_noise", "first_sweep", "cell_eligibility", "nullif_eligible")
                    if (path := getattr(args, key)) is not None}
            emit(stream, {"kind": "session", **binding, "operation": args.operation, "build": receipt,
                          "settings_sha256": digest(PROTOCOL), "manifest_sha256": digest(args.manifest) if args.manifest else None,
                          "receipts": uses,
                          "candidate": source_receipt(args.candidate) if args.candidate else None})
            if args.operation == "noise":
                result = noise(args, settings, stream)
            elif args.operation in ("sweep", "repeat"):
                result = sharded(args, settings, stream, repeat=args.operation == "repeat")
            else:
                counts = cells.read(args.frozen_noise)["samples"]
                proof = cells.read(args.cell_eligibility)
                assert proof["binaries"] == receipt["binaries"] and proof["manifest_sha256"] == digest(args.manifest)
                assert proof["fixture_proof_sha256"] == digest(args.fixture / "fixture-proof.json")
                excluded = proof["excluded"]
                selected = [item for item in spec if item["label"] not in excluded]
                result = trial(selected, args, settings, counts, stream, resources(args), "cells")
                result.update({label: {"status": "untimable", "reason": reason} for label, reason in excluded.items()})
        cells.write(args.output / "summary.json", result)


if __name__ == "__main__":
    main()
