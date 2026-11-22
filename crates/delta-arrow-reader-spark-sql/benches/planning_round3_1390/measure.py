"""Measure the fixed planning workload; observe wide-query stages and rule visits."""
import argparse
import gzip
import json
from pathlib import Path
from statistics import median
import subprocess
import time

from build import assemble, run

import paired

HERE = Path(__file__).resolve().parent
WIDE = "stress/wide_400"
PHASES = ("logical_ns", "physical_ns", "total_ns")


def load_build(directory, kind, protocol):
    receipt = run.read(directory / "receipt.json")
    binary = directory / "planning_attribution"
    assert receipt["kind"] == kind and receipt["protocol_commit"] == protocol
    assert assemble.digest(binary) == receipt["binary_sha256"]
    sealed = directory / "sealed-source"
    assert run.tree_hash(sealed) == receipt["assembly"]["tree_sha256"]
    return receipt | {"binary": str(binary)}


def fixture(root):
    expected = run.read(assemble.WORKSPACE / "benches/reports/planning-1379-metadata.json")["delta_files"]
    assert run.delta_hashes(root) == expected
    return expected


def emit(stream, row):
    stream.write(json.dumps(row, separators=(",", ":"), sort_keys=True) + "\n")
    stream.flush()


def archive(destination, name):
    return gzip.open(destination / (name + ".jsonl.gz"), "xt")


def sample(configuration, label, binary, delta, output, deadline, case_count):
    assert time.monotonic() < deadline, "planning schedule expired"
    result = run.invoke(binary, ["measure", str(delta), configuration, label, "-"], output, label)
    assert time.monotonic() <= deadline, "planning schedule expired"
    assert len(result) == case_count and len({row["id"] for row in result}) == case_count
    assert not (output / (label + ".stderr")).stat().st_size
    for row in result:
        assert row["configuration"] == configuration and row["process"] == label
        logical, physical, total = [row[phase] for phase in PHASES]
        assert len(logical) == len(physical) == len(total) == len(row["events"]) == 9
        assert [a + b for a, b in zip(logical, physical, strict=True)] == total
    return result


def wide_trace(canonical, instrumented, inventory, delta, output):
    trace_root = output / "traces"
    trace_root.mkdir()
    calls = []
    for mode in run.CONFIGURATIONS:
        rows = run.invoke(canonical["binary"], ["trace", str(delta), mode, mode, "-"], trace_root, mode)
        assert len(rows) == (309 if mode.startswith("spark-") else 30)
        assert len({row["id"] for row in rows}) == len(rows)
        assert not (trace_root / (mode + ".stderr")).stat().st_size
        wide, = [row for row in rows if row["id"] == WIDE]
        reference = trace_root / (mode + "-wide-reference.jsonl")
        reference.write_text(json.dumps(wide) + "\n")
        for build in (instrumented, inventory):
            label = mode + "-" + build["kind"]
            traces = run.invoke(build["binary"], ["trace", str(delta), mode, label, str(reference)],
                                trace_root, label)
            assert len(traces) == 1 and traces[0]["id"] == WIDE
            diagnostic = (trace_root / (label + ".stderr")).read_text()
            if build["kind"] == "stages":
                assert not diagnostic
                continue
            for line in diagnostic.splitlines():
                tag, stage, index, rule, projections, expressions, roots, children = line.split("\t")
                assert tag == "DAR_WIDE" and stage in ("analyzer", "optimizer")
                calls.append(dict(configuration=mode, stage=stage, rule=rule, index=int(index),
                                  input_projections=int(projections), input_expressions=int(expressions),
                                  root_visits=int(roots), child_visits=int(children)))
    assert {row["configuration"] for row in calls} == set(run.CONFIGURATIONS)
    run.write(output / "rule-visits.json", calls)


def stage_samples(instrumented, delta, output):
    path = output / "timings-stages"
    path.mkdir()
    deadline = time.monotonic() + 600
    process_medians = {}
    with archive(output, "wide-stages") as stream:
        for replica in range(5):
            modes = run.CONFIGURATIONS[replica % 4:] + run.CONFIGURATIONS[:replica % 4]
            for mode in modes:
                label = f"{replica+1:02}-{mode}"
                row, = sample(mode, label, instrumented["binary"], delta, path, deadline, 1)
                assert row["id"] == WIDE
                emit(stream, row | {"replica": replica + 1})
                # Sum repeated invocations of a span within one plan, then take the median.
                totals = []
                for captured in row["events"]:
                    spans = {}
                    for name, ns in captured:
                        spans[name] = spans.get(name, 0) + ns
                    assert all(name in spans for name in ("physical.analyzer", "physical.optimizer"))
                    if mode != "native-plain":
                        assert "physical.planner" in spans
                    totals.append(spans)
                assert all(set(s) == set(totals[0]) for s in totals)
                medians = {name: median(s[name] for s in totals) for name in totals[0]}
                medians.update({name: median(row[name]) for name in PHASES})
                process_medians.setdefault(mode, []).append(medians)
    return process_medians


def paired_samples(builds, delta, output):
    path = output / "timings-paired"
    path.mkdir()
    deadline = time.monotonic() + 600
    manifest = run.read(assemble.WORKSPACE / "benches/planning/queries.json")
    ids = {row["id"] for row in manifest["queries"] if row["group"] in ("paired", "stress")}
    executions = list(paired.process_order())
    assert len(ids) == 30 and len(executions) == 50
    records = []
    with archive(output, "paired-samples") as stream:
        for item, arm in executions:
            label = item["process"] + "-" + arm
            rows = sample(item["configuration"], label, builds[arm]["binary"], delta, path, deadline, 30)
            assert {row["id"] for row in rows} == ids and all(row["events"] == [[]] * 9 for row in rows)
            for row in rows:
                record = item | row | {"process": label, "build": arm}
                records.append(record)
                emit(stream, record)
            print(label, flush=True)
    assert len(records) == 1_500
    return paired.aggregate(records, ids)


def main(canonical_path, stage_path, inventory_path, delta, output):
    protocol = run.protocol_commit()
    canonical = load_build(canonical_path, "canonical", protocol)
    instrumented = load_build(stage_path, "stages", protocol)
    inventory = load_build(inventory_path, "inventory", protocol)
    assert len({b["source_commit"] for b in (canonical, instrumented, inventory)}) == 1
    assert len({b["binary_sha256"] for b in (canonical, instrumented, inventory)}) == 3
    hashes = fixture(delta)
    output.mkdir()
    run.write(output / "builds.json", [canonical, instrumented, inventory])
    wide_trace(canonical, instrumented, inventory, delta, output)
    stages = stage_samples(instrumented, delta, output)
    rows = paired_samples({"staging": canonical, "candidate": canonical}, delta, output)
    assert fixture(delta) == hashes
    run.write(output / "summary.json", {"process_stage_medians_ns": stages, "paired": rows})


if __name__ == "__main__":
    cli = argparse.ArgumentParser(description=__doc__)
    for argument in ("canonical_path", "stage_path", "inventory_path", "delta", "output"):
        cli.add_argument(argument, type=lambda value: Path(value).resolve())
    main(**vars(cli.parse_args()))
