"""Replay the sealed planning comparison with alternating release builds."""

import argparse
import json
from pathlib import Path
import platform
from statistics import median
import subprocess
import sys
import time

sys.dont_write_bytecode = True
from build import verify_compiler_inputs
from measure import WIDE, archive, assemble, emit, fixture, paired, paired_samples, run, sample
from measure_default_planner import planner_spans


def load(directory, kind, protocol):
    receipt = run.read(directory / "receipt.json")
    binary = directory / "planning_attribution"
    assert receipt["kind"] == kind
    assert receipt["protocol_commit"] == protocol
    assert receipt["source_commit"] == receipt["assembly"]["baseline"]
    assert receipt["protocol_commit"] == receipt["assembly"]["protocol_commit"]
    assert assemble.digest(binary) == receipt["binary_sha256"]
    assert run.tree_hash(directory / "sealed-source") == receipt["assembly"]["tree_sha256"]
    return receipt | {"binary": str(binary)}


def verify_pair(builds):
    before, after = (builds[arm] for arm in paired.ARMS)
    assert before["toolchain"] == after["toolchain"]
    before_overlay, after_overlay = (build["assembly"]["assembly"] for build in (before, after))
    if before["kind"] == "planner-stages":
        # Both builds have identical hooks. The Sail wrapper can contain the trial edit.
        sail_planner = assemble.SAIL + "sail-plan/src/physical_plan/mod.rs"
        assert before["assembly"]["anchors"] == after["assembly"]["anchors"]
        assert before["assembly"]["registry_observer"] == after["assembly"]["registry_observer"]
        before_overlay = {name: value for name, value in before_overlay.items() if name != sail_planner}
        after_overlay = {name: value for name, value in after_overlay.items() if name != sail_planner}
    assert before_overlay == after_overlay
    assert before["binary_sha256"] != after["binary_sha256"]


def capture_wide_traces(builds, delta, output):
    traces = output / "stage-traces"
    traces.mkdir()
    for mode in run.CONFIGURATIONS:
        trace = output / "traces" / f"staging-{mode}.jsonl"
        expected, = [row for row in map(json.loads, trace.read_text().splitlines()) if row["id"] == WIDE]
        reference = traces / (mode + "-reference.jsonl")
        reference.write_text(json.dumps(expected) + "\n")
        for arm in paired.ARMS:
            label = f"trace-{mode}-{arm}"
            result = run.invoke(builds[arm]["binary"], ["trace", str(delta), mode, label, str(reference)], traces, label)
            assert len(result) == 1 and result[0]["id"] == WIDE
            assert not (traces / (label + ".stderr")).stat().st_size


def compare_wide_stages(builds, delta, output, spans):
    timing = output / "stage-timings"
    timing.mkdir()
    deadline = time.monotonic() + 600
    stats = []
    with archive(output, "stage-samples") as stream:
        for replica in range(5):
            modes = run.CONFIGURATIONS[replica % 4:] + run.CONFIGURATIONS[:replica % 4]
            arms = paired.ARMS if replica % 2 == 0 else paired.ARMS[::-1]
            for mode in modes:
                expected = sorted(name for name in spans if
                                  mode != "native-plain" or not name.startswith("physical.planner."))
                for arm in arms:
                    label = f"{replica + 1:02}-{mode}-{arm}"
                    row, = sample(mode, label, builds[arm]["binary"], delta, timing, deadline, 1)
                    assert row["id"] == WIDE
                    emit(stream, row | {"replica": replica + 1, "build": arm})
                    totals = []
                    for events in row["events"]:
                        if builds[arm]["kind"] == "planner-stages":
                            planner_spans(events)
                        selected = [(name, ns) for name, ns in events if name in spans]
                        assert sorted(name for name, _ in selected) == expected, (label, expected)
                        totals.append(sum(ns for _, ns in selected))
                    value = median(totals)
                    stats.append(dict(mode=mode, replica=replica + 1, build=arm, span_ns=value))
    run.write(output / "stage-summary.json", {"spans": spans, "process_medians": stats})


def main():
    cli = argparse.ArgumentParser(description=__doc__)
    for name in ("settings", "staging", "candidate", "delta", "output"):
        cli.add_argument(name, type=lambda value: Path(value).resolve())
    cli.add_argument("--stages", nargs=2, type=lambda value: Path(value).resolve())
    args = cli.parse_args()
    assert not sys.flags.optimize and platform.system() == "Linux"
    protocol = run.protocol_commit()
    settings_file = args.settings.relative_to(assemble.REPO)
    assert subprocess.check_output(["git", "show", f"{protocol}:{settings_file}"],
                                   cwd=assemble.REPO) == args.settings.read_bytes()
    settings = run.read(args.settings)
    spans = settings.get("observer_spans", [])
    assert bool(args.stages) == bool(spans)
    expected_delta = fixture(args.delta)
    builds = {arm: load(getattr(args, arm), "canonical", settings[arm]["canonical_protocol"])
              for arm in paired.ARMS}
    verify_pair(builds)
    verify_compiler_inputs(builds["staging"]["source_commit"], settings["staging_head"])
    verify_compiler_inputs(builds["candidate"]["source_commit"], protocol)
    stages = None
    if args.stages:
        kind = settings.get("observer_kind", "stages")
        stages = {arm: load(path, kind, settings[arm]["stages_protocol"])
                  for arm, path in zip(paired.ARMS, args.stages, strict=True)}
        verify_pair(stages)
        for arm in paired.ARMS:
            verify_compiler_inputs(stages[arm]["source_commit"], builds[arm]["source_commit"])
    recipe = str(Path(__file__).with_name("build.py").relative_to(assemble.REPO))
    for release in (builds, stages) if stages else (builds,):
        subprocess.run(["git", "diff", "--exit-code", release["candidate"]["protocol_commit"],
                        protocol, "--", recipe], cwd=assemble.REPO, check=True, capture_output=True)
    args.output.mkdir()
    run.write(args.output / "metadata.json", dict(protocol_commit=protocol, builds=builds,
              settings_sha256=assemble.digest(args.settings), delta_files=expected_delta))
    paired.capture_traces(builds, args.delta, args.output / "traces")
    if stages:
        capture_wide_traces(stages, args.delta, args.output)
    run.write(args.output / "paired-summary.json", paired_samples(builds, args.delta, args.output))
    if stages:
        run.write(args.output / "stages.json", stages)
        compare_wide_stages(stages, args.delta, args.output, spans)
    assert fixture(args.delta) == expected_delta


if __name__ == "__main__":
    main()
