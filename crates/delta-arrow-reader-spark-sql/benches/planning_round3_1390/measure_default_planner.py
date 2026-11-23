"""Split the default planner and metadata spans for the frozen wide fixture."""

import argparse
import gzip
import json
from pathlib import Path
import platform
from statistics import median
import subprocess
import sys

sys.dont_write_bytecode = True
from measure import (
    CANONICAL_BUILD_PROTOCOL, assemble, fixture, run, stage_samples,
    verify_compiler_inputs, wide_trace,
)

STAGING_HEAD = "b0de2e2b73258551c2088a8358c2d15b6365fb2f"
INVENTORY_BUILD_PROTOCOL = "837e6780c923a2a7a2bd542ab824c8349465d311"
OBSERVER_BUILD_PROTOCOL = "e115b11b95143ac0f6bdc44225eb08da6aa454fa"
PHYSICAL_RULES = (
    "OutputRequirements", "aggregate_statistics", "join_selection",
    "LimitedDistinctAggregation", "FilterPushdown", "EnforceDistribution",
    "CombinePartialFinalAggregate", "EnforceSorting", "OptimizeAggregateOrder",
    "WindowTopN", "ProjectionPushdown", "OutputRequirements", "LimitAggregation",
    "LimitPushPastWindows", "HashJoinBuffering", "LimitPushdown", "TopKRepartition",
    "ProjectionPushdown", "PushdownSort", "EnsureCooperative", "FilterPushdown(Post)",
    "SanityCheckPlan",
)


def planner_spans(events):
    expected = [f"default.rule/{rule}/{part}" for rule in PHYSICAL_RULES
                for part in ("body", "invariants")]
    actual = [(name, ns) for name, ns in events if name.startswith("default.rule/")]
    assert [name for name, _ in actual] == expected
    spans = {}
    for index, (name, ns) in enumerate(actual):
        spans[f"default.rule/{index // 2 + 1:02}/{name.removeprefix('default.rule/')}"] = ns
    for name, ns in events:
        if name.startswith(("default.", "metadata.")) and not name.startswith("default.rule/"):
            spans[name] = spans.get(name, 0) + ns
    spans["default.lowering_remainder"] = spans["default.lowering"] - sum(
        ns for name, ns in spans.items() if name.startswith("default.projection."))
    assert spans["default.lowering_remainder"] >= 0
    return spans


def read_processes(path):
    processes = {}
    with gzip.open(path, "rt") as stream:
        for line in stream:
            row = json.loads(line)
            samples = [planner_spans(events) for events in row["events"]]
            assert len(samples) == 9 and all(set(sample) == set(samples[0]) for sample in samples)
            medians = {name: median(sample[name] for sample in samples) for name in samples[0]}
            processes.setdefault(row["configuration"], []).append(medians)
    return processes


def summarize(processes):
    required = {"default.lowering", "default.optimization",
                "default.invariants.start", "default.invariants.finish"}
    child_spans = {"default.projection.expressions", "default.projection.build",
                   "metadata.alias_walk", "metadata.recompute_schema"}
    assert set(processes) == set(run.CONFIGURATIONS)
    medians = {}
    for mode, blocks in processes.items():
        assert len(blocks) == 5
        names = set(blocks[0])
        assert all(set(block) == names for block in blocks)
        assert required <= names
        if mode.startswith("spark-"):
            assert child_spans <= names
        medians[mode] = {name: median(block[name] for block in blocks)
                         for name in sorted(names) if name.startswith(("default.", "metadata."))}
    return medians


def main():
    from compare import load

    cli = argparse.ArgumentParser(description=__doc__)
    for argument in ("canonical", "observer", "inventory", "delta", "output"):
        cli.add_argument(argument, type=lambda path: Path(path).resolve())
    args = cli.parse_args()
    assert platform.system() == "Linux" and not sys.flags.optimize
    protocol = run.protocol_commit()
    canonical = load(args.canonical, "canonical", CANONICAL_BUILD_PROTOCOL)
    observer = load(args.observer, "planner-stages", OBSERVER_BUILD_PROTOCOL)
    recipes = [str(Path(__file__).with_name(name).relative_to(assemble.REPO))
               for name in ("build.py", "observe_default_planner.py")]
    subprocess.run(["git", "diff", "--exit-code", OBSERVER_BUILD_PROTOCOL, protocol, "--", *recipes],
                   cwd=assemble.REPO, check=True, capture_output=True)
    inventory = load(args.inventory, "inventory", INVENTORY_BUILD_PROTOCOL)
    for build in (canonical, observer, inventory):
        verify_compiler_inputs(build["source_commit"], STAGING_HEAD)
        assert build["toolchain"] == canonical["toolchain"]
    assert len({build["binary_sha256"] for build in (canonical, observer, inventory)}) == 3
    expected_delta = fixture(args.delta)
    args.output.mkdir()
    run.write(args.output / "builds.json", dict(protocol_commit=protocol, fixture=expected_delta,
              canonical=canonical, observer=observer, inventory=inventory))
    calls = wide_trace(canonical, observer, inventory, args.delta, args.output)
    stage_samples(observer, args.delta, args.output, calls)
    assert fixture(args.delta) == expected_delta
    raw_medians = read_processes(args.output / "wide-stages.jsonl.gz")
    run.write(args.output / "process-medians.json", raw_medians)
    medians = summarize(raw_medians)
    run.write(args.output / "summary.json", medians)
    native = medians["native-configured"]
    modes = (medians["spark-off"], medians["spark-on"])
    names = set(native).union(*modes)
    for name in sorted(names):
        values = [native.get(name, 0), *(mode.get(name, 0) for mode in modes)]
        print(name, *(f"{value / 1_000:.3f}" for value in values), "us")


if __name__ == "__main__":
    main()
