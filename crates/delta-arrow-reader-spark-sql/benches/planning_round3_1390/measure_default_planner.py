"""Split the default planner and metadata spans for the frozen wide fixture."""

import argparse
from pathlib import Path
import platform
from statistics import median
import sys

sys.dont_write_bytecode = True
from compare import load
from measure import (
    CANONICAL_BUILD_PROTOCOL, assemble, fixture, run, stage_samples,
    verify_compiler_inputs, wide_trace,
)

STAGING_HEAD = "b0de2e2b73258551c2088a8358c2d15b6365fb2f"
INVENTORY_BUILD_PROTOCOL = "837e6780c923a2a7a2bd542ab824c8349465d311"


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
    cli = argparse.ArgumentParser(description=__doc__)
    for argument in ("canonical", "observer", "inventory", "delta", "output"):
        cli.add_argument(argument, type=lambda path: Path(path).resolve())
    args = cli.parse_args()
    assert platform.system() == "Linux" and not sys.flags.optimize
    protocol = run.protocol_commit()
    canonical = load(args.canonical, "canonical", CANONICAL_BUILD_PROTOCOL)
    observer = load(args.observer, "planner-stages", protocol)
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
    raw_medians = stage_samples(observer, args.delta, args.output, calls)
    assert fixture(args.delta) == expected_delta
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
