# NULLIF computed-right selection trial (#620)

[Stage attribution](nullif-620-residual-attribution.md) puts most
computed-right overhead in filtering and scattering. Compare one
candidate that evaluates a Float64 column plus a Float64 literal on
the original batch. Its typed check permits only that infallible
addition and the existing kernel opt-in. Computed casts, callbacks,
checked integer arithmetic and other expressions retain selection.

## Paired run

Use the same Rust 1.96.0 release settings and committed inputs as the
[rebase run](nullif-620-rebase-protocol.md). The
[manifest](../nullif_620_round3/computed-right-selection.json) pins
both binary sources and the staging head. Staging differs from the
baseline build only in the report and evidence files. The candidate's
offset assertion in `a2df1f2e` is in the test module. The release
benchmark compiles the runtime from `50f83cbe`.

Run the full preflight for both configurations in all three modes. Require exact
native and Spark values and executed types on the frozen inputs,
including checked arithmetic. The full workspace tests and candidate
differentials check failures, row selection and operand effects.
A mismatching function is excluded from timing and reported.

Run 20 four-process blocks for each family. Alternate both the
baseline/candidate order and which family starts a block. Each
NULLIF process covers the 13 manifest cells. Each checked-arithmetic
process covers all 36 inputs and both phases across the three modes.
Run on CPUs 2-5 with one warmup and three retained samples per cell.
Use a minimal process environment with UTC. Preserve all raw samples.
Bound preflight to 25 minutes, timing to 35 minutes and each process
to 180 seconds. Run timing after local builds and tests finish.

## Decision

For each sparse computed-right batch size in both ANSI modes, require
a paired execution gain above repeat noise with at least 16 of the
20 blocks faster. For every other timed Spark NULLIF execution cell,
a paired slowdown above repeat noise in at least 16 blocks fails the
check. Use the existing definitions of the paired median and repeat
noise.

Apply the one-sided Holm test at 0.025 to all measured native NULLIF
phase controls and all checked-arithmetic phase controls. A significant
paired increase above repeat noise fails the candidate. Twenty blocks
allow a detectable increase under the group correction. List positive
paired deltas separately from failed controls. A timeout, missing
observation, failed correctness check or failed timing comparison
rejects the candidate. Report the rejected trial if any of these occur.

Use the existing recheck controller, then apply the decision as
shown below. The two build receipts carry the exact source, binary
and build identities, with empty `environment` objects.

```python
import json
from pathlib import Path
import runpy
import sys

manifest, output, baseline_receipt, candidate_receipt = map(Path, sys.argv[1:])
old_controller = runpy.run_path("benches/nullif_620_recheck/run.py")
controller = old_controller["measure"].__globals__
controller["MANIFEST"] = settings = json.loads(manifest.read_text())
binaries, identities = controller["load_builds"](
    (baseline_receipt, candidate_receipt)
)
controller["preflight"](output / "preflight", binaries, identities)
controller["measure"](
    output / "timing", binaries, identities, output / "preflight/receipt.json"
)
controller["summarize"](output / "timing")
metrics = json.loads((output / "timing/summary.json").read_text())["all_metrics"]
failures = []
gain_cases = set(settings["required_gains"])
targets = 0
for item in metrics:
    cell, mode, phase = item["key"]
    if not cell.startswith("nullif-") or mode == "native" or phase != "execution_ns":
        continue
    if cell in gain_cases:
        targets += 1
        if item["delta"] >= -item["noise"] or sum(
            delta < 0 for delta in item["pair_deltas"]
        ) < 16:
            failures.append([cell, mode, "gain"])
    elif item["delta"] > item["noise"] and sum(
        delta > 0 for delta in item["pair_deltas"]
    ) >= 16:
        failures.append([cell, mode, "regression"])
assert targets == 4

controls = sorted(
    (item for item in metrics
     if item["key"][0].startswith("checked-") or item["key"][1] == "native"),
    key=lambda item: item["probability"],
)
assert len(controls) == 242
for index, item in enumerate(controls):
    if item["probability"] > 0.025 / (len(controls) - index):
        break
    if item["delta"] > item["noise"]:
        failures.append(item["key"])

(output / "decision.json").write_text(json.dumps({
    "pass": not failures, "failed_comparisons": failures, "controls": len(controls),
}, indent=2) + "\n")
if failures:
    sys.exit("Computed-right candidate did not pass all timing checks")
```

Commit the compressed samples and report in the same PR. The
preflight and the two schedules pin all comparisons to this session.
