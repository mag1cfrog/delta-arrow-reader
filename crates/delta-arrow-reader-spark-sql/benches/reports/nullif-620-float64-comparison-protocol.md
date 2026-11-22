# Float64 NULLIF comparison trial (#620)

The [attribution](nullif-620-residual-attribution.md) measures Float64
normalization, Arrow equality and output validity separately. This trial
checks one change for those regions. It builds the output bitmap from
Float64 values and input validity, retaining the original left value
buffer. Scalar, mixed-type and non-Float64 comparisons use the original
path. The candidate changes Sail only.

## Paired run

Use the pinned Rust 1.96.0 release binaries from the
[manifest](../nullif_620_round3/float64-comparison.json). The baseline
source matches the product code in #1517. Run the full existing
correctness preflight across both binaries in all three modes, plus the
candidate's workspace tests and frozen differentials. Exclude the
candidate from timings if any required value, type or failure changes.
Record exact build receipts using empty `environment` objects.

In one session, run all 72 NULLIF cells across 12 four-process blocks
with the existing recheck controller. Alternate staging, candidate,
candidate, staging and the reverse. Use the same 262,144 rows, CPUs
2-5, three configurations and one warmup plus three retained samples.
Separate planning from execution. Limit each process to 180 seconds,
the preflight to 25 minutes and timing to 20 minutes. Keep the local
machine free from other builds and timings during this session.

## Decision

Require the sparse two-column execution gain with 8,192-row batches
in both ANSI modes. Each median paired reduction must exceed repeat
noise and at least 10 of the 12 blocks must improve. The three native
controls use the rebase protocol's Holm and noise rule. Apply a Spark
regression guard to the listed protected execution cells in both modes.
A median paired increase above repeat noise with at least 10 slower
blocks fails that guard. Report the other cells and related uncertainty.
A timeout, missing observation, failed correctness preflight, failed
native gate, failed gain or failed guard rejects the candidate. Retain
and report all available raw samples even if the candidate is rejected.

Run the following controller from the Spark workspace using the
specified manifest, evidence directory and two build receipts. Use a
minimal process environment with UTC. Commit the timed raw samples
with the report.

```python
import json
from pathlib import Path
import runpy
import sys

manifest, output, baseline_receipt, candidate_receipt = map(Path, sys.argv[1:])
old_controller = runpy.run_path("benches/nullif_620_recheck/run.py")
controller = old_controller["measure"].__globals__
controller["MANIFEST"] = settings = json.loads(manifest.read_text())
settings["spark_speedup_anchors"] = sorted(controller["cell_ids"]("nullif"))
binaries, identities = controller["load_builds"](
    (baseline_receipt, candidate_receipt)
)
controller["preflight"](output / "preflight", binaries, identities)
controller["FAMILIES"] = ("nullif",)
controller["measure"](
    output / "timing", binaries, identities, output / "preflight/receipt.json"
)
controller["summarize"](output / "timing")
summary = json.loads((output / "timing/summary.json").read_text())
failures = summary["detected_regressions"]
metrics = {tuple(item["key"]): item for item in summary["all_metrics"]}
for mode in ("spark-off", "spark-on"):
    for cell in settings["speedup_anchors"]:
        scored = metrics[cell, mode, "execution_ns"]
        if scored["delta"] >= -scored["noise"] or sum(
            delta < 0 for delta in scored["pair_deltas"]
        ) < 10:
            failures.append([cell, mode, "gain"])
    for cell in settings["protected_spark"]:
        scored = metrics[cell, mode, "execution_ns"]
        if scored["delta"] > scored["noise"] and sum(
            delta > 0 for delta in scored["pair_deltas"]
        ) >= 10:
            failures.append([cell, mode, "regression"])
(output / "decision.json").write_text(json.dumps({
    "pass": not failures, "failed_comparisons": failures,
}, indent=2) + "\n")
if failures:
    sys.exit("NULLIF candidate did not pass all timing checks")
```
