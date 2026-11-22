# NULLIF candidate rebase check (#620)

The November 22 decision approves the saved NULLIF patch and withdraws the
prior requirement to observe both fallback bands. Staging changed
`UPSTREAM.md` after the patch was saved. This run checks the rebased candidate
on the three native controls and two sparse two-column cases from the
[prior recheck](nullif-620-recheck.md).

## Builds and correctness

Use the staging and candidate commits in [rebase.json](../nullif_620_round3/rebase.json).
Build the existing `nullif_620` target on Linux with Rust 1.96.0,
`--locked --release` and release debug level 1. Keep build settings equal.
Record both executable digests, source revisions and the toolchain version.
Pass the existing NULLIF and checked-arithmetic preflights in both binaries.
Require all 648 value and executed-type observations to match the native
reference, and check the 16 NULLIF operand-effect records per build. Run
the full candidate workspace suite separately.

## Timing and decision

Use the existing controller and Arrow fixtures. Run 12 four-process blocks
on CPUs 2-5. Start odd blocks with staging and even blocks with the candidate.
The middle positions run the other binary twice. Each process warms each of
the five cells once and retains three observations in native, Spark ANSI off
and Spark ANSI on. Keep plan and execution measurements separate. Run no
other builds or timings during this session. The preflight is capped at
25 minutes and the 48 timed processes at 20 minutes, with a three-minute
limit per process.

Reuse the recheck's paired delta and same-binary noise definitions. For
the three native controls, a median paired increase above noise with a
one-sided sign test significant under Holm correction at 0.025 fails the
check. For each sparse cell in both ANSI modes, require a paired median
decrease larger than repeat noise. The check passes when all seven
comparisons meet these rules. A missing sample, timeout, correctness
failure or insufficient decrease fails the check. Retain all samples
already collected and report any excluded cells.

From the Spark workspace, pass the evidence directory and two build receipts
to the following runner. Each receipt gives the source commit, target,
profile, toolchain, build command, binary path and executable digest. The
existing controller validates those pins. Supply a minimal process
environment with UTC, and record no inherited environment values. Use an empty "environment" object
in both receipts for compatibility with the existing controller.

```python
import json
from pathlib import Path
import runpy
import sys

output, staging_receipt, candidate_receipt = map(Path, sys.argv[1:])
previous = runpy.run_path("benches/nullif_620_recheck/run.py")
controller = previous["measure"].__globals__
controller["MANIFEST"] = json.loads(
    Path("benches/nullif_620_round3/rebase.json").read_text()
)
binaries, identities = controller["load_builds"](
    (staging_receipt, candidate_receipt)
)
controller["preflight"](output / "preflight", binaries, identities)
controller["FAMILIES"] = ("nullif",)
controller["measure"](
    output / "timing", binaries, identities, output / "preflight/receipt.json"
)
controller["summarize"](output / "timing")
summary = json.loads((output / "timing/summary.json").read_text())
native = summary["gate_metrics"]
anchors = set(controller["MANIFEST"]["spark_speedup_anchors"])
sparse = [
    item for item in summary["all_metrics"]
    if item["key"][0] in anchors
    and item["key"][1] in {"spark-off", "spark-on"}
    and item["key"][2] == "execution_ns"
]
assert len(native) == 3 and len(sparse) == 4
failures = summary["detected_regressions"] + [
    item["key"] for item in sparse if item["delta"] >= -item["noise"]
]
(output / "decision.json").write_text(json.dumps({
    "pass": not failures, "failed_comparisons": failures,
}, indent=2) + "\n")
if failures:
    sys.exit("Rebase timing check failed")
```

Commit the manifest and protocol before timing. Commit the compressed raw
samples, schedule, build and preflight identities, and report together.
