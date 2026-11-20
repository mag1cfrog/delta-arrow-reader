# Wide projection planning comparison (#1390, PR3)

The attribution found two passes of the projection rule taking about 4.85 ms
on the 400-column Spark query and 0.51 ms on native-configured. This slice
tests one change to repeated column lookups while merging projections.
[Attribution](planning-1390.md)

## References and acceptance

The current baseline is staging
`fa71ede95fa1b6ea68dc5e6217a3b142feb8df79`. The current baseline's canonical
phase times are in the [statement parser report](planning-1390-parser-walls.csv).
The rule was unchanged in that slice, so the observed projection-rule
reference is the [original stage table](planning-1390-stages.csv). The two
reference builds serve different comparisons. Report the rule measurements
with their own same-binary A/B differences.

The candidate changes the existing DataFusion optimizer fork. The focused
correctness tests cover the old field-resolution rules and compare exact
native rows, NULLs and executed Arrow types in plain and configured sessions.
Both session kinds use the same candidate fork. Run those tests and the full
Spark workspace, formatting and strict root-crate Clippy before timing. Keep
all previous named test outcomes.

The canonical and observed drivers compare all 678 historical planning
snapshots through the public APIs. Each trace includes 558 ordinary Spark
queries and both native controls for all 30 paired and stress queries.
Stop on any changed plan, field, property or schema, and on any formerly
eligible query failure. The driver rejects any Delta data reads during this
check. The measurement schedules only plan queries.

## Measurement

Use the same release flags, local Linux CPU 2, fixture hashes and target
configuration as the [attribution protocol](planning-1390-protocol.md).
Reuse the frozen attribution schedule's 30 canonical processes. They cover
five main processes for each of four configurations and five native-configured
A/B pairs. The canonical walls compare every input's logical, physical and
total times with the retained parser slice. They also show the cumulative
change since the attribution baseline.

Reuse the 40 observed processes for rule attribution on the candidate. Each
input/configuration has five A/B pairs. Sum only the elapsed bodies for
`optimize_projections` across its passes within each sample. Check the
recorded pass indices against the reference. Use the same two warmups, nine
samples and 300-second timeout per process. The observer times the rewrite,
apart from per-rule validation and preparation. Its measurements do not
replace the canonical phase times.

Eight untimed plan-trace processes and these 70 timing processes fit the same
78-process cap as the attribution. Both builds and all runs are serial.
Compare the results and timings of native-plain and native-configured from
the same release executable as the Spark measurements. Record every observed
native slowdown beyond process dispersion and same-shape A/B variation from
the two rounds. An unresolved native timing regression blocks retaining the
fork change. A remaining Spark cost is recorded with its measured size and
attributed code, then the issue stays open for a maintainer decision.

Run from the Spark workspace after committing both the protocol and
candidate. Use fresh destinations, the exact local #1379 fixture and the
owned Cargo target. Run the build and trace for each mode before the
measurement steps.

```sh
runner=benches/planning_attribution/replay.py
manifest=benches/planning_attribution/projection-rule.json
python3 "$runner" build "$manifest" "$EVIDENCE/source" "$OWNED_TARGET" "$EVIDENCE/build"
python3 "$runner" build-observed "$manifest" "$EVIDENCE/source-observed" "$OWNED_TARGET" "$EVIDENCE/build-observed"
python3 "$runner" trace "$manifest" "$EVIDENCE/build/receipt.json" "$DAR_DELTA_ROOT" "$EVIDENCE/plans"
python3 "$runner" trace "$manifest" "$EVIDENCE/build-observed/receipt.json" "$DAR_DELTA_ROOT" "$EVIDENCE/plans-observed"
python3 "$runner" measure "$manifest" "$EVIDENCE/plans" "$DAR_DELTA_ROOT" "$EVIDENCE/run"
python3 "$runner" measure "$manifest" "$EVIDENCE/plans-observed" "$DAR_DELTA_ROOT" "$EVIDENCE/run-observed"
python3 "$runner" summarize "$manifest" "$EVIDENCE/run" "$EVIDENCE/walls.csv"
python3 "$runner" summarize-projection-rule "$manifest" "$EVIDENCE/run-observed" "$EVIDENCE/rule"
```

Keep the complete process logs and the sealed executables with the local
measurements. Include the canonical samples, extracted rule samples and
before/after tables in the report. If the candidate fails a gate, preserve
that result and keep the existing baseline. This slice has one optimization
attempt.
