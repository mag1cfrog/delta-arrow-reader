# Pending RANGE isolation protocol (#1099)

The [previous recheck](pending-range-1099-recheck.md) preserved the Spark
gain and detected eight native regressions. This candidate puts the
pending search inside a separate Spark aggregate ordered by a signed
integer column. The native
window planner, aggregate loops and RANGE state source are the same
in both builds.

## Builds and correctness

The [source pins](../pending_range_1099_isolated/sources.json) specify
staging and the candidate. Build both with Rust 1.96.0, the same locked
release configuration and the same generated harness. Keep build and
measurement files outside the checkout. Compare size, alignment and
field offsets of the shared window state. Compare the native plain and
sliding stateful evaluators, the two native aggregate result loops and
the RANGE calculation routine. Pair the two result-loop instantiations
by size. Normalize linked addresses and compiler-generated anonymous
relocation labels. Keep named targets, branch offsets, field offsets
and instruction immediates. Include the comparison in the report.

Use the existing [pending-frame manifest](../pending_range_1099/manifest.json)
and frozen Spark 4.2.0 capture. Require both builds to agree on all 63
trace contexts, including complete rows, Fields, failures and
progressive delivery. Check the Spark references for both ANSI modes.
Wrapped cases with different native results remain untimed. The 27
constant contexts must all remain eligible.

The [native controls](../pending_range_1099_isolated/native-controls.json)
use one million increasing INT keys in 8,192-row batches. The partition
control has 1,000 rows per partition. Require one bounded window,
unchanged native physical plan and Fields and exact COUNT for every row.
Run correctness checks outside the timers.

## One Linux timing session

Pin to CPU 2. In each of 12 blocks launch staging, candidate, candidate
and staging in separate processes. Reverse the order on alternate
blocks. Each process times the 27 frozen contexts followed by the three
ordinary native controls with rotated control order. Use two warmups and
seven retained executions per cell. Include execution and collection
time. Exclude planning, synthetic input construction and result checking.
Cap the session at 35 minutes and each process at five minutes. Run with
no concurrent builds or local measurements.

Use the same block and replicate medians as the recheck. Native
regression requires a positive median paired shift above session
repeat noise and one-sided sign-test rejection, Holm-corrected across
all 12 native cells at 5%. Both 4,096-row, one-row-batch Spark anchors
must be at least ten times faster with a paired improvement above
repeat noise and the sign test across both anchors. Report all 18
Spark mode comparisons. A changed result, changed shared layout,
missing sample, failed process or expired deadline fails the gate.
Each timed execution flushes its sample before checking the result.
The coordinator archives the completed and partial process output.
Keep all generated samples with the report even if the candidate fails.

Run from the Spark SQL crate directory in the protocol checkout.

```sh
python3 benches/pending_range_1099_isolated/build.py SOURCE TARGET OUTPUT
python3 benches/pending_range_1099_isolated/compare_native.py STAGING_BINARY CANDIDATE_BINARY DISASSEMBLY_OUTPUT
python3 benches/pending_range_1099_isolated/run.py STAGING_BUILD CANDIDATE_BUILD SPARK_CAPTURE OUTPUT
```
