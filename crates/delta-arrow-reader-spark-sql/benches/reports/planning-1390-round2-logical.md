# Logical phase and deferred set-session snapshot (#1390)

The [logical phase table](planning-1390-round2-logical-phases.csv)
covers 25 paired statements and two stress statements. Each row
reports the median of five process medians, in nanoseconds.
The largest interval for every paired query and for CASE is
resolution. Its paired-statement median is 71.664 / 70.852 µs,
ANSI off / on. Parser construction is 26.209 / 26.219 µs.
CASE spends 1.588 / 1.573 ms resolving. IN spends most of its
time in grammar traversal, 1.099 / 1.094 ms.

In the separate [resolver table](planning-1390-round2-logical-resolver.csv),
recursive query resolution is the largest part for the paired
queries, 45.284 / 45.454 µs at the median. Set lowering takes
14.607 / 14.497 µs and includes a 9.528 / 9.368 µs session
snapshot even when the query has no set. The candidate moves that
snapshot after the existing search for pending sets.

| Query | Phase | Path | Staging ms | Candidate ms | Paired change ms | Native floor ms |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| paired/filter | logical | native-plain | 0.063 | 0.062 | -0.000 | 0.001 |
| paired/filter | logical | native-configured | 0.064 | 0.065 | +0.001 | 0.001 |
| paired/filter | logical | spark-off | 0.120 | 0.108 | -0.013 | 0.001 |
| paired/filter | logical | spark-on | 0.120 | 0.108 | -0.013 | 0.001 |
| paired/grouped_aggregates | logical | native-plain | 0.154 | 0.152 | -0.002 | 0.002 |
| paired/grouped_aggregates | logical | native-configured | 0.153 | 0.152 | -0.001 | 0.002 |
| paired/grouped_aggregates | logical | spark-off | 0.233 | 0.217 | -0.016 | 0.002 |
| paired/grouped_aggregates | logical | spark-on | 0.235 | 0.221 | -0.015 | 0.002 |
| paired/projection | logical | native-plain | 0.046 | 0.046 | -0.001 | 0.000 |
| paired/projection | logical | native-configured | 0.048 | 0.048 | +0.001 | 0.000 |
| paired/projection | logical | spark-off | 0.099 | 0.086 | -0.013 | 0.000 |
| paired/projection | logical | spark-on | 0.099 | 0.086 | -0.012 | 0.000 |
| stress/case_200 | logical | native-plain | 1.195 | 1.204 | +0.020 | 0.001 |
| stress/case_200 | logical | native-configured | 1.196 | 1.194 | +0.001 | 0.001 |
| stress/case_200 | logical | spark-off | 2.633 | 2.592 | -0.060 | 0.001 |
| stress/case_200 | logical | spark-on | 2.627 | 2.606 | -0.024 | 0.001 |
| stress/in_1000 | logical | native-plain | 0.742 | 0.742 | -0.004 | 0.015 |
| stress/in_1000 | logical | native-configured | 0.761 | 0.760 | -0.004 | 0.015 |
| stress/in_1000 | logical | spark-off | 1.805 | 1.770 | -0.050 | 0.015 |
| stress/in_1000 | logical | spark-on | 1.879 | 1.774 | -0.111 | 0.015 |

Keep the staging implementation. The candidate reduces the median
paired logical time by 13.284 / 13.073 µs across the 25 statements.
23 / 22 reach the row-level improvement criterion. Two
native-configured rows breach the regression gate. EXCEPT physical
has a median paired increase of 3.517 µs against a 0.561 µs floor.
INTERSECT total increases by 3.758 µs against 2.655 µs. Both have
five positive paired deltas and disjoint process ranges. Those
native paths do not use Spark set lowering. This session therefore
does not attribute the shifts to the deferred snapshot. The exact
trial and report are retained on `perf/1390-logical-phase-attribution`.

Using the retained staging arm of this session, CASE logical
still exceeds native-configured by 1.437 / 1.432 ms, IN by
1.044 / 1.118 ms, and the 400-column query by 1.368 / 1.367 ms.
The median gap across the 25 paired statements is 63.688 /
62.767 µs. The ordinary recursive resolution and IN grammar
costs remain.

The [matched table](planning-1390-round2-logical.csv) contains all 360 phase rows.
Each row comes from five alternating process pairs. The floor
comes from that query and phase in the native A/B controls.
Spark-to-native differences use each build's own native-configured
measurements in this session.

## Validation

Formatting, root-crate Clippy and 693 workspace library and
focused integration tests passed. These include read-only
admission, set operations and the new no-snapshot test. Both
builds matched all 678 plan traces and exact Arrow schemas.
The timed driver executed no queries and observed no Delta data
reads. The 54 rows in each instrumented table come from 10
processes and 2,430 retained samples. Timing used the uninstrumented
binaries and the preset regression check.

The [protocol](../planning_round2_1390/logical-comparison.md) was committed at
`2403849046cbeef42c465f2e9d91d8257edb59b8` before clocks. The run spanned
2026-11-21T14:56:45Z to 2026-11-21T14:57:05Z. There were 50 processes
and 13,500 retained logical/physical samples. Raw data and exact
plans are under `costs-168-2026-11-20/1390/logical-phases`.

| Build | Product source | Release executable SHA256 |
| --- | --- | --- |
| staging | `8bdf07b78a3f8ea3cf86646298be380f97d9c20d` | `1dd6ad92a5d9c9817401307efc639e81b6afa03c11d58cda0ed4913caf7a1319` |
| candidate | `cbe4704f7a64c6b8998c6293ef8a223f75ff8a96` | `b6654e04e1f0ba58c34467f974958b1e542e4728aeaf45a05f3b83251707b354` |
