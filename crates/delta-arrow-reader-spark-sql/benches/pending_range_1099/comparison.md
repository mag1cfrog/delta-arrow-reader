# Pending RANGE search comparison (#1099)

Use the [attribution protocol](README.md) unchanged for shapes, input,
release settings, CPU and schedule. This comparison permits one six-process
candidate schedule. The two baseline diagnosis processes exhausted the
optional profile allowance. The 18 differing wrapped pairs remain counts-only.

Before clocks, compare the two candidate traces with each other and both
ANSI modes with the same Spark 4.2.0 captures. Require exact agreement with
the baseline's complete canonical trace, including native results, types,
plan evidence, batch boundaries, deferred errors and output before EOF.
No prerequisite restarts the baseline timings.

For every constant Spark context and every wrapped ANSI-off context, the
candidate should perform one upper comparison per input row before EOF in
the observed build. Compare the resulting work counters with the baseline.
Keep the accumulator, copy volume and retained rows visible. The completed
and still-waiting controls must preserve their output boundaries. All native
work counters must agree exactly. The focused committed differential tests
cover changed rows, append-only reuse, pruning and fallback controls.

Only the 27 ordinary constant cells are timed. Each canonical release
process uses its own native arm in the same binary as its Spark arms.
Compare the medians of 21 samples and the ranges of three process medians.
Flag a concrete native timing regression if the candidate range is entirely
above the baseline and the median increase exceeds the larger of the two
within-revision ranges. Display smaller differences with their ranges.

Report whole executor wall times and the observed source regions separately.
The region times include instrumentation through EOF. Scaling of comparisons,
not a wall-clock threshold, tests whether the repeated search is eliminated.
