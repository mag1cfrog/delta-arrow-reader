# MEDIAN and PERCENTILE batch counts (#1413)

The [attribution](../reports/functions-1413-attribution.md) found
the shared numeric accumulator's per-row frequency update. For an
unweighted Double batch, the candidate validates the total nonnull
count once and inserts the same values in the same order. The
per-row path handles an overflowing total, weighted inputs and
retractions.

Require exact unit comparisons against the per-row path for nulls,
empty batches, slices, nonfinite values, signed zero, probabilities
and retractions. The overflow test checks the error and the state
after a successful row precedes it. Run the existing
[Spark comparisons](../../tests/percentile_frequency.rs)
for PERCENTILE and MEDIAN in both ANSI modes.

Use the [unchanged aggregate comparisons](percentile-count-comparison.json).
Require both builds and all three modes to consume one million
input rows and return the same single result and executed Float64
type. Check that both Spark builds use the same percentile plan.
Use the [matched release schedule](../window_aggregates_1408/string-aggregation-protocol.md#schedule-and-pass-rule)
with a 1,800-second bound. Keep the shared change if at least one
function improves in both ANSI modes in at least two of three process
pairs and its paired median reductions exceed its native A/A floor.
Reject it for a Spark mode with all three pairs slower, a median
increase above its floor and disjoint process median ranges. Report
both native builds and every gap above candidate native plus the floor.
