# Logarithm input validity (#1413)

The [attribution](../reports/functions-1413-attribution.md) shows a
CASE around the argument of each Spark logarithm. The candidate uses
the same greater-than-zero comparison for a Double column and applies
the mask to the existing values buffer. LOG1P still adds one first.
The original CASE handles other inputs. The logarithm kernels remain
the same.

The [differential test](../../tests/floating_extrema/logarithm_domain.rs)
compares with the original CASE in both ANSI modes. The inputs include
signed zeros, both signs of quiet and signaling NaNs, infinities,
subnormal values, NULL values and bases, and slices and empty batches.
Require exact output fields and Float64 bits.

Use the [unchanged five comparisons](log-domain-comparison.json).
Verify the original CASE and candidate mask in the actual plans.
Require exact million-row results and executed types across both builds,
native and both ANSI modes. Use the
[matched release schedule](../window_aggregates_1408/string-aggregation-protocol.md#schedule-and-pass-rule)
with a 1,800-second bound. Keep the shared change if any function improves
in at least two of three paired process medians in both ANSI modes and
both paired median decreases exceed its own native A/A floor. Reject
the change if any Spark mode has all three pairs slower, a median
increase above the floor and disjoint process median ranges. Report
both native builds and every candidate gap above native plus that floor.
