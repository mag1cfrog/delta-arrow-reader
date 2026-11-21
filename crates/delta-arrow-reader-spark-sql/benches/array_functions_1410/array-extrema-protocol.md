# Typed BIGINT list extrema comparison (#1410)

Use the two frozen statements in [array_extrema.json](array_extrema.json)
and the [matched schedule](../window_aggregates_1408/string-aggregation-protocol.md#schedule-and-pass-rule).
Keep the #1391 fixture, projection, CPU affinity and Linux release flags.
Require all million results by ID, both executed column types and input
guards to agree across both builds, ANSI modes and native before clocks.

The committed differential compares with the original accumulators.
Include List and LargeList arrays, empty lists and batches, NULL parents
with nonempty child ranges, NULL children, sliced offsets, and both
BIGINT bounds. Check the floating fallback's NaN and signed zero bits.

Report median and range of nine retained samples in ns per input row
above projection. For each function and ANSI mode the target is candidate
Spark at or below candidate native plus the same-session native A/A floor
from process medians. Exclude mismatched cases. Record any remaining gaps
and their causes after this comparison.
