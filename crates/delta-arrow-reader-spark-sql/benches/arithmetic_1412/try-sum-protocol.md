# Bounded integer TRY_SUM comparison (#1412)

The [attribution](../reports/arithmetic-1412-attribution.md#try_sum-and-bitmap-buckets)
identifies the ordered overflow check in the locked TRY_SUM
accumulator. If all raw values fit in INT and the running BIGINT
sum has enough headroom for the absolute sum of the entire batch,
no prefix sum can overflow. Reduce the batch with Arrow SUM and
pass its result to the existing accumulator. It still owns overflow,
partial states and merges. Batches with an empty or failed state
use the original update.

Compare the [frozen query](try-sum.json) with both ANSI modes and native.
Require exact values and executed Arrow types on both builds. First run
the [untimed SQL controls](try-sum-validation.json) for INT-range
bounds, wider inputs, both overflow directions, NULLs, empty input,
grouping, DISTINCT and FILTER, windows, and DECIMAL and DOUBLE
results. An accumulator test also compares state after each update,
including overflow that a later value would otherwise cancel.

Use the [matched Linux release schedule](../window_aggregates_1408/string-aggregation-protocol.md#schedule-and-pass-rule).
The target is at or below candidate native plus the same-session A/A
floor in both ANSI modes.
