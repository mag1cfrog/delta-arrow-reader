# VECTOR_INNER_PRODUCT comparison protocol (#1410)

The [profile](../reports/array-functions-1410-attribution.md) and source
trace identify per-row Arrow child slices and downcasts. The candidate
uses offsets over flat FLOAT buffers with no child NULLs and keeps the
same dot product. Parent validity and the dimension check precede
the multiplication. Child NULLs keep the slice-based path.

Use the frozen `scalar.vector.vector_inner_product` case selected by
[vector-inner-product.json](vector-inner-product.json), with one million
deterministic input rows. Check all keyed values and executed Arrow
types (`Int64`, `Float32`) against staging and the native control in
both ANSI modes before clocks. Exclude the case if any comparison
differs. Check sliced and empty inputs, both offset widths, parent
and child NULLs, dimension errors and exact FLOAT bits with tests.

Use the [matched schedule and pass rule](../window_aggregates_1408/string-aggregation-protocol.md#schedule-and-pass-rule)
in one Linux release session. Rotate staging and candidate across
three replicas. Run three measured query/projection pairs per
process after warmup and retain the candidate native A/A controls.
Each ANSI median must be at or below the candidate native median
plus its repeat floor. If the target is missed, report the remaining
cost without another attempt.
