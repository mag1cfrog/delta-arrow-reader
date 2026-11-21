# SLICE comparison protocol (#1410)

The [profile](../reports/array-functions-1410-attribution.md) and source
trace identify the wrapper's per-row start/end preparation. The
candidate passes representable positive scalar coordinates to the
existing native slice kernel for List and LargeList arrays. The
existing path still handles other inputs and restores result fields.

Use the frozen `scalar.array.slice` case selected by
[array-slice.json](array-slice.json), with one million deterministic
rows. Compare all keyed values and executed Arrow types across
staging, candidate and native, in both ANSI modes before clocks.
Exclude the case if any comparison differs. Compare scalar bounds
against row coordinates in the differential tests, with both list
widths, original element fields, NULLs, empty and sliced batches,
and coordinates beyond the end or outside the positive bound.

Use the [matched schedule and pass rule](../window_aggregates_1408/string-aggregation-protocol.md#schedule-and-pass-rule)
in one Linux release session. Rotate staging and candidate across
three replicas. Run three measured query/projection pairs per
process after warmup and retain the candidate native A/A controls.
Each ANSI median must be at or below the candidate native median
plus its repeat floor. If the target is missed, report the remaining
cost without another attempt.
