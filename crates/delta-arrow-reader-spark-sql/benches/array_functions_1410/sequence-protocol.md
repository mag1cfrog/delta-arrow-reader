# SEQUENCE comparison protocol (#1410)

The [profile](../reports/array-functions-1410-attribution.md) and source
trace identify sequence-specific row preparation and integer emission.
The candidate checks default INT ranges and stores offsets directly
when neither argument array contains NULLs. It keeps the same limits,
element field and lazy argument evaluation. Other inputs use the
original generator.

Use the frozen `scalar.array.sequence` case selected by
[sequence.json](sequence.json), with one million deterministic rows.
Compare all keyed values and executed Arrow types across staging,
candidate and the native range control in both ANSI modes before
clocks. Exclude the case if any comparison differs. The differential
covers sliced and empty batches, ascending and descending ranges,
equal endpoints, INT limits, NULLs and row and batch size errors.

Use the [matched schedule and pass rule](../window_aggregates_1408/string-aggregation-protocol.md#schedule-and-pass-rule)
in one Linux release session. Rotate staging and candidate across
three replicas. Run three measured query/projection pairs per
process after warmup and retain the candidate native A/A controls.
Each ANSI median must be at or below the candidate native median
plus its repeat floor. If the target is missed, report the remaining
cost without another attempt.
