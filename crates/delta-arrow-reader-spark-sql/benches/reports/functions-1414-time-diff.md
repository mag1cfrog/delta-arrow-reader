# Literal TIME_DIFF end time (#1414)

For a time column, literal unit and literal end time, TIME_DIFF applies
the existing subtraction and division without broadcasting the end.
Other input shapes use the existing evaluation.

The differential test compares literal and broadcast end times for all
five units, invalid and NULL units, empty batches, time-of-day bounds,
NULL values and mismatched batch lengths. Formatting, Spark-root Clippy
and 679 selected tests passed.

## Matched release timing

Values are the median and range of process medians, in ns per input row
above projection.

| Function | ANSI | Before [range] | Trial [range] | Paired reduction | Floor | Decision |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| `TIME_DIFF` | off | 3.924 [3.919, 4.060] | 3.726 [3.697, 3.741] | 0.227 | 0.071 | Faster |
| `TIME_DIFF` | on | 3.902 [3.876, 3.906] | 3.709 [3.686, 3.723] | 0.193 | 0.018 | Faster |

Both builds return identical million-row results and executed types in
each ANSI mode. The paired reductions clear the improvement rule in
both modes.

The [pinned comparison](../spark_only_functions_1414/time-diff-comparison.json)
followed the [shared protocol](../spark_only_functions_1414/protocol.md).
The 12 processes retained 36 samples. Raw evidence is available locally
under `costs-168-2026-11-20/1414/time-diff`.
