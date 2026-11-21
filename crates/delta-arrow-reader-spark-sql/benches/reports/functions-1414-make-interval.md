# Literal MAKE_INTERVAL fields (#1414)

When only the year is a column, the constructor prepares the valid
literal day and time parts once. Each row keeps the checked
multiply-then-add calculation for months. NULL or out-of-range fixed
inputs use the existing evaluation, preserving which error occurs first.
MAKE_INTERVAL and TRY_MAKE_INTERVAL share this kernel.

The differential test compares literal and broadcast fields on empty
and NULL input, positive and negative intervals, checked arithmetic,
and Arrow nanosecond limits. It covers a negative month value that
would cancel a widened year multiplication and verifies error order
when both year and time exceed their ranges. Formatting, Spark-root
Clippy and 692 selected tests passed, including existing Spark references.

## Matched release timing

Values are the median and range of process medians, in ns per input row
above projection.

| Function | ANSI | Before [range] | Trial [range] | Paired reduction | Floor | Decision |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| `MAKE_INTERVAL` | off | 22.619 [22.421, 22.697] | 21.537 [21.532, 21.631] | 1.066 | 0.138 | Faster |
| `MAKE_INTERVAL` | on | 22.522 [22.450, 22.549] | 21.516 [21.236, 21.554] | 1.033 | 0.159 | Faster |
| `TRY_MAKE_INTERVAL` | off | 22.536 [22.517, 22.589] | 21.481 [21.320, 21.545] | 1.055 | 0.113 | Faster |
| `TRY_MAKE_INTERVAL` | on | 22.606 [22.398, 22.772] | 21.535 [21.440, 21.561] | 1.046 | 0.187 | Faster |

Both builds return identical million-row results and executed types.
The paired reductions clear the improvement rule in all four cases.

The [pinned comparison](../spark_only_functions_1414/make-interval-comparison.json)
followed the [shared protocol](../spark_only_functions_1414/protocol.md).
The 24 processes retained 72 samples. Raw evidence is available locally
under `costs-168-2026-11-20/1414/make-interval`.
