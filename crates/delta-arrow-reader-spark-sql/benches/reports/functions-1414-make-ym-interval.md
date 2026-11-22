# Literal MAKE_YM_INTERVAL months (#1414)

With a year column and literal integer months, MAKE_YM_INTERVAL uses
the same checked arithmetic without expanding the month. This includes
the default month of zero. Its original adapter handles other shapes.

The differential test compares exact output and errors for empty or
NULL input, signed year and month extremes, sliced arrays and
multiply-before-add overflow. Formatting, Spark-root Clippy and 680
selected tests passed, including existing Spark references.

## Matched release timing

Values are the median and range of process medians, in ns per input row
above projection.

| Function | ANSI | Before [range] | Trial [range] | Paired reduction | Floor | Decision |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| `MAKE_YM_INTERVAL` | off | 2.336 [2.310, 2.398] | 1.911 [1.868, 1.916] | 0.442 | 0.044 | Faster |
| `MAKE_YM_INTERVAL` | on | 2.312 [2.304, 2.313] | 1.904 [1.861, 1.922] | 0.400 | 0.030 | Faster |

Both builds returned identical million-row results and executed types
in each ANSI mode. The paired reductions clear both comparison floors.

The [pinned comparison](../spark_only_functions_1414/make-ym-interval-comparison.json)
followed the [shared protocol](../spark_only_functions_1414/protocol.md).
The 12 processes retained 36 samples. Raw evidence is available locally
under `costs-168-2026-11-20/1414/make-ym-interval`.
