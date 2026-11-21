# Literal MONTHS_BETWEEN rounding option (#1414)

The final round-off kernel reads a literal Boolean without broadcasting
it. It applies the existing rounding and NULL handling to the double
column and keeps the same result length. Array flags still use the
existing row evaluation.

Differential tests compare the exact floating-point bits and NULLs with
a broadcast flag, including NaN, infinity, signed zero and empty input.
The selected tests include the saved Spark MONTHS_BETWEEN comparisons.
Formatting, Spark-root Clippy and 691 selected tests passed.

## Matched release timing

Values are the median and range of process medians, in ns per input row
above projection.

| Function | ANSI | Before [range] | Trial [range] | Paired reduction | Floor | Decision |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| `MONTHS_BETWEEN` | off | 55.233 [54.986, 57.387] | 54.595 [54.594, 56.546] | 0.639 | 1.201 | Within comparison floor |
| `MONTHS_BETWEEN` | on | 54.723 [54.629, 56.995] | 54.481 [54.237, 57.315] | 0.392 | 1.539 | Within comparison floor |

The two builds return the same million rows and executed types in each
ANSI mode. Both timing differences fall within their comparison floors.
The change removes the literal Boolean broadcast in both modes.

The [pinned comparison](../spark_only_functions_1414/months-between-comparison.json)
followed the [shared protocol](../spark_only_functions_1414/protocol.md).
The 12 processes retained 36 samples. Raw evidence is available locally
under `costs-168-2026-11-20/1414/months-between`.
