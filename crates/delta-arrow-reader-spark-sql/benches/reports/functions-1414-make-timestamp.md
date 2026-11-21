# Literal MAKE_TIMESTAMP fields (#1414)

The six-argument constructor uses the existing calendar and clock rules
to compute the timestamp of the first day once when only the day varies.
It gets the valid day range from Chrono and adds whole days to that
first timestamp. Invalid fixed settings, NULL literal fields and other
argument shapes use the existing evaluation. All six MAKE_TIMESTAMP
forms share this constructor.

The differential test compares the literal and broadcast paths for
leap and non-leap February, 30- and 31-day months, fractional seconds,
leap-second rollover, years 1 and 9999, invalid inputs and NULLs. It
compares complete Arrow arrays or the same execution error in both
strict and TRY constructors. Formatting, Spark-root Clippy and 690
selected tests passed.

## Matched release timing

Values are the median and range of process medians, in ns per input row
above projection.

| Function | ANSI | Before [range] | Trial [range] | Paired reduction | Floor | Decision |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| `MAKE_TIMESTAMP` | off | 59.273 [59.078, 59.692] | 43.318 [43.198, 44.297] | 15.759 | 0.550 | Faster |
| `MAKE_TIMESTAMP` | on | 59.053 [59.043, 59.137] | 43.439 [43.310, 43.696] | 15.698 | 0.193 | Faster |
| `MAKE_TIMESTAMP_LTZ` | off | 59.394 [58.993, 59.441] | 43.332 [43.224, 43.430] | 16.011 | 0.224 | Faster |
| `MAKE_TIMESTAMP_LTZ` | on | 59.380 [59.092, 59.558] | 43.233 [43.226, 43.353] | 16.155 | 0.233 | Faster |
| `MAKE_TIMESTAMP_NTZ` | off | 23.109 [22.885, 23.182] | 7.954 [4.579, 8.571] | 15.155 | 1.996 | Faster |
| `MAKE_TIMESTAMP_NTZ` | on | 22.914 [22.903, 26.448] | 8.107 [7.877, 8.446] | 15.026 | 1.772 | Faster |
| `TRY_MAKE_TIMESTAMP` | off | 59.448 [59.246, 60.063] | 43.329 [43.217, 43.455] | 16.118 | 0.409 | Faster |
| `TRY_MAKE_TIMESTAMP` | on | 59.657 [59.281, 60.099] | 43.406 [43.195, 43.526] | 16.462 | 0.409 | Faster |
| `TRY_MAKE_TIMESTAMP_LTZ` | off | 59.203 [59.189, 59.335] | 43.383 [43.305, 43.438] | 15.884 | 0.073 | Faster |
| `TRY_MAKE_TIMESTAMP_LTZ` | on | 59.292 [59.250, 59.472] | 43.241 [43.209, 43.275] | 16.084 | 0.111 | Faster |
| `TRY_MAKE_TIMESTAMP_NTZ` | off | 22.892 [22.823, 22.951] | 8.360 [8.286, 8.448] | 14.532 | 0.081 | Faster |
| `TRY_MAKE_TIMESTAMP_NTZ` | on | 22.958 [22.847, 22.998] | 7.827 [4.551, 8.362] | 15.020 | 1.906 | Faster |

The physical plans pass a day column and five constants to the shared
constructor in all six forms. The default and LTZ forms keep their UTC
cast. Each comparison returned the same million input and output rows
and executed types. The paired reductions clear the improvement rule
in every mode, from 14.532 to 16.462 ns per input row.

The [pinned comparison](../spark_only_functions_1414/make-timestamp-comparison.json)
followed the [shared protocol](../spark_only_functions_1414/protocol.md).
The 72 processes retained 216 samples. Raw evidence is available locally
under `costs-168-2026-11-20/1414/make-timestamp`.
