# Literal FROM_CSV settings (#1414)

FROM_CSV reads schema and options once per batch. The new adapter
supplies literal settings as one-element arrays for nonempty CSV
columns. The existing expansion handles empty batches, NULL option
maps and array settings.

Both builds return the same complete million-row result and executed
types in both ANSI modes. Differential tests cover successful comma
and pipe parsing with NULL rows, invalid schema and options, eager
rejection of NULL option entries, lazy validation on all-NULL input
and empty batches. Formatting, Spark-root Clippy and 671 selected
tests passed.

## Matched release timing

Values are the median and range of process medians, in ns per input
row above projection.

| Function | ANSI | Before [range] | Trial [range] | Paired reduction | Floor | Decision |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| `FROM_CSV` | off | 512.061 [507.952, 513.148] | 512.984 [512.245, 517.797] | -0.923 | 2.776 | Within comparison floor |
| `FROM_CSV` | on | 509.165 [509.090, 513.640] | 511.839 [510.807, 528.138] | -2.749 | 8.665 | Within comparison floor |

Both timing differences fall within the comparison floor. The
singleton adapter removes repeated copies of the settings. The
measurements leave the size and direction of any throughput effect
unresolved.

The [pinned comparison](../spark_only_functions_1414/from-csv-comparison.json)
followed the [shared protocol](../spark_only_functions_1414/protocol.md).
The 12 processes retained 36 samples. Raw evidence is available under
`costs-168-2026-11-20/1414/from-csv`.
