# Literal FROM_JSON settings (#1414)

FROM_JSON reads one schema and one options map per batch. The singleton
settings path keeps those literals as one-element arrays for nonempty
JSON columns. Empty batches, array settings and NULL or sliced option
maps use the existing expansion. A slice can retain entries outside
the selected map row, which the original broadcast filters out.

Both builds return identical million-row results and executed types
in both ANSI modes. Differential tests also compare malformed JSON,
invalid and NULL schema, empty batches and date formats from valid,
NULL and sliced maps. Formatting, Spark-root Clippy and 690 selected
tests passed.

## Matched release timing

Values are the median and range of process medians, in ns per input row
above projection.

| Function | ANSI | Before [range] | Trial [range] | Paired reduction | Floor | Decision |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| `FROM_JSON` | off | 236.320 [236.119, 238.110] | 233.686 [232.149, 235.543] | 2.433 | 1.697 | Faster |
| `FROM_JSON` | on | 236.381 [236.256, 250.954] | 233.474 [232.945, 259.354] | 3.436 | 13.205 | Within comparison floor |

The ANSI-off reduction clears the improvement rule. The ANSI-on
measurements overlap within their floor. The singleton path
removes repeated literal copies in both modes.

The [pinned comparison](../spark_only_functions_1414/from-json-comparison.json)
followed the [shared protocol](../spark_only_functions_1414/protocol.md).
The 12 processes retained 36 samples. Raw evidence is available
locally under `costs-168-2026-11-20/1414/from-json`.
