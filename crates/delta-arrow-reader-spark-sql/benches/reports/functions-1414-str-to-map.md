# STR_TO_MAP delimiter reuse (#1414)

STR_TO_MAP compiles a scalar key/value delimiter before processing a
batch. The row loop now borrows that regex. Delimiter columns still
parse the regex for the current row. The resulting map keeps its
key and value types, offsets and duplicate-key policy.

Both builds returned identical results and Arrow types over all one
million rows in both ANSI modes. Tests compare literal and repeated
column delimiters across both duplicate policies, regex and Unicode
delimiters, NULL and empty lists, and empty input. Formatting,
Spark-root Clippy and 672 selected tests passed.

## Matched release timing

Values are the median and range of process medians, in ns per input
row above projection.

| Function | ANSI | Before [range] | Trial [range] | Paired reduction | Floor | Decision |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| `STR_TO_MAP` | off | 319.819 [316.331, 324.596] | 201.733 [201.680, 202.497] | 117.322 | 4.132 | Faster |
| `STR_TO_MAP` | on | 314.163 [312.769, 314.654] | 202.133 [201.335, 203.112] | 111.542 | 0.942 | Faster |

All process pairs favor borrowing. Both paired median reductions
exceed the measured floor.

The [pinned comparison](../spark_only_functions_1414/str-to-map-comparison.json)
followed the [shared protocol](../spark_only_functions_1414/protocol.md).
The 12 processes retained 36 samples. Raw data is available locally
under `costs-168-2026-11-20/1414/str-to-map`.
