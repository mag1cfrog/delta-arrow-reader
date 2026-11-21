# Literal numeric TO_CHAR and TO_VARCHAR formats (#1414)

The numeric formatter reads a format once per batch. The trial supplies
one copy of a Utf8 literal format for a nonempty value column. Empty
batches, other argument shapes and broadcasts beyond Utf8 offset limits
use the existing adapter. Datetime and binary formatting use separate
functions.

The differential test compares the existing adapter on decimal and
floating-point columns, NULLs, invalid formats, empty batches and the
Utf8 offset boundary in both ANSI modes. Formatting, Spark-root Clippy
and 675 selected tests passed.

## Matched release timing

Values are the median and range of process medians, in ns per input row
above projection.

| Function | ANSI | Before [range] | Trial [range] | Paired reduction | Floor | Decision |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| `TO_CHAR` | off | 153.072 [152.192, 153.984] | 159.188 [156.739, 159.832] | -6.760 | 1.547 | Slower |
| `TO_CHAR` | on | 154.161 [152.916, 161.642] | 157.412 [156.999, 167.988] | -2.837 | 5.495 | Within comparison floor |
| `TO_VARCHAR` | off | 153.923 [153.115, 155.384] | 157.345 [156.528, 157.515] | -3.592 | 1.134 | Slower |
| `TO_VARCHAR` | on | 153.543 [153.110, 155.416] | 158.285 [157.678, 179.737] | -4.742 | 11.030 | Within comparison floor |

All eight million-row result files and executed types matched their
reference. The trial regressed for both names with ANSI off. The timing
cannot isolate which part of the change accounts for the increase.
Staging keeps the existing adapter and its repeated format arrays.

The [pinned comparison](https://github.com/mag1cfrog/delta-arrow-reader/blob/39277deb3877a35f92f688262e1ade4ff6bb7c0f/crates/delta-arrow-reader-spark-sql/benches/spark_only_functions_1414/to-char-comparison.json)
followed the [shared protocol](../spark_only_functions_1414/protocol.md).
The 24 processes retained 72 samples. Raw evidence is available locally
under `costs-168-2026-11-20/1414/to-char`.
