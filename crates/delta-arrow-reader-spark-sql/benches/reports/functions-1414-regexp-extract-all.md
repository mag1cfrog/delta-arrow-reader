# Literal REGEXP_EXTRACT_ALL capture group (#1414)

The trial checks an in-range literal index against a compiled literal
pattern once per batch. It retains the existing row checks for other
arguments, including invalid indices whose error depends on whether a
row matches. A differential test covers NULL and empty inputs,
optional captures, Unicode and invalid patterns and indices.
Formatting, Spark-root Clippy and 673 selected tests passed.

## Matched release timing

Values are the median and range of process medians, in ns per input row
above projection.

| Function | ANSI | Before [range] | Trial [range] | Paired reduction | Floor | Decision |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| `REGEXP_EXTRACT_ALL` | off | 143.731 [143.367, 144.165] | 154.299 [153.462, 168.411] | -10.133 | 7.475 | Slower |
| `REGEXP_EXTRACT_ALL` | on | 145.090 [142.965, 147.070] | 152.719 [152.159, 153.168] | -7.069 | 2.052 | Slower |

Both builds returned the same million rows and executed types in each
ANSI mode. The paired increases exceed both floors. The timing cannot
isolate the cause of the increase, so staging keeps the row checks.

The [pinned comparison](https://github.com/mag1cfrog/delta-arrow-reader/blob/7c38659a614621e233f2ea0e92298b32ffc014ab/crates/delta-arrow-reader-spark-sql/benches/spark_only_functions_1414/regexp-extract-all-comparison.json)
followed the [shared protocol](../spark_only_functions_1414/protocol.md).
The 12 processes retained 36 samples. Raw evidence is available locally
under `costs-168-2026-11-20/1414/regexp-extract-all`.
