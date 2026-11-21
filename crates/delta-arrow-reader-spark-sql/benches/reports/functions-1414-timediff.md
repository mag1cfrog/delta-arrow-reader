# Literal TIMEDIFF end timestamp (#1414)

With a column start and a literal microsecond end timestamp, the kernel
converts the endpoint to local time once. Both paths share the same
local difference calculation and retain the original rounding, overflow
and NULL behavior. Invalid units and other argument shapes use the
existing evaluation.

The differential test compares literal and broadcast endpoints for
empty and NULL inputs, UTC and a DST transition, invalid units and
zones, and the extreme microsecond values. Formatting, Spark-root
Clippy and 680 selected tests passed, including existing Spark references.

## Matched release timing

Values are the median and range of process medians, in ns per input row
above projection.

| Function | ANSI | Before [range] | Trial [range] | Paired reduction | Floor | Decision |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| `TIMEDIFF` | off | 42.142 [42.118, 42.212] | 27.119 [27.072, 27.202] | 14.999 | 0.065 | Faster |
| `TIMEDIFF` | on | 41.995 [41.813, 42.162] | 27.138 [27.077, 27.159] | 14.918 | 0.175 | Faster |

Each build returned the same million rows and executed types in both
ANSI modes. The paired reductions clear the improvement rule in both
modes.

The [pinned comparison](../spark_only_functions_1414/timediff-comparison.json)
followed the [shared protocol](../spark_only_functions_1414/protocol.md).
The 12 processes retained 36 samples. Raw evidence is available locally
under `costs-168-2026-11-20/1414/timediff`.
