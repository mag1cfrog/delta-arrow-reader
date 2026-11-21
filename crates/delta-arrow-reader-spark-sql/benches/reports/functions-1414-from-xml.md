# FROM_XML literal settings trial (#1414)

The existing XML adapter broadcasts literal schema and options as full
arrays, then reads the first value of each. The trial passed each setting
as a one-element array for a nonempty XML input column. It kept the
original expansion for empty batches, NULL options maps and array
settings. Both builds returned the same one million rows and executed
types in both ANSI modes.

## Timing

Values are the median and range of process medians, in ns per input row
above the plain projection. A negative paired reduction means the
trial was slower.

| Function | ANSI | Before [range] | Trial [range] | Paired reduction | Floor | Decision |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| `FROM_XML` | off | 1442.316 [1440.211, 1468.687] | 1474.481 [1459.746, 1484.881] | -19.535 | 14.238 | Slower |
| `FROM_XML` | on | 1439.143 [1431.424, 1439.882] | 1467.757 [1449.205, 1475.629] | -28.614 | 13.212 | Slower |

All three process pairs became slower in both ANSI modes. The paired
increases exceeded the floors. The trial removed literal expansion,
but the timing cannot isolate the cause of the increase. Staging keeps
the existing adapter and its repeated settings arrays.

The differential test covers empty input, invalid and NULL schema,
malformed XML and valid, invalid and NULL option maps. It compares the
original broadcast with the trial at the same input lengths.
An explicit empty schema string panics in the original struct builder
under direct UDF invocation. That case is outside the timed statement.
Formatting, Spark-root Clippy and 670 selected tests passed.

The [pinned comparison](https://github.com/mag1cfrog/delta-arrow-reader/blob/b6d9f8f34b2095a6f4315241c0ca68aabcd51002/crates/delta-arrow-reader-spark-sql/benches/spark_only_functions_1414/from-xml-comparison.json)
used the frozen #1391 statement and the
[shared protocol](../spark_only_functions_1414/protocol.md).
The 12 processes retained 36 samples. Raw data is available locally
under `costs-168-2026-11-20/1414/from-xml`.
