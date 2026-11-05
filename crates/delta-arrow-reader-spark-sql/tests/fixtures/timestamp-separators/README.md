# Timestamp date/time separators

`timestamp-separators-spark.jsonl` records 3,348 observations from Spark 4.2.0,
revision `32f7299601108917fb01920a54e084595b7b3bf8`, Java 17.0.20.1 and
Python 3.12.14. The header stores the build properties, capture script hash,
settings and the registered `separator_inputs` table.

Spark accepts exactly one space or uppercase `T` between a complete
year-month-day date and its time. The 40 input strings cover lowercase `t`,
repeated or padded separators, tabs, other letters and separators after partial
dates. Controls retain uppercase `T`, one space, partial dates, trailing spaces,
offsets, region names, fractional and truncated nanosecond seconds, negative
years, pre-epoch values and New York's 2026 DST gap and overlap.

Each string is converted by CAST and TRY_CAST to TIMESTAMP and TIMESTAMP_NTZ,
TO_TIMESTAMP, TO_TIMESTAMP_NTZ, DATE_FORMAT, comparison with a TIMESTAMP literal
and CAST to DATE. Functions that convert STRING arguments with DataFusion's
native cast (#942) and mixed STRING/TIMESTAMP IN or GREATEST operands (#944)
do not use this parser and are outside these references.

Constant inputs use `decode(unhex(...))`; column inputs read
the registered table with a NULL row. Printable strings are also used as
TIMESTAMP, TIMESTAMP_NTZ and TIMESTAMP_LTZ typed literals. Every query runs in
both ANSI modes with UTC and `America/New_York` session zones.

The capture excludes `ConvertToLocalRelation`, so column expressions run during
execution rather than optimization. Rows come from Spark's internal rows:
TIMESTAMP values are UTC-epoch microseconds, TIMESTAMP_NTZ values are wall-clock
microseconds and DATE values are epoch days. Each record keeps the analyzed,
optimized and executed schemas, or the error condition, first message line and
failing stage.

`timestamp_separators.rs` compares every record at batch sizes 1 and 32.
DAR reports timestamp parse failures with the shared parser message instead of
Spark's error class, so an expected error must occur at the same stage with
that parser message. A DATE cast keeps its existing `[CAST_INVALID_INPUT]`
message.

Three existing differences outside the separator contract keep their Spark
expectations in the fixture. The test checks the current DAR result for each:

- #937: 218 constant conversions to DATE, TIMESTAMP or TIMESTAMP_NTZ fail while
  collecting rows instead of during optimization.
- #938: 92 ANSI DATE_FORMAT calls return NULL for strings Spark rejects.
- #939: 8 DATE_FORMAT calls print year -1 as `0002` instead of `-0001`.

At the staging base, 136 of the #937 conversions and 68 of the #938 calls
returned a wrong value because their separator was accepted. They now fail or
return NULL; only the stage or ANSI error remains different from Spark.

Recapture from the crate root:

```sh
JAVA_HOME=<JDK 17 or 21> SPARK_LOCAL_IP=127.0.0.1 python \
    tests/fixtures/timestamp-separators/capture_timestamp_separators_spark.py \
    "$(mktemp -d)/timestamp-separators-spark.jsonl"
```
