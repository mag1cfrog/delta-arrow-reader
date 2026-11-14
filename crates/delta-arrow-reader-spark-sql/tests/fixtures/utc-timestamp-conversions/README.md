# UTC timestamp conversions around DST transitions

`utc-timestamp-conversions-spark.jsonl` records 4,356 observations from Spark 4.2.0,
revision `32f7299601108917fb01920a54e084595b7b3bf8`, Java 17.0.20.1 and Python
3.12.14 (#154). The header stores the build properties, capture script hash,
settings and the registered `wall_clock_times` table. Two captures were
byte-identical.

The 726 queries run in both ANSI modes with UTC, `America/Los_Angeles` and
`America/Havana` session zones. TO_UTC_TIMESTAMP and FROM_UTC_TIMESTAMP convert 21
wall-clock times: the starts, middles and ends of the 2024 Los Angeles spring gap
and fall overlap and the microseconds around them, the UTC instants around both
Los Angeles transitions, the skipped and repeated Havana midnights of 2024-03-10 and
2024-11-03, an ordinary time, a pre-epoch fraction and a pre-1883 local-mean-time
value. Each time is a TIMESTAMP_NTZ, TIMESTAMP, STRING and DATE (its day) literal,
converted with `America/Los_Angeles`, `America/Havana`, `UTC` and `+05:30`. Column
queries read the same values from `wall_clock_times` as TIMESTAMP_NTZ, a
UTC-annotated TIMESTAMP, STRING and DATE, with literal zones and with a per-row
zone. Probes add NULL values and zones, invalid, empty and legacy (`PST`) zones, an
invalid per-row zone, a generated name and the stopped experiment's two queries
verbatim.

The capture excludes `ConvertToLocalRelation`, so column expressions run during
execution rather than optimization. Rows come from Spark's internal rows: TIMESTAMP
values are UTC-epoch microseconds. Each record keeps the analyzed, optimized and
executed schemas, or the error condition, first message line and failing stage.

`utc_timestamp_conversions.rs` compares every record at batch sizes 1 and 32. It also
checks that every executed TIMESTAMP field carries the session zone.

The remaining error-stage difference keeps its Spark expectation in the fixture.
The test replaces only that stage with the current DAR result and checks its count:

- #1025: 24 conversions with an invalid or empty constant zone fail while
  collecting rows instead of during optimization.

Offset conversions now use the original Spark expectations. The
[offset argument fixture](../utc-offset-zones/README.md) adds ordinary signed offsets,
prefixes, seconds, dynamic source/target zones and NULL/evaluation controls.

An explicit CAST from DATE to TIMESTAMP at a skipped or repeated midnight is recorded
by the [DATE to TIMESTAMP references](../date-to-timestamp/README.md) (#1027).

Recapture from the crate root:

```sh
JAVA_HOME=<JDK 17 or 21> SPARK_LOCAL_IP=127.0.0.1 python \
    tests/fixtures/utc-timestamp-conversions/capture_utc_timestamp_conversions_spark.py \
    "$(mktemp -d)/utc-timestamp-conversions-spark.jsonl"
```
