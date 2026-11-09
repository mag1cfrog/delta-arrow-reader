# WEEKDAY and DAYOFWEEK

`weekday-spark.jsonl` records 200 observations from Spark 4.2.0, revision
`32f7299601108917fb01920a54e084595b7b3bf8`, Java 17.0.20.1 and Python 3.12.14 (#1088).
The header stores the build properties, capture script hash, settings and the
registered `day_inputs`, `required_inputs` and `string_inputs` tables. Two captures were
byte-identical.

`day_inputs` holds 11 days and NULL as DATE, as TIMESTAMP at 03:30 UTC, as TIMESTAMP_NTZ at
03:30 and as `yyyy-MM-dd` STRING. The days are:
- Monday 2024-03-04 through Sunday 2024-03-10, the Los Angeles spring change;
- 0001-01-01, a Monday, and 9999-12-31, a Friday;
- 1969-12-31 and 1883-11-18, the day Los Angeles left local mean time.

Each TIMESTAMP falls on the previous day in Los Angeles. `required_inputs` holds Sunday
2024-03-10 and Monday 2024-03-11 the same ways in NOT NULL columns. `string_inputs` holds
STRING values with a time, surrounding spaces, an ISO `T` and zone, a short month and day,
`bad` and the empty string; each is queried alone.

50 queries run in both ANSI modes in UTC and America/Los_Angeles:
- WEEKDAY and DAYOFWEEK over each `day_inputs` and `required_inputs` column and each
  `string_inputs` value;
- both functions over DATE, TIMESTAMP (with and without an offset), TIMESTAMP_NTZ,
  STRING, year 1 and NULL literals, and STRING literals Spark cannot read: `bad`, the
  empty string and a lowercase `t` separator;
- a filter on Sundays and a GROUP BY of WEEKDAY.

Each record keeps the analyzed, optimized and executed schemas and values read from Spark's
internal rows, or the error condition, Java exception class, first message line and failing
stage. `ConvertToLocalRelation` is excluded.

`weekday.rs` compares names, type families, nullability, metadata and typed rows at batch
sizes 1 and 32 in each record's session zone. A failure must occur at Spark's stage with
Spark's error condition in DAR's message.

No differences are retained. The implicit cast of a NOT NULL TIMESTAMP_NTZ to DATE is
nullable, as Spark declares it: at analysis for the 8 literal records, and at every stage for
the 8 NOT NULL column records (#1003).

Recapture from the crate root:

```sh
JAVA_HOME=<JDK 17 or 21> SPARK_LOCAL_IP=127.0.0.1 python \
    tests/fixtures/weekday/capture_weekday_spark.py "$(mktemp -d)/weekday-spark.jsonl"
```
