# TIMESTAMP calendar range

`timestamp-calendar-range-spark.jsonl` records 104 observations from Spark 4.2.0, revision
`32f7299601108917fb01920a54e084595b7b3bf8`, Java 17.0.20.1 and Python 3.12.14 (#1087). The
header stores the build properties, capture script hash, settings and the registered `inputs`
table. Two captures were byte-identical.

`inputs` holds eight rows of a NOT NULL INT key `k`, two nullable TIMESTAMP columns `ts` and
`ts2`, and a nullable DATE `d`:
- 0001-01-02 and 9999-12-31 23:59:59.999999 UTC, each beside the other;
- one second outside and one second inside each limit of Arrow's nanosecond range,
  1677-09-21 00:12:43.145224192 to 2262-04-11 23:47:16.854775807;
- 1800-06-15 and 1883-11-18, in Los Angeles local mean time (-07:52:58);
- 03:30 PDT on 2024-03-10, and 01:30 on 2024-11-03 in PDT and in PST;
- NULL in every column.

26 queries run in both ANSI modes in UTC and America/Los_Angeles:
- DATE_TRUNC of `ts` to YEAR, YYYY, YY, QUARTER, MONTH, MM, MON, WEEK, DAY, DD, HOUR, MINUTE,
  SECOND, MILLISECOND and MICROSECOND, and to DAY of a struct field;
- MONTHS_BETWEEN of `ts` and `ts2`, rounded and not, of `ts` and a TIMESTAMP literal, and of
  `d` and `ts`;
- WEEKOFYEAR of `ts` and of `d`;
- DATE_TRUNC, MONTHS_BETWEEN and WEEKOFYEAR of literals in years 1 and 9999.

Spark returns a value for every record. DOUBLE values are recorded as the 16 hex digits of
their IEEE 754 bits.

Each record keeps the analyzed, optimized and executed schemas and values read from Spark's
internal rows. `ConvertToLocalRelation` is excluded.

`timestamp_calendar_range.rs` compares names, type families with the session zone, metadata
and typed rows at batch sizes 1 and 32, and nullability in the executed schema; the analyzed
and optimized nullability is not compared (#1236). One difference is retained, at DAR's exact
values: #1052, the date library's Los Angeles rules end DST in 2099 where Java's recur, so the
quarter of 9999-12-31 starts at 9999-10-01 00:00 PST, an hour after Spark's PDT, and
MONTHS_BETWEEN reads the time of day of 2262-04-11 an hour earlier (10 rows in Los Angeles).

Recapture from the crate root:

```sh
JAVA_HOME=<JDK 17 or 21> SPARK_LOCAL_IP=127.0.0.1 python \
    tests/fixtures/timestamp-calendar-range/capture_timestamp_calendar_range_spark.py \
    "$(mktemp -d)/timestamp-calendar-range-spark.jsonl"
```
