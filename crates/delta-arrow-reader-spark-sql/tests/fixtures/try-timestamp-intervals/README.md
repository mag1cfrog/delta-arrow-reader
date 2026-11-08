# TRY_ADD and TRY_SUBTRACT of TIMESTAMP values and intervals

`try-timestamp-intervals-spark.jsonl` records 224 observations from Spark 4.2.0, revision
`32f7299601108917fb01920a54e084595b7b3bf8`, Java 17.0.20.1 and Python 3.12.14 (#1129).
The header stores the build properties, capture script hash, settings and the registered
`inputs` and `edges` tables. Each failure keeps Spark's error condition and the class and
message of its innermost Java exception. Two captures were byte-identical.

`inputs` holds six rows and an all-NULL row. Each row has a TIMESTAMP, a TIMESTAMP_NTZ, a
DAY TO SECOND interval and a YEAR TO MONTH interval, registered as INT months and cast in
SQL because PySpark has no YEAR TO MONTH values. In Los Angeles, the TIMESTAMP values are:
- a day before a skipped 02:30 and a repeated 01:30 in 2024;
- a month before a skipped 02:30 in 2024;
- 12:10 local mean time on 1883-11-17.

There are also the first microsecond of year 1 and the last of year 9999 in UTC. The TIMESTAMP_NTZ
values are:
- the skipped 02:30 and the repeated 01:30;
- a month end;
- 12:10 on 1883-11-17;
- years 1 and 9999.

`edges` holds the largest and smallest BIGINT microseconds and the epoch, read with
`timestamp_micros`.

56 queries run in both ANSI modes in UTC and America/Los_Angeles:
- over `inputs`:
  - TRY_ADD of TIMESTAMP and TIMESTAMP_NTZ values with DAY, DAY TO SECOND, MONTH, calendar and
    column intervals, in both argument orders;
  - TRY_SUBTRACT of the same intervals and of the smallest calendar day count;
  - TRY_SUBTRACT of TIMESTAMP, TIMESTAMP_NTZ and DATE values from each other;
  - TRY_ADD and TRY_SUBTRACT with an untyped NULL;
  - TRY_ADD in a lambda.
- 17 literal queries: the four #1129 examples, typed and untyped NULL operands, negations of
  the smallest intervals, TIMESTAMP_NTZ subtraction in a skipped hour and wrong argument
  counts.
- 6 queries over `edges`, whose overflowing rows give NULL.

Each record keeps the analyzed, optimized and executed schemas and values read from Spark's
internal rows. `ConvertToLocalRelation` is excluded. Spark fails only for the wrong argument
counts and for a column minus the smallest constant calendar interval, which it negates
while optimizing, outside TRY.

`try_timestamp_intervals.rs` compares names, type families, nullability, metadata and typed
rows at batch sizes 1 and 32 in each record's session zone. A failure must occur at Spark's
stage with its error condition. One difference is retained at DAR's exact current result:
#1133, DAR negates that constant calendar interval per row inside TRY and returns NULL for
every row (4 records).

Recapture from the crate root:

```sh
JAVA_HOME=<JDK 17 or 21> SPARK_LOCAL_IP=127.0.0.1 python \
    tests/fixtures/try-timestamp-intervals/capture_try_timestamp_intervals_spark.py \
    "$(mktemp -d)/try-timestamp-intervals-spark.jsonl"
```
