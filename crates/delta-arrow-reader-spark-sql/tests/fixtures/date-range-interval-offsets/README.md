# DATE RANGE interval offsets

`date-range-interval-offsets-spark.jsonl` records 56 observations from Spark 4.2.0, revision
`32f7299601108917fb01920a54e084595b7b3bf8`, Java 17.0.20.1 and Python 3.12.14 (#947). The header
stores the build properties, capture script hash, settings and the registered `inputs` table.
Two captures were byte-identical.

`inputs` holds eight rows of a NOT NULL INT key `k`, a nullable DATE `d` with a gap after
2020-01-04, a NULL and the days of the 2020-03-08 Los Angeles DST change, and a nullable INT
value `v`.

14 queries run in both ANSI modes in UTC and America/Los_Angeles, each a SUM of `v` over a RANGE
frame ordered by `d`:
- with `INTERVAL 1 DAY`, `INTERVAL 2 DAYS` and `INTERVAL '3' DAY` PRECEDING, `INTERVAL 1 DAY`
  FOLLOWING, on both sides, descending, `INTERVAL 0 DAY` PRECEDING, UNBOUNDED PRECEDING to
  `INTERVAL 1 DAY` FOLLOWING, partitioned by `k % 2`, and ordered by a DATE expression of `k`;
- with the numeric offset `1 PRECEDING`;
- with `INTERVAL 24 HOURS`, `INTERVAL 12 HOURS` and `INTERVAL '1 00' DAY TO HOUR` PRECEDING.

A DAY offset moves the frame by whole days, as an INT offset does; the NULL key forms its own
frame. Spark rejects a sub-day offset type over a DATE key with RANGE_FRAME_INVALID_TYPE in both
ANSI modes.

Each record keeps the analyzed, optimized and executed schemas and values read from Spark's
internal rows, collected through the Dataset API, or the failing stage and Spark's error.
`ConvertToLocalRelation` is excluded.

`date_range_interval_offsets.rs` compares names, type families with the session zone, metadata
and typed rows at batch sizes 1 and 32, and nullability in the executed schema; the analyzed and
optimized nullability is not compared (#1236). Where Spark fails, DAR must fail; the error
condition, message and stage are not compared (#1236). One difference is retained: DAR keeps no
interval qualifier on a RANGE offset, so `INTERVAL 24 HOURS` and `INTERVAL '1 00' DAY TO HOUR`
give the one-day frame, Spark's rows for `INTERVAL 1 DAY`, where Spark rejects the offset type
(#1236, 8 records).

Recapture from the crate root:

```sh
JAVA_HOME=<JDK 17 or 21> SPARK_LOCAL_IP=127.0.0.1 python \
    tests/fixtures/date-range-interval-offsets/capture_date_range_interval_offsets_spark.py \
    "$(mktemp -d)/date-range-interval-offsets-spark.jsonl"
```
