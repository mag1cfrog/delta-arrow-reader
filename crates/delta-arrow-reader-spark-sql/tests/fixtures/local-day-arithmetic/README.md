# TIMESTAMP day arithmetic in session time zones

`local-day-arithmetic-spark.jsonl` records 684 observations from Spark 4.2.0, revision
`32f7299601108917fb01920a54e084595b7b3bf8`, Java 17.0.20.1 and Python 3.12.14 (#1093).
The header stores the build properties, capture script hash, settings and the registered
`instants` and `edges` tables. Two captures were byte-identical.

`instants` holds 18 TIMESTAMP values and NULL, each with a BIGINT quantity `n` and a
DAY TO SECOND interval `d`, some NULL. In Los Angeles the values are:
- 2024 local times a day or a month before or after a skipped 02:30 or a repeated 01:30,
  on both sides of the repeated 01:30, and just before and after the skipped hour;
- 12:10 and 12:03 local mean time (-07:52:58) on 1883-11-17, a day before Los Angeles
  repeated 12:00:00-12:07:02 to leave local mean time, and 12:03 PST on 1883-11-19;
- 1800-06-15, the last microsecond of 1969, 0001-01-01 and 9999-12-31 UTC, and a 2024
  summer instant.

`edges` holds the largest and smallest BIGINT microseconds, read with `timestamp_micros`.

114 queries run in both ANSI modes in UTC, America/Los_Angeles and +05:30:
- over the column, TIMESTAMP plus and minus DAY, DAY TO SECOND, HOUR, MINUTE, SECOND,
  YEAR TO MONTH and calendar intervals, literal and column intervals, interval plus
  TIMESTAMP, TIMESTAMPADD and DATEADD in every unit with literal and column quantities,
  three-argument DATE_ADD, SEQUENCE with HOUR, DAY, DAY TO HOUR, MONTH, calendar and
  default steps, TIMESTAMP minus TIMESTAMP, DATE and TIMESTAMP_NTZ values, every pair of
  rows, and lambdas;
- 30 literal queries: NULL operands, TIMESTAMP_NTZ, STRING and DATE operands, exact and
  overflowing BIGINT quantities, and DECIMAL, STRING and NULL quantities;
- 13 queries over `edges`, ten of which overflow.

Each record keeps the analyzed, optimized and executed schemas and values read from Spark's
internal rows. `ConvertToLocalRelation` is excluded.

`local_day_arithmetic.rs` compares names, type families, nullability, metadata and typed
rows at batch sizes 1 and 32 in each record's session zone. A TIMESTAMP result must be
annotated with the session zone. Two differences are retained, each at DAR's exact current
result:
- #1052: in Los Angeles, 9999-12-31 plus a quarter is 10000-03-31, which Java reads in PDT
  and the date library, whose rules end in 2099, in PST, an hour later (6 records);
- #952: DATE plus an HOUR or DAY TO HOUR interval stays a DATE of the DATE plus the
  interval's whole days, where Spark gives a TIMESTAMP (12 records).

Recapture from the crate root:

```sh
JAVA_HOME=<JDK 17 or 21> SPARK_LOCAL_IP=127.0.0.1 python \
    tests/fixtures/local-day-arithmetic/capture_local_day_arithmetic_spark.py \
    "$(mktemp -d)/local-day-arithmetic-spark.jsonl"
```
