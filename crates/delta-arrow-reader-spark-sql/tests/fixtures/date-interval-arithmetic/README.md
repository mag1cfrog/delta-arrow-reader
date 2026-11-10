# DATE and interval arithmetic

`date-interval-arithmetic-spark.jsonl` records 168 observations from Spark 4.2.0, revision
`32f7299601108917fb01920a54e084595b7b3bf8`, Java 17.0.20.1 and Python 3.12.14 (#952). The
header stores the build properties, capture script hash, settings and the registered `inputs`
table. Two captures were byte-identical.

`inputs` holds eight rows of a NOT NULL INT key `k` and a nullable DATE `d`: 2020-01-01, the
month end 2020-01-31, the leap day 2020-02-29, the Los Angeles DST change days 2020-03-08 and
2020-11-01, years 1 and 9999, and NULL.

42 queries run in both ANSI modes in UTC and America/Los_Angeles:
- `d` plus or minus a day-time interval with time units: literals of HOUR, MINUTE, SECOND,
  fractional SECOND, DAY TO HOUR, DAY TO SECOND, HOUR TO MINUTE, `INTERVAL '24' HOUR` and a
  multi-unit literal; CAST of a number and of NULL to INTERVAL HOUR; MAKE_DT_INTERVAL; a
  TIMESTAMP difference; a negation; a DAY plus an HOUR interval; a DAY interval times 2 and an
  HOUR interval divided by 2; the interval first; and TRY_ADD in both orders and TRY_SUBTRACT;
- `d` plus or minus a DAY interval: literals, the interval first, a CAST, a DATE difference, a
  negation, a sum of DAY intervals, a NULL CAST, TRY_ADD in both orders and TRY_SUBTRACT, and
  differences that reach years 1 and 9999 from every row;
- `d` plus a MONTH and minus a YEAR interval;
- `d` plus an HOUR or a DAY interval column of a subquery;
- DATE literals plus or minus an HOUR or DAY interval.

Spark adds a DAY interval to a DATE with `date_add` of its day count and returns a DATE. It
casts the DATE to TIMESTAMP at its midnight in the session zone for any other day-time
interval and returns a TIMESTAMP, whose end field Spark takes from the interval's type:
`INTERVAL '24' HOUR` and a DAY interval times 2, a `DAY TO SECOND` interval, give a TIMESTAMP,
while a sum of DAY intervals gives a DATE. No query fails.

Each record keeps the analyzed, optimized and executed schemas and values read from Spark's
internal rows. `ConvertToLocalRelation` is excluded.

`date_interval_arithmetic.rs` compares names, type families with the session zone, metadata and
typed rows at batch sizes 1 and 32, and nullability in the executed schema; the analyzed and
optimized nullability is not compared (#1236). One difference is retained at DAR's exact
current result: #1250, a day-time interval column carries no qualifier, so `d` plus the HOUR
column of a subquery keeps DATE arithmetic and gives `d`, where Spark gives a TIMESTAMP (4
records).

Recapture from the crate root:

```sh
JAVA_HOME=<JDK 17 or 21> SPARK_LOCAL_IP=127.0.0.1 python \
    tests/fixtures/date-interval-arithmetic/capture_date_interval_arithmetic_spark.py \
    "$(mktemp -d)/date-interval-arithmetic-spark.jsonl"
```
