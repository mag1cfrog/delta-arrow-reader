# Ordering and comparing calendar intervals

`calendar-interval-ordering-spark.jsonl` records 152 observations from Spark 4.2.0, revision
`32f7299601108917fb01920a54e084595b7b3bf8`, Java 17.0.20.1 and Python 3.12.14 (#1149).
The header stores the build properties, capture script hash, settings and the registered
`required` table of NOT NULL INT keys `k` and day counts `d`. Each failure keeps Spark's error
condition and the class and message of its innermost Java exception. Two captures were
byte-identical.

38 queries run in both ANSI modes in UTC and America/Los_Angeles over calendar intervals built
by `make_interval(0, 0, 0, d)`:
- ORDER BY an expression, DESC NULLS LAST, an alias, an ordinal, a constant, a second key, an
  ARRAY and a STRUCT holding an interval, SORT BY, and window ORDER BY with and without
  PARTITION BY;
- `<`, `>=`, `=`, `<>`, `<=>`, BETWEEN, IN, NOT IN, a WHERE comparison, constant comparisons,
  GREATEST and LEAST;
- MAX, MIN, MAX_BY, SORT_ARRAY, ARRAY_SORT and ARRAY_MAX, and GROUP BY and DISTINCT;
- the same ORDER BY, window ORDER BY and comparisons over DAY TO SECOND and YEAR TO MONTH
  intervals from `make_dt_interval(d)` and `make_ym_interval(d)`, whose ordered rows are
  recorded in Spark's order.

Spark rejects every ordering and comparison of a calendar interval during analysis with
`DATATYPE_MISMATCH.INVALID_ORDERING_TYPE`, in both ANSI modes. GROUP BY and DISTINCT are
allowed. DAY TO SECOND and YEAR TO MONTH intervals order and compare.

Each record keeps the analyzed, optimized and executed schemas and values read from Spark's
internal rows, collected through the Dataset API. `ConvertToLocalRelation` is excluded.

`calendar_interval_ordering.rs` compares names, type families, nullability, metadata and typed
rows at batch sizes 1 and 32, keeping row order where Spark's record does. An analysis failure
must have Spark's condition. Two differences are retained, each at DAR's exact current result:
- #1160: MAX, MIN, MAX_BY, SORT_ARRAY, ARRAY_SORT and ARRAY_MAX accept calendar intervals and
  return DAR's rows, where Spark rejects them (24 records);
- #1159: `make_ym_interval(d) = INTERVAL '1' YEAR` is nullable where Spark declares it NOT NULL
  (4 records).

Recapture from the crate root:

```sh
JAVA_HOME=<JDK 17 or 21> SPARK_LOCAL_IP=127.0.0.1 python \
    tests/fixtures/calendar-interval-ordering/capture_calendar_interval_ordering_spark.py \
    "$(mktemp -d)/calendar-interval-ordering-spark.jsonl"
```
