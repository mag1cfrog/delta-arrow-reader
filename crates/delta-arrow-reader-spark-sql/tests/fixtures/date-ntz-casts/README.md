# Casts between TIMESTAMP_NTZ and DATE

`date-ntz-casts-spark.jsonl` records 324 observations from Spark 4.2.0, revision
`32f7299601108917fb01920a54e084595b7b3bf8`, Java 17.0.20.1 and Python 3.12.14 (#1003).
The header stores the build properties, capture script hash, settings and the registered
`inputs` table. Each failure keeps Spark's error condition and the class and message of its
innermost Java exception. Two captures were byte-identical.

`inputs` holds four rows of NOT NULL TIMESTAMP_NTZ `tn` and DATE `dr`, and nullable copies `n`
and `d` that are NULL in the last row: the times Los Angeles skips and repeats on its DST days
beside those days, the last microsecond before the epoch, and year 1 beside 9999-12-31.

27 cases run over the NOT NULL columns, the nullable columns and literals, 81 queries in both
ANSI modes in UTC and America/Los_Angeles:
- CAST and TRY_CAST of a TIMESTAMP_NTZ to DATE and of a DATE to TIMESTAMP_NTZ;
- TO_DATE, DATE_ADD, DATE_SUB, DATEDIFF in both positions, LAST_DAY, YEAR, MONTH, DAY,
  DAYOFMONTH, DAYOFYEAR, QUARTER, WEEKOFYEAR, DAYNAME, MONTHNAME, ADD_MONTHS, NEXT_DAY, TRUNC,
  UNIX_DATE, WEEKDAY and DAYOFWEEK of a TIMESTAMP_NTZ;
- a DATE compared with a TIMESTAMP_NTZ by `<` and `=`.

Spark declares a cast between TIMESTAMP_NTZ and DATE nullable whatever its input
(`Cast.forceNullable`). So each of these results over a NOT NULL column is nullable at every
stage, and over a literal until constant folding, in both modes and zones. TRY_CAST of a DATE to
TIMESTAMP_NTZ cannot fail and keeps its input's nullability. UNIX_DATE takes only a DATE and
rejects a TIMESTAMP_NTZ during analysis with `DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE`.

Each record keeps the analyzed, optimized and executed schemas and values read from Spark's
internal rows, collected through the Dataset API. `ConvertToLocalRelation` is excluded.

`date_ntz_casts.rs` compares names, type families, nullability, metadata and typed rows at
batch sizes 1 and 32. A failure must occur at Spark's stage with Spark's condition. Two
differences are retained:
- #1087: TRUNC of the year 1 row fails while collecting rows, where DataFusion converts the DATE
  to a nanosecond TIMESTAMP, pinned to text DAR's error must contain (4 records);
- #1188: UNIX_DATE casts the TIMESTAMP_NTZ to DATE and counts its days, where Spark rejects it,
  at DAR's exact rows (12 records).

Recapture from the crate root:

```sh
JAVA_HOME=<JDK 17 or 21> SPARK_LOCAL_IP=127.0.0.1 python \
    tests/fixtures/date-ntz-casts/capture_date_ntz_casts_spark.py \
    "$(mktemp -d)/date-ntz-casts-spark.jsonl"
```
