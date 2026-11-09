# Casts between TIMESTAMP_NTZ and DATE

`date-ntz-casts-spark.jsonl` records 644 observations from Spark 4.2.0, revision
`32f7299601108917fb01920a54e084595b7b3bf8`, Java 17.0.20.1 and Python 3.12.14 (#1003).
The header stores the build properties, capture script hash, settings and the registered
`inputs` table. Each failure keeps Spark's error condition and the class and message of its
innermost Java exception. Two captures were byte-identical.

`inputs` holds four rows of NOT NULL TIMESTAMP_NTZ `tn` and DATE `dr`, and nullable copies `n`
and `d` that are NULL in the last row: the times Los Angeles skips and repeats on its DST days
beside those days, the last microsecond before the epoch, and year 1 beside 9999-12-31.

161 queries run in both ANSI modes in UTC and America/Los_Angeles. 48 cases run over the NOT
NULL columns, the nullable columns and literals:
- CAST and TRY_CAST of a TIMESTAMP_NTZ to DATE and of a DATE to TIMESTAMP_NTZ, DATE() and
  TO_TIMESTAMP_NTZ;
- TO_DATE, DATE_ADD, DATE_SUB, DATEDIFF in both positions, LAST_DAY, YEAR, MONTH, DAY,
  DAYOFMONTH, DAYOFYEAR, QUARTER, WEEKOFYEAR, DAYNAME, MONTHNAME, ADD_MONTHS, NEXT_DAY, TRUNC,
  UNIX_DATE, WEEKDAY and DAYOFWEEK of a TIMESTAMP_NTZ;
- EXTRACT of YEAR, QUARTER, MONTH, WEEK, DAY, DAYOFWEEK, DOW_ISO, DOY, YEAROFWEEK and HOUR,
  and of the aliases Y, QTR, MONS, W, D and DAYS, DATE_PART of DAY and DATEPART of MONTH;
- a DATE compared with a TIMESTAMP_NTZ by `<` and `=`, and COALESCE of the two.

Over the columns, CASE and IF choose between a DATE and a TIMESTAMP_NTZ, and UNION ALL combines
them. TO_TIMESTAMP and TRY_TO_TIMESTAMP of the DATE columns and a literal run with the session
timestamp type TIMESTAMP_NTZ; those records name it in `timestamp_type`. Five more queries
record cases owned elsewhere: a constant cast over `WHERE false`, a DATE in an IN list of
TIMESTAMP_NTZ values, TRUNC to HOUR, a DATE column compared with a TIMESTAMP_NTZ literal, and a
TIMESTAMP_NTZ column cast to DATE compared with a DATE literal.

Spark declares a cast between TIMESTAMP_NTZ and DATE nullable whatever its input
(`Cast.forceNullable`). So each of these results over a NOT NULL column is nullable at every
stage, and over a literal until constant folding, in both modes and zones. TRY_CAST of a DATE to
TIMESTAMP_NTZ cannot fail and keeps its input's nullability, while TRY_TO_TIMESTAMP of a DATE
is that cast without TRY and is nullable. EXTRACT of HOUR reads the
TIMESTAMP_NTZ itself and keeps its nullability, and COALESCE with a NOT NULL value is NOT NULL.
UNIX_DATE takes only a DATE and rejects a TIMESTAMP_NTZ during analysis with
`DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE`. TRUNC gives NULL for the HOUR unit, and a constant
over `WHERE false` is NOT NULL once optimized. Spark's optimizer rewrites the comparisons with
a literal without the cast, so they are NOT NULL once optimized.

Each record keeps the analyzed, optimized and executed schemas and values read from Spark's
internal rows, collected through the Dataset API. `ConvertToLocalRelation` is excluded.

`date_ntz_casts.rs` compares names, type families, nullability, metadata and typed rows at
batch sizes 1 and 32. A failure must occur at Spark's stage with Spark's condition. A second
test runs the DATE to TIMESTAMP_NTZ cast, TO_TIMESTAMP_NTZ, TO_TIMESTAMP and TRY_TO_TIMESTAMP
column records over Date64 copies of the DATE columns, which Spark lacks, and requires Spark's
results. Six differences are retained:
- #1087: TRUNC of the year 1 row fails while collecting rows, where DataFusion's truncation leaves
  the nanosecond range, pinned to text DAR's error must contain (4 records);
- #1190: the constant cast over `WHERE false` stays nullable after optimization (4 records);
- #1191: the DATE in an IN list of TIMESTAMP_NTZ values is NOT NULL (4 records);
- #1192: TRUNC to HOUR gives each row's DATE where Spark gives NULL (4 records);
- #1116: EXTRACT of DAYOFWEEK, DOW_ISO and YEAROFWEEK fails while collecting rows as an
  unsupported date part, pinned to text DAR's error must contain (36 records);
- #1197: the comparisons with a literal stay nullable after optimization (8 records).

Recapture from the crate root:

```sh
JAVA_HOME=<JDK 17 or 21> SPARK_LOCAL_IP=127.0.0.1 python \
    tests/fixtures/date-ntz-casts/capture_date_ntz_casts_spark.py \
    "$(mktemp -d)/date-ntz-casts-spark.jsonl"
```
