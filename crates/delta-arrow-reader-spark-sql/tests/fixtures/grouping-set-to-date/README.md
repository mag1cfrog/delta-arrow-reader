# Formatted TO_DATE of grouping-set keys

`grouping-set-to-date-spark.jsonl` records 96 observations from Spark 4.2.0, revision
`32f7299601108917fb01920a54e084595b7b3bf8`, Java 17.0.20.1 and Python 3.12.14 (#1222). The
header stores the build properties, capture script hash, settings and the registered `inputs`
table. Two captures were byte-identical.

`inputs` holds three rows of a NOT NULL INT key `k`, NOT NULL DATE `d`, TIMESTAMP `ts` and
TIMESTAMP_NTZ `tn` columns, a nullable DATE `dn` with one NULL, and a NOT NULL STRING format
`f`. The TIMESTAMP 2024-03-10 07:30 UTC is still 2024-03-09 in Los Angeles, and the
TIMESTAMP_NTZ values are 23:59:59 and midnight.

24 queries run in both ANSI modes in UTC and America/Los_Angeles:
- TO_DATE with the format `yyyy-MM-dd` of each of `d`, `ts`, `tn` and `dn` grouped by ROLLUP,
  CUBE, GROUPING SETS with the empty set, and plain GROUP BY, and without aggregation;
- TO_DATE of `ts` with the format column `f` under ROLLUP of both, TRY_TO_DATE and
  UNIX_TIMESTAMP of `ts` under ROLLUP, and TO_DATE beside COUNT under ROLLUP.

Spark returns a NULL result in the subtotal rows, where the key is NULL, and declares the result
nullable under a grouping set; over a NOT NULL key without one it is NOT NULL in ANSI mode. It
reads a TIMESTAMP at its local date and a TIMESTAMP_NTZ, with a format, as the TIMESTAMP of the
same microseconds, so its date in Los Angeles can be the day before. No query fails.

Each record keeps the analyzed, optimized and executed schemas and values read from Spark's
internal rows. `ConvertToLocalRelation` is excluded.

`grouping_set_to_date.rs` compares names, type families with the session zone, metadata and
typed rows at batch sizes 1 and 32, and nullability in the executed schema; the analyzed and
optimized nullability is not compared (#1236).

Recapture from the crate root:

```sh
JAVA_HOME=<JDK 17 or 21> SPARK_LOCAL_IP=127.0.0.1 python \
    tests/fixtures/grouping-set-to-date/capture_grouping_set_to_date_spark.py \
    "$(mktemp -d)/grouping-set-to-date-spark.jsonl"
```
