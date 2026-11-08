# DATE and TIMESTAMP_NTZ subtraction

`date-ntz-subtraction-spark.jsonl` records 80 observations from Spark 4.2.0, revision
`32f7299601108917fb01920a54e084595b7b3bf8`, Java 17.0.20.1 and Python 3.12.14 (#1135).
The header stores the build properties, capture script hash, settings and the registered
`inputs` table. Each failure keeps Spark's error condition and the class and message of its
innermost Java exception. Two captures were byte-identical.

`inputs` holds seven rows of a TIMESTAMP_NTZ and a DATE:
- 2024-03-10 02:30, which Los Angeles skips, against that day;
- midnight against the next day;
- years 1 and 9999 against each other;
- the last microsecond of 1969 against the epoch;
- a NULL DATE, and a NULL TIMESTAMP_NTZ.

20 queries run in both ANSI modes in UTC and America/Los_Angeles:
- over `inputs`, `-` and TRY_SUBTRACT in both orders, against literals and in a lambda;
- literals in both orders, with NULLs;
- DATE values whose midnight is beyond BIGINT microseconds, made with `date_from_unix_date`,
  as literals and beside the column, with `-` and TRY_SUBTRACT.

Spark casts the DATE to TIMESTAMP_NTZ at its midnight, which it declares nullable, and returns
the DAY TO SECOND interval between the wall-clock times. Beyond BIGINT the cast fails with
`long overflow`. Spark folds the cast of a constant DATE while optimizing, so the failure
escapes TRY_SUBTRACT beside a column. An all-literal TRY_SUBTRACT folds inside TRY and gives
NULL.

Each record keeps the analyzed, optimized and executed schemas and values read from Spark's
internal rows. `ConvertToLocalRelation` is excluded.

`date_ntz_subtraction.rs` compares names, type families, nullability, metadata and typed rows
at batch sizes 1 and 32 in each record's session zone. A failure must occur at Spark's stage
with its error condition, or with the Java message of an `ArithmeticException`. No differences
are retained.

Recapture from the crate root:

```sh
JAVA_HOME=<JDK 17 or 21> SPARK_LOCAL_IP=127.0.0.1 python \
    tests/fixtures/date-ntz-subtraction/capture_date_ntz_subtraction_spark.py \
    "$(mktemp -d)/date-ntz-subtraction-spark.jsonl"
```
