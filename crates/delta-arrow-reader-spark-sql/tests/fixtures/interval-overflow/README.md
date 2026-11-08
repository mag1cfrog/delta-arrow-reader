# Interval `+` and `-` overflow

`interval-overflow-spark.jsonl` records 92 observations from Spark 4.2.0, revision
`32f7299601108917fb01920a54e084595b7b3bf8`, Java 17.0.20.1 and Python 3.12.14 (#1141).
The header stores the build properties, capture script hash, settings and the registered
`inputs` table. Each failure keeps Spark's error condition and the class and message of its
innermost Java exception. Two captures were byte-identical.

`inputs` holds seven pairs of DAY TO SECOND intervals `a` and `b`. Four fit: a day and two hours,
opposite days, and a NULL on either side. Three overflow: the largest and smallest values beside
a microsecond.

23 queries run in both ANSI modes in UTC and America/Los_Angeles:
- over `inputs`, `+` and `-` of the columns that fit and that overflow, a column beside an
  overflowing literal, and a constant overflow in a CASE branch that does or does not run and
  in COALESCE after a non-NULL column;
- literals that overflow at both ends, at the largest DAY and HOUR values and the smallest DAY
  value, and that fit, with a NULL, and YEAR TO MONTH pairs that overflow and fit.

Spark adds and subtracts the stored counts exactly and fails with
`INTERVAL_ARITHMETIC_OVERFLOW.WITH_SUGGESTION` in both ANSI modes: while optimizing a constant,
while collecting rows otherwise, and not at all in a branch that does not run.

Each record keeps the analyzed, optimized and executed schemas and values read from Spark's
internal rows. `ConvertToLocalRelation` is excluded.

`interval_overflow.rs` compares names, type families, nullability, metadata and typed rows at
batch sizes 1 and 32. A failure must occur at Spark's stage with its error condition. Arrow
keeps no interval qualifier, so every DAY TO SECOND qualifier is one type family and every YEAR
TO MONTH qualifier another. No differences are retained.

Recapture from the crate root:

```sh
JAVA_HOME=<JDK 17 or 21> SPARK_LOCAL_IP=127.0.0.1 python \
    tests/fixtures/interval-overflow/capture_interval_overflow_spark.py \
    "$(mktemp -d)/interval-overflow-spark.jsonl"
```
