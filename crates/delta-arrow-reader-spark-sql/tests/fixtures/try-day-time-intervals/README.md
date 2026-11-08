# TRY_ADD and TRY_SUBTRACT of DAY TO SECOND intervals

`try-day-time-intervals-spark.jsonl` records 124 observations from Spark 4.2.0, revision
`32f7299601108917fb01920a54e084595b7b3bf8`, Java 17.0.20.1 and Python 3.12.14 (#1136).
The header stores the build properties, capture script hash, settings and the registered
`inputs` table. Two captures were byte-identical.

`inputs` holds seven pairs of DAY TO SECOND intervals `a` and `b`:
- a day and two hours;
- the largest and the smallest interval beside a microsecond of either sign, which overflow
  when added or subtracted;
- opposite days;
- a NULL on either side.

31 queries run in both ANSI modes in UTC and America/Los_Angeles:
- over `inputs`: TRY_ADD and TRY_SUBTRACT of the columns, with HOUR and DAY literals, with a
  literal of every DAY TO SECOND qualifier in both orders, with an untyped NULL, in a lambda
  and without aliases;
- literals: every ordered pair of the ten DAY TO SECOND qualifiers, overflow at both ends and
  at the largest DAY, HOUR and MINUTE values, typed and untyped NULLs, two untyped NULLs,
  Spark's default column names, and YEAR TO MONTH pairs for comparison.

Spark adds and subtracts the microseconds exactly, into the wider qualifier, and returns a
nullable result that is NULL on overflow. A folded non-NULL literal result is NOT NULL once
optimized. Two untyped NULLs give a NULL DOUBLE. Each record keeps the
analyzed, optimized and executed schemas and values read from Spark's internal rows.
`ConvertToLocalRelation` is excluded.

`try_day_time_intervals.rs` compares names, type families, nullability, metadata and typed
rows at batch sizes 1 and 32. Arrow keeps no interval qualifier, so every DAY TO SECOND
qualifier is one type family and every YEAR TO MONTH qualifier another. No differences are
retained.

Recapture from the crate root:

```sh
JAVA_HOME=<JDK 17 or 21> SPARK_LOCAL_IP=127.0.0.1 python \
    tests/fixtures/try-day-time-intervals/capture_try_day_time_intervals_spark.py \
    "$(mktemp -d)/try-day-time-intervals-spark.jsonl"
```
