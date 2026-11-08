# TRY_MAKE_INTERVAL values, errors and nullability

`try-make-interval-spark.jsonl` records 180 observations from Spark 4.2.0, revision
`32f7299601108917fb01920a54e084595b7b3bf8`, Java 17.0.20.1 and Python 3.12.14 (#1150).
The header stores the build properties, capture script hash, settings and the registered
`inputs`, `required` and `others` tables, the same tables as the
[MAKE_INTERVAL references](../make-interval/README.md). Each failure keeps Spark's error
condition and the class and message of its innermost Java exception. Two captures were
byte-identical.

45 queries run in both ANSI modes in UTC and America/Los_Angeles:
- over the tables, every argument with an overflowing row, days alone, NOT NULL days, a
  TIMESTAMP minus TRY_MAKE_INTERVAL of a NOT NULL column, BIGINT, STRING and DOUBLE columns
  with rows that fit and rows that fail, a NULL argument before a malformed one, hours beyond
  Arrow's nanoseconds, malformed STRING seconds, TRY_ADD over overflowing, malformed and
  BIGINT arguments, and TRY_ADD and TRY_SUBTRACT of a column beside a constant malformed or
  overflowing argument;
- literals with every argument count from none to eight, negative values, NULL arguments, a
  NULL before a malformed argument, month, year and day overflows, BIGINT years beyond INT,
  DOUBLE, STRING, malformed STRING and BOOLEAN parts, seconds beyond DECIMAL(18, 6), hours
  beyond Arrow's nanoseconds, TIMESTAMP minus an overflowing and the smallest interval, and
  TRY_ADD of a malformed argument, malformed STRING seconds, and unaliased calls whose
  generated names Spark builds from the arguments and the filled-in defaults.

Spark's TRY_MAKE_INTERVAL is MAKE_INTERVAL with `failOnError = false` and one to seven
arguments: an overflow gives NULL and the result is nullable in both ANSI modes, but the
argument casts follow the ANSI mode, so under ANSI a malformed or overflowing argument fails.
Inside TRY_ADD, Spark's `TryEval` turns those failures into NULL.

Each record keeps the analyzed, optimized and executed schemas and values read from Spark's
internal rows. `ConvertToLocalRelation` is excluded.

`try_make_interval.rs` compares names, type families, nullability, metadata and typed rows at
batch sizes 1 and 32, with the error rules of `make_interval.rs`. Four differences are
retained, each at DAR's exact current result:
- #1145: 3000000 INT hours, literal and column, fail with DAR's range error while collecting
  rows, in both modes (8 records);
- #1124: under ANSI, seconds beyond DECIMAL(18, 6) fail with Arrow's precision error instead of
  `NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION`, and malformed STRING seconds with a plain cast
  error instead of `CAST_INVALID_INPUT` (6 records);
- #1133: under ANSI, TRY_ADD or TRY_SUBTRACT of a column beside a constant malformed or
  overflowing argument returns NULL, where Spark folds the cast outside TRY and fails while
  optimizing (8 records);
- #749: unaliased calls are named with the arguments as written, where Spark adds the filled-in
  defaults (8 records, each at all three stages).

Recapture from the crate root:

```sh
JAVA_HOME=<JDK 17 or 21> SPARK_LOCAL_IP=127.0.0.1 python \
    tests/fixtures/try-make-interval/capture_try_make_interval_spark.py \
    "$(mktemp -d)/try-make-interval-spark.jsonl"
```
