# MAKE_INTERVAL values, errors and nullability

`make-interval-spark.jsonl` records 256 observations from Spark 4.2.0, revision
`32f7299601108917fb01920a54e084595b7b3bf8`, Java 17.0.20.1 and Python 3.12.14 (#1131).
The header stores the build properties, capture script hash, settings and the registered
`inputs`, `required` and `others` tables. Each failure keeps Spark's error condition and the
class and message of its innermost Java exception. Two captures were byte-identical.

`inputs` holds INT years, months, weeks, days, hours and minutes and DECIMAL(18, 6) seconds:
positive and negative values, a NULL year, NULL seconds, and years and months whose month
count overflows. `required` holds NOT NULL INT values. `others` holds BIGINT, DOUBLE, STRING
and DECIMAL(12, 2) values that fit, one row whose values overflow INT or DECIMAL(18, 6) or
are malformed, and a NULL row.

64 queries run in both ANSI modes in UTC and America/Los_Angeles:
- over the tables, every argument, days alone, the overflowing row, NOT NULL arguments, a
  TIMESTAMP minus MAKE_INTERVAL of a NOT NULL column, and BIGINT, DOUBLE, STRING and DECIMAL
  columns as parts and as seconds, with rows that fit and rows that fail;
- literals with every argument count from none to eight, negative and fractional values,
  NULL arguments, month, day and microsecond overflows, a BIGINT hour count that wraps to an
  INT beyond Arrow's nanoseconds, a TIMESTAMP minus MAKE_INTERVAL, TINYINT, SMALLINT,
  BIGINT, DECIMAL, DOUBLE, NaN, STRING, empty and malformed STRING, BOOLEAN and DATE parts,
  and INT, BIGINT, DOUBLE, half-microsecond DOUBLE, NaN, FLOAT, rounded DECIMAL, wide
  DECIMAL, STRING, malformed STRING and BOOLEAN seconds.

Spark casts each part to INT and the seconds to DECIMAL(18, 6) with its casts for the ANSI
mode, adds them with Java's exact arithmetic, and fails with `ARITHMETIC_OVERFLOW` under ANSI
or returns NULL otherwise. Under ANSI the result is nullable only for a nullable argument or
cast; otherwise it is always nullable.

Each record keeps the analyzed, optimized and executed schemas and values read from Spark's
internal rows. `ConvertToLocalRelation` is excluded.

`make_interval.rs` compares names, type families, nullability, metadata and typed rows at
batch sizes 1 and 32. A failure must occur at Spark's stage. MAKE_INTERVAL's own failures
must have Spark's whole message. Analysis failures need only the condition, because Spark's
message quotes its plan. Failures of the argument casts need only the condition, because
their messages belong to the shared casts (#813, #1124). Two differences are retained, each
at DAR's exact current result:
- #1145: outside ANSI, a BIGINT hour count that wraps to an INT gives about -6.2e18
  microseconds, beyond Arrow's nanoseconds, so DAR fails while collecting rows (4 records);
- #1124: under ANSI, DECIMAL, BIGINT and DOUBLE seconds beyond DECIMAL(18, 6) fail with
  Arrow's precision error instead of `NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION`, and
  malformed STRING seconds with a plain cast error instead of `CAST_INVALID_INPUT`
  (10 records).

Recapture from the crate root:

```sh
JAVA_HOME=<JDK 17 or 21> SPARK_LOCAL_IP=127.0.0.1 python \
    tests/fixtures/make-interval/capture_make_interval_spark.py \
    "$(mktemp -d)/make-interval-spark.jsonl"
```
