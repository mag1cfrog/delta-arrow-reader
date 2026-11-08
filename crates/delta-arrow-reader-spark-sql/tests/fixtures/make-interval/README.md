# MAKE_INTERVAL values, errors and nullability

`make-interval-spark.jsonl` records 392 observations from Spark 4.2.0, revision
`32f7299601108917fb01920a54e084595b7b3bf8`, Java 17.0.20.1 and Python 3.12.14 (#1131, #1148).
The header stores the build properties, capture script hash, settings and the registered
`inputs`, `required`, `others` and `ordered` tables. Each failure keeps Spark's error condition
and the class and message of its innermost Java exception. Two captures were byte-identical.

`inputs` holds INT years, months, weeks, days, hours and minutes and DECIMAL(18, 6) seconds:
positive and negative values, a NULL year, NULL seconds, and years and months whose month
count overflows. `required` holds NOT NULL INT values. `others` holds BIGINT, DOUBLE, STRING,
DECIMAL(12, 2) and INT values that fit, one row whose values overflow INT or DECIMAL(18, 6),
are malformed, are NULL, or give hours beyond Arrow's nanoseconds, and a NULL row. `ordered`
holds NOT NULL rows that fail in different arguments: BIGINT hours beyond INT and years whose
months overflow in row 0, malformed STRING years in row 2, malformed STRING seconds in row 4
and DECIMAL years beyond INT in row 6, with succeeding odd rows. Every failing row is even, so
at batch size 1 the failing rows are in the same input partition.

98 queries run in both ANSI modes in UTC and America/Los_Angeles:
- over the tables, every argument, days alone, the overflowing row, NOT NULL arguments, a
  TIMESTAMP minus MAKE_INTERVAL of a NOT NULL column, BIGINT, DOUBLE, STRING and DECIMAL
  columns as parts and as seconds with rows that fit and rows that fail, a NULL argument
  before and after a failing one, a NULL argument beside a failing constant, hours beyond
  Arrow's nanoseconds, MAKE_INTERVAL inside TRY_ADD, TRY_SUBTRACT, TRY_MULTIPLY and
  TRY_DIVIDE with overflows, invalid STRING and BIGINT parts, a constant overflow and hours
  beyond Arrow's nanoseconds, TRY_ADD of two numbers over an overflowing MAKE_INTERVAL, an
  unreached CASE branch with a constant overflow, and over `ordered`, failures in different
  arguments and rows, a later argument failing in an earlier row, and the arithmetic
  overflowing in an earlier row than an argument cast;
- literals with every argument count from none to eight, negative and fractional values, NULL
  arguments before and after malformed or overflowing ones, month and day overflows, BIGINT
  years and hours beyond INT, hours and seconds beyond Arrow's nanoseconds, a TIMESTAMP minus
  MAKE_INTERVAL, TRY_ADD, TRY_SUBTRACT, TRY_MULTIPLY and TRY_DIVIDE of overflowing and
  malformed arguments and of hours beyond Arrow's nanoseconds, TINYINT, SMALLINT, BIGINT,
  DECIMAL, DOUBLE, NaN, STRING, empty and malformed STRING, BOOLEAN and DATE parts, and INT,
  BIGINT, DOUBLE, half-microsecond DOUBLE, NaN, FLOAT, rounded DECIMAL, wide DECIMAL, STRING,
  malformed STRING and BOOLEAN seconds.

Spark casts each part to INT and the seconds to DECIMAL(18, 6) with its casts for the ANSI
mode, evaluates the arguments in order up to the first NULL, adds them with Java's exact
arithmetic, and fails with `ARITHMETIC_OVERFLOW` under ANSI or returns NULL otherwise. Under
ANSI the result is nullable only for a nullable argument or cast; otherwise it is always
nullable. A NULL literal argument gives NULL whatever the other arguments are. TRY_ADD,
TRY_SUBTRACT, TRY_MULTIPLY and TRY_DIVIDE with a non-numeric operand give NULL where
MAKE_INTERVAL or its casts fail; over two numbers the failure remains.

Each record keeps the analyzed, optimized and executed schemas and values read from Spark's
internal rows. `ConvertToLocalRelation` is excluded.

`make_interval.rs` compares names, type families, nullability, metadata and typed rows at
batch sizes 1 and 32. A failure must occur at Spark's stage. MAKE_INTERVAL's own failures
must have Spark's whole message. Analysis failures need only the condition, because Spark's
message quotes its plan. Failures of the argument casts need only the condition, because the
shared casts report it over their own cause (#1147 for INT casts, #1124 for DECIMAL casts).
A second test runs the unreached CASE branch over an empty batch. Three differences are
retained, each at DAR's exact current result:
- #1145: a result whose microseconds exceed Arrow's nanoseconds, from INT hours, large
  seconds, or outside ANSI a BIGINT hour count that wraps to an INT, fails with DAR's range
  error, while optimizing a constant under ANSI outside TRY and while collecting rows
  otherwise (24 records);
- #1124: under ANSI, DECIMAL, BIGINT and DOUBLE seconds beyond DECIMAL(18, 6) fail with
  Arrow's precision error instead of `NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION`, and
  malformed STRING seconds with a plain cast error instead of `CAST_INVALID_INPUT`, also in
  the first failing row of `ordered` (12 records);
- #1133: under ANSI, TRY_ADD or TRY_SUBTRACT of a column and a constant overflowing
  MAKE_INTERVAL returns NULL, where Spark folds the constant outside TRY and fails while
  optimizing (4 records).

Recapture from the crate root:

```sh
JAVA_HOME=<JDK 17 or 21> SPARK_LOCAL_IP=127.0.0.1 python \
    tests/fixtures/make-interval/capture_make_interval_spark.py \
    "$(mktemp -d)/make-interval-spark.jsonl"
```
