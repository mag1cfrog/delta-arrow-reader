# MAKE_YM_INTERVAL values, errors and nullability

`make-ym-interval-spark.jsonl` records 460 observations from Spark 4.2.0, revision
`32f7299601108917fb01920a54e084595b7b3bf8`, Java 17.0.20.1 and Python 3.12.14 (#1159, #1165,
#1148).
The header stores the build properties, capture script hash, settings and the registered
tables:
- `nullable`, `required` and `empty`: INT keys `k`, INT years `y` and months `mo`, BIGINT `b` and
  STRING `t`, nullable in `nullable` and NOT NULL in the others;
- `numbers` and `numbers_required`: INT keys `k`, DECIMAL(12, 2) `dc`, DOUBLE `f` and FLOAT `fl`,
  nullable in `numbers` and NOT NULL in `numbers_required`;
- `wide_decimals`: INT keys `k` and NOT NULL DECIMAL(38, 17) `w` values just short of the next
  integer away from zero, which a DOUBLE would round to it;
- `ordered`: INT keys `k`, INT years `y`, STRING months `t` and BIGINT `b`, NOT NULL. The
  months overflow in row 0 and are malformed in row 2, and `b` is beyond INT in rows 0 and 2 with
  different values (#1148). Every failing row is even, so at batch size 1 the failing rows are in
  the same input partition, which is read in order.

Each failure keeps Spark's error condition and the class and message of its innermost Java
exception. Two captures were byte-identical.

115 queries run in both ANSI modes in UTC and America/Los_Angeles:
- over the tables, years and months, years alone and months alone over nullable and NOT NULL
  columns and an empty table; rows at the YEAR TO MONTH limits and one month past them; BIGINT
  arguments that fit, wrap past INT or overflow the interval; STRING months that fit, carry
  spaces, are malformed, empty or beyond INT; TINYINT and SMALLINT arguments; NULL and typed
  NULL literals beside a column, also beside a STRING column, and `NULL + 1` and `nullif(1, 1)`
  beside a STRING column; a NULL year beside an overflowing literal month, a malformed STRING
  month or a BIGINT month beyond INT, and those failing years beside a NULL month; a malformed
  month in row 0 and a malformed year in row 2, and over `ordered` an overflow in row 0 before
  a malformed month in row 2, and BIGINT months beyond INT in rows 0 and 2 (#1148); INT() of a
  STRING column; and a comparison, a negation, IS NULL and TRY_ADD over the result;
- over the number tables, DECIMAL, DOUBLE and FLOAT arguments with fractions, signs and NULL,
  values beyond INT, DECIMAL years that overflow the interval, NaN and both infinities, a NULL
  DECIMAL year beside a DOUBLE month beyond INT and the reverse, NOT NULL arguments, a
  comparison and TRY_ADD over the result, and the wide DECIMALs;
- literals with none, one and two arguments, negative values, both limits and one month past
  them, overflowing years, NULL arguments, an overflowing year beside a NULL, a malformed STRING
  beside a NULL literal, a cast NULL, `NULL + 1`, `nullif(1, 1)` or `IF(false, 1, NULL)`,
  STRING, malformed and empty STRING, BIGINT, BIGINT beyond INT, TINYINT and SMALLINT, DECIMAL,
  DOUBLE and FLOAT arguments, negative DECIMALs, DECIMAL and DOUBLE beyond INT, a whole
  DECIMAL beyond INT, a FLOAT of 2^31, a NaN DOUBLE, and DECIMALs near 1, near the INT limit and
  near the years limit; BOOLEAN, DATE, TIMESTAMP, BINARY, ARRAY and interval arguments; a
  comparison; TRY_ADD, TRY_SUBTRACT, TRY_MULTIPLY, TRY_DIVIDE and TRY_ADD to a DATE over an
  overflow; and TRY_ADD over a DOUBLE beyond INT;
- unaliased calls, whose generated names Spark builds from the arguments and the defaulted
  months.

Spark's MAKE_YM_INTERVAL casts any number or STRING argument to INT and computes
`years * 12 + months` with Java's exact arithmetic. The overflow fails with
`INTERVAL_ARITHMETIC_OVERFLOW.WITHOUT_SUGGESTION` in both ANSI modes, while optimizing for a
constant. Other argument types fail during analysis with
`DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE`.

The casts are Spark's CAST rules:
- DECIMAL, FLOAT and DOUBLE truncate toward zero, except that under ANSI a DECIMAL just short of
  the next integer away from zero moves to it, as through a DOUBLE.
- Under ANSI a value beyond INT, NaN or an infinity fails with `CAST_OVERFLOW`. Otherwise a
  DECIMAL wraps, a FLOAT or DOUBLE saturates and NaN gives 0.
- The STRING, DECIMAL, FLOAT and DOUBLE casts are nullable during analysis in both modes.

NullPropagation replaces a call with a NULL argument, as a literal, a cast of one or an
expression over a NULL literal such as `NULL + 1`, by NULL before either argument is evaluated.
A NULL from NULLIF or IF is not replaced in a constant call, so a malformed constant STRING
beside it fails under ANSI; beside a column, it folds to NULL first and the call gives NULL.
Otherwise the years are evaluated first, and a NULL year gives NULL without evaluating the
months. The result is nullable only for a nullable argument. TRY arithmetic's `TryEval` turns
an overflow into NULL.

Each record keeps the analyzed, optimized and executed schemas and values read from Spark's
internal rows, collected through the Dataset API. `ConvertToLocalRelation` is excluded.

`make_ym_interval.rs` compares names, type families, nullability, metadata and typed rows at
batch sizes 1 and 32. Every record runs twice: over the recorded STRING columns and over the
same columns dictionary-encoded with UInt16 keys, as Delta's provider reads STRING partition
columns. A failure must occur at Spark's stage:
- an analysis failure must have Spark's condition;
- an argument cast failure must have Spark's condition and name Spark's failing value, as DAR
  prints it: a STRING as written, a BIGINT's digits, a DECIMAL's integer part, and a DOUBLE or
  FLOAT as Rust prints an f64 or f32; the rest of its message differs (#1147);
- any other failure must have the first line of Spark's message, without its query context.

The replay records DAR's result for four retained differences.

- #749: an unaliased call is named with the arguments as written, where Spark adds the
  defaulted months (4 records, each at all three stages);
- #1151: TRY_ADD, TRY_SUBTRACT, TRY_MULTIPLY and TRY_DIVIDE over an overflowing
  MAKE_YM_INTERVAL fail with the overflow, and under ANSI TRY_ADD over a DOUBLE beyond INT fails
  with `CAST_OVERFLOW`, while optimizing a constant and while collecting rows otherwise, where
  Spark gives NULL (28 records);
- #1168: under ANSI, the DECIMALs near 1, near the INT limit and near the years limit, and the
  wide DECIMAL column, are truncated exactly, where Spark moves them to the next integer away
  from zero; where that integer is beyond a limit, Spark fails while optimizing (8 records);
- #1157: under ANSI, a constant call with a malformed STRING beside `NULL + 1` or an explicit
  cast of a NULL STRING to INT fails while optimizing, where Spark gives NULL (4 records).

Recapture from the crate root:

```sh
JAVA_HOME=<JDK 17 or 21> SPARK_LOCAL_IP=127.0.0.1 python \
    tests/fixtures/make-ym-interval/capture_make_ym_interval_spark.py \
    "$(mktemp -d)/make-ym-interval-spark.jsonl"
```
