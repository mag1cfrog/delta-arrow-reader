# MAKE_YM_INTERVAL values, errors and nullability

`make-ym-interval-spark.jsonl` records 308 observations from Spark 4.2.0, revision
`32f7299601108917fb01920a54e084595b7b3bf8`, Java 17.0.20.1 and Python 3.12.14 (#1159).
The header stores the build properties, capture script hash, settings and the registered
`nullable`, `required` and `empty` tables of INT keys `k`, INT years `y` and months `mo`, BIGINT
`b` and STRING `t`, nullable in `nullable` and NOT NULL in the others. Each failure keeps
Spark's error condition and the class and message of its innermost Java exception. Two captures
were byte-identical.

77 queries run in both ANSI modes in UTC and America/Los_Angeles:
- over the tables, years and months, years alone and months alone over nullable and NOT NULL
  columns and an empty table; rows at the YEAR TO MONTH limits and one month past them; BIGINT
  arguments that fit, wrap past INT or overflow the interval; STRING months that fit, carry
  spaces, are malformed, empty or beyond INT; TINYINT and SMALLINT arguments; NULL and typed
  NULL literals beside a column, also beside a STRING column, and a computed NULL beside a
  STRING column; a NULL year beside an overflowing
  literal month, a malformed STRING month or a BIGINT month beyond INT, and those failing years
  beside a NULL month; a malformed month in one row and a malformed year in a later one; INT()
  of a STRING column; and a comparison, a negation, IS NULL and TRY_ADD over the result;
- literals with none, one and two arguments, negative values, both limits and one month past
  them, overflowing years, NULL arguments, an overflowing year beside a NULL, a malformed STRING
  beside a NULL literal, a cast NULL or a computed NULL, STRING, malformed and empty STRING,
  BIGINT, BIGINT beyond INT, TINYINT and SMALLINT, DECIMAL, DOUBLE and BOOLEAN arguments, a
  comparison, and TRY_ADD, TRY_SUBTRACT, TRY_MULTIPLY, TRY_DIVIDE and TRY_ADD to a DATE over an
  overflow;
- unaliased calls, whose generated names Spark builds from the arguments and the defaulted
  months.

Spark's MAKE_YM_INTERVAL casts its arguments to INT and computes `years * 12 + months` with
Java's exact arithmetic, failing with `INTERVAL_ARITHMETIC_OVERFLOW.WITHOUT_SUGGESTION` in
both ANSI modes, while optimizing for a constant. NullPropagation replaces a call with a NULL
argument, as a literal, a cast of one or a computed NULL, by NULL before either argument is
evaluated. Otherwise the years are evaluated
first, and a NULL year gives NULL without evaluating the months. The result is nullable only for
a nullable argument; a STRING, DECIMAL or DOUBLE argument's cast is nullable during analysis in
both modes. TRY arithmetic's `TryEval` turns an overflow into NULL.

Each record keeps the analyzed, optimized and executed schemas and values read from Spark's
internal rows, collected through the Dataset API. `ConvertToLocalRelation` is excluded.

`make_ym_interval.rs` compares names, type families, nullability, metadata and typed rows at
batch sizes 1 and 32. Every record runs twice: over the recorded STRING columns and over the
same columns dictionary-encoded with UInt16 keys, as Delta's provider reads STRING partition
columns. A failure must occur at Spark's stage:
- an analysis failure must have Spark's condition;
- an argument cast failure must have Spark's condition and name Spark's failing value; the rest
  of its message differs (#1147);
- any other failure must have the first line of Spark's message, without its query context.

Six differences are retained, each at DAR's exact current result:
- #1165: DECIMAL and DOUBLE arguments, which Spark casts to INT, and a BOOLEAN argument, which
  Spark rejects with `DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE`, fail while planning with DAR's
  admission error (12 records);
- #749: an unaliased call is named with the arguments as written, where Spark adds the
  defaulted months (4 records, each at all three stages);
- #1166: MAKE_YM_INTERVAL of INT() over NOT NULL STRING values is NOT NULL, where Spark's is
  nullable, and IS NULL over it folds to false for a malformed value, where Spark gives true
  outside ANSI and fails under ANSI. Over dictionary-encoded columns Arrow's cast also fails on
  a malformed value in a row the filter removed (8 records);
- #1151: TRY_ADD, TRY_SUBTRACT, TRY_MULTIPLY and TRY_DIVIDE over an overflowing
  MAKE_YM_INTERVAL fail with the overflow, while optimizing a constant and while collecting rows
  otherwise, where Spark gives NULL (24 records);
- #1157: under ANSI, a constant call with a malformed STRING beside `NULL + 1` or an explicit
  cast of a NULL STRING to INT fails while optimizing, where Spark gives NULL (4 records);
- #1148: under ANSI at batch size 32, a malformed year in row 3 is reported before row 0's
  malformed month, which Spark and DAR at batch size 1 report (2 records).

Recapture from the crate root:

```sh
JAVA_HOME=<JDK 17 or 21> SPARK_LOCAL_IP=127.0.0.1 python \
    tests/fixtures/make-ym-interval/capture_make_ym_interval_spark.py \
    "$(mktemp -d)/make-ym-interval-spark.jsonl"
```
