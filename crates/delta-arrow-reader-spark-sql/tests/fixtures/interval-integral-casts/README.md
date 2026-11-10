# Interval casts to integral and DECIMAL types

`interval-integral-casts-spark.jsonl` records 1816 observations from Spark 4.2.0, revision
`32f7299601108917fb01920a54e084595b7b3bf8`, Java 17.0.20.1 and Python 3.12.14 (#1152). The
header stores the build properties, capture script hash, settings and the registered tables:
- `counts`: INT keys `k` and nullable INT counts `n`: zero, 3 and -3, the TINYINT and SMALLINT
  limits and their neighbors, the largest day count of a DAY TO SECOND interval and its
  negation, and NULL;
- `seconds`: BIGINT second counts beyond INT, the INT limit and its neighbor, and NULL;
- `fractions`: DECIMAL(10, 3) second counts with fractions, halves and a value that rounds past
  DECIMAL(4, 1), and NULL;
- `pairs`: DATE and TIMESTAMP pairs three days apart in both orders, around the Los Angeles DST
  start, from year 1 to 9999, and with a NULL;
- `spans`: a DAY TO SECOND column of 1 day 02:03:04.5, its negation, 1.5 seconds and NULL.

Each failure keeps Spark's error condition and the class and message of its innermost Java
exception. Two captures were byte-identical.

454 queries run in both ANSI modes in UTC and America/Los_Angeles:
- every year-month and day-time qualifier, built from `n` as a count of its end field, cast to
  TINYINT, SMALLINT, INT, BIGINT, DECIMAL, DECIMAL(20, 6) and DECIMAL(4, 1), and with TRY_CAST
  to TINYINT, SMALLINT, INT and DECIMAL(4, 1);
- SECOND, MINUTE TO SECOND and DAY TO SECOND intervals of `b` seconds, and SECOND, MINUTE TO
  SECOND, HOUR TO SECOND and DAY TO SECOND intervals of the `f` fractions, to each target;
- 37 literals: each qualifier, signs, `INTERVAL 3 DAYS 4 HOURS` and the other unit syntaxes,
  the largest and smallest values, a STRING cast to INTERVAL DAY, casts between qualifiers and
  NULL, to INT, BIGINT and DECIMAL(20, 6), and literals that overflow TINYINT and INT, with CAST
  and TRY_CAST;
- DATE and TIMESTAMP differences, sums and differences with DAY, HOUR and MINUTE intervals,
  TRY_ADD and TRY_SUBTRACT, negation, ABS, products and quotients, COALESCE, IF, CASE, NULLIF,
  GREATEST and LEAST, MAKE_DT_INTERVAL and MAKE_YM_INTERVAL, and YEAR interval arithmetic;
- the `spans` column;
- columns of subqueries, a common table expression, nested subqueries, SUM, MIN, MAX and AVG
  in a subquery and grouped, SUM and AVG directly, LAG, VALUES, UNION ALL, an ARRAY element and
  EXPLODE;
- qualifiers through `*` and `a.*`, an unaliased CAST in a subquery, a UNION with an untyped
  NULL on the left, a scalar subquery, MAX_BY, and a common table expression read directly by
  a set operation and again on its own; and DATE arithmetic with an HOUR or DAY column of a
  subquery or of `*`, with the `spans` column alone, plus an HOUR interval and in COALESCE, and
  with an ARRAY element, alone and plus an HOUR interval, beside a CAST of that sum; and
  chains of three set operations that end in an HOUR interval or a TIMESTAMP difference, with
  UNION ALL and EXCEPT, cast and added to a DATE, and one that reads a common table
  expression directly with a coerced column, beside another reference to it;
- DOUBLE, FLOAT and BOOLEAN targets, which Spark rejects, and unaliased names.

Spark counts the interval's end field: whole years or months, and whole days, hours, minutes or
seconds truncated toward zero, so `INTERVAL '1 2' DAY TO HOUR` is 26. A DECIMAL keeps a SECOND
end field's microseconds and rounds HALF_UP to its scale. A count beyond an integral target
fails with `CAST_OVERFLOW`, and one beyond a DECIMAL's precision with
`NUMERIC_VALUE_OUT_OF_RANGE`, in both ANSI modes; TRY_CAST gives NULL. A sum, a difference, a
conditional, VALUES and UNION widen the qualifier from the earliest start field to the latest
end field. A scaled interval, AVG, MAKE_DT_INTERVAL and a TIMESTAMP difference are `DAY TO
SECOND`, and MAKE_YM_INTERVAL is `YEAR TO MONTH`. A DECIMAL result is nullable at analysis. A
DATE plus an interval ending at DAY is a DATE, and plus one with time units a TIMESTAMP.

Each record keeps the analyzed, optimized and executed schemas and values read from Spark's
internal rows, collected through the Dataset API. `ConvertToLocalRelation` is excluded.

`interval_integral_casts.rs` compares names, type families and metadata at every stage,
nullability in the executed schema, and typed rows at batch sizes 1 and 32. The analyzed and
optimized nullability and the stage of a failure are not compared (#1236). A failure must have
Spark's first message line; over registered rows the failing value, which depends on which row
fails first, is left out. Four differences are retained, each at DAR's exact current result:
- #1236: a STRING cast to INTERVAL DAY fails in DAR's interval parser while optimizing
  (12 records);
- #1090: GREATEST and LEAST of NOT NULL intervals are nullable, so the cast of one is nullable
  where Spark's is not (16 records);
- #1236: an ARRAY element or an exploded value of a DAY interval carries no qualifier and
  counts seconds: `array(d2 - d1)[0]` overflows INT, and an exploded DAY is 86400 and nullable
  (8 records);
- #1236: a set operation whose left input reads a common table expression directly keeps that
  expression's DAY qualifier, so `TABLE c UNION ALL SELECT INTERVAL '1' HOUR` counts 3 and 0
  days where Spark counts 72 and 1 hours, also in a chain with a coerced column; another
  reference to the expression still counts 3 (8 records).

Recapture from the crate root:

```sh
JAVA_HOME=<JDK 17 or 21> SPARK_LOCAL_IP=127.0.0.1 python \
    tests/fixtures/interval-integral-casts/capture_interval_integral_casts_spark.py \
    "$(mktemp -d)/interval-integral-casts-spark.jsonl"
```
