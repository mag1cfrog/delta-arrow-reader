# Interval qualifiers and generated names

`interval-qualifiers-spark.jsonl` records 1546 observations from Spark 4.2.0,
revision `32f7299601108917fb01920a54e084595b7b3bf8`, in UTC and both ANSI
modes. The header stores the build properties, Java and Python versions,
capture script hash, settings and the registered `inputs`, `scale_inputs`,
`scale_limits`, `dt_scale_inputs` and `dt_scale_limits` tables.

The query corpus is the original #749 capture from candidate commit `c54d99c3`
(script SHA256 `12cc8fd6...`, 432 observations) plus 24 review probes, 48
observations in both ANSI modes: eleven NULL operands of interval multiplication
and TIMESTAMP, TIMESTAMP_NTZ and interval addition or subtraction, nine signs
and boundaries written inside literals, and four quoted unit strings with zero,
cancelled or mixed units. The #951 probes add 67 more, 134 observations: every
year-month and day-time qualifier at its minimum and maximum and one unit past
each (the SECOND maximum is the earlier signed-literal probe), minimum literals
with the sign outside the string, in unit syntax and in quoted unit strings, and
the minimum qualified strings cast to their own type. The #781 probes add 241
more, 482 observations, for year-month intervals multiplied in both operand
orders and divided by numbers: 48 constant numbers covering every Spark numeric
type, signs, rounding ties, DECIMAL digits, the storage limits, overflow, zero
and negative zero divisors, NaN, infinities and NULLs; five NULL intervals and
NULL operands beside failing ones; three failing constants in filtered, unused,
LIMIT 0, VALUES and CASE contexts; 42 queries over the two scaling tables,
whose rows hold intervals, numbers of every type, NULLs and limits; and 26
queries with a failing constant operand beside a column operand, in each operand
position with and without a false filter, or with NULL intervals that skip a
number failing for their row. The #985 probes add 225 more, 450 observations,
for day-time intervals in the same shapes: 42 constant numbers, including the
microsecond storage limits, a widened FLOAT tie and a DAY TO SECOND literal;
five NULL probes; the three failing constants in eight contexts; 44 queries over
the two day-time tables, whose interval columns hold exact microsecond counts,
including untyped NULL numbers; and 26 failing-operand and NULL-skip queries.
The corpus covers all 13 field ranges, zero, negated and signed literals, unit-syntax and cancelled
literals, arithmetic, timestamp operand order, interval CASTs and TIMESTAMP/DATE
RANGE offsets. Every original record matches the original capture's SQL, public
schema, error condition and row count. The original capture read values through
string casts. This capture reads them from Spark's internal rows: YEAR-MONTH
months, DAY-TIME microseconds, TIMESTAMP UTC microseconds and calendar intervals
as months, days and microseconds. `ConvertToLocalRelation` is excluded, so
registered-input expressions run during execution. Each record keeps the
analyzed, optimized and executed schemas, or the error condition, first message
line and failing stage. Where Spark raises a plain Java exception without a
condition, the record keeps its class and message instead, and a job that fails
with one records its innermost cause, because PySpark leaves that error
unconverted with a first line naming a py4j object id.

`interval_qualifiers.rs` compares names, nullability, metadata, typed rows and
errors at batch sizes 1 and 32. DAR must fail at Spark's stage and name Spark's
condition as `[CONDITION]`, or report the plain exception's message. Spark's own Arrow conversion maps every
day-time range to `Duration(Microsecond)` and every year-month range to
`Interval(YearMonth)`, so types are compared at that level and interval fields
are checked through generated names.

The 12 year-month inline VALUES scaling-error observations fail during analysis
with `INVALID_INLINE_TABLE.FAILED_SQL_EXPRESSION_EVALUATION`, retaining their
underlying arithmetic error.

Existing differences outside the name contract keep their Spark expectations.
The test checks the current DAR result for each:

- #782: 20 day-time negation results have a nullable public field.
- #765: 14 narrowing interval CASTs keep the source value.
- #727: 12 unaliased RANGE windows keep the name `SUM(v)`.
- #947: 10 RANGE frames with DATE interval or TIMESTAMP numeric offsets return a
  running sum.
- #955: 54 literals that Spark rejects at or past a storage limit fail at
  Spark's stage without Spark's error condition. Spark raises a plain
  `java.lang.ArithmeticException` for the multi-field day-time ones, and rejects
  the minimum `INTERVAL -9223372036854775808 MICROSECONDS` because unit syntax
  reads the magnitude before the sign.
- #722: 12 failing day-time scaling constants in inline VALUES fail while optimizing with
  their own error, which Spark wraps in
  `INVALID_INLINE_TABLE.FAILED_SQL_EXPRESSION_EVALUATION` during analysis.
- #979: 12 ANSI scaling queries with a `1 / 0` number fail while optimizing with
  `Arrow error: Divide by zero error` instead of Spark's `DIVIDE_BY_ZERO`.

Recapture from the crate root:

```sh
JAVA_HOME=<JDK 17 or 21> SPARK_LOCAL_IP=127.0.0.1 python \
    tests/fixtures/interval-qualifiers/capture_interval_qualifiers_spark.py \
    "$(mktemp -d)/interval-qualifiers-spark.jsonl"
```
