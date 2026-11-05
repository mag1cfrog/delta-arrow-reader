# Interval qualifiers and generated names

`interval-qualifiers-spark.jsonl` records 480 observations from Spark 4.2.0,
revision `32f7299601108917fb01920a54e084595b7b3bf8`, in UTC and both ANSI
modes. The header stores the build properties, Java and Python versions,
capture script hash, settings and the registered `inputs` table.

The query corpus is the original #749 capture from candidate commit `c54d99c3`
(script SHA256 `12cc8fd6...`, 432 observations) plus 24 review probes, 48
observations in both ANSI modes: eleven NULL operands of interval multiplication
and TIMESTAMP, TIMESTAMP_NTZ and interval addition or subtraction, nine signs
and boundaries written inside literals, and four quoted unit strings with zero,
cancelled or mixed units. It covers all 13 year-month and day-time field ranges,
zero, negated and signed literals, unit-syntax and cancelled literals,
arithmetic, timestamp operand order, interval CASTs and TIMESTAMP/DATE RANGE
offsets. Every original record matches the original capture's SQL, public
schema, error condition and row count. The original capture read values through
string casts. This capture reads them from Spark's internal rows: YEAR-MONTH
months, DAY-TIME microseconds, TIMESTAMP UTC microseconds and calendar intervals
as months, days and microseconds. `ConvertToLocalRelation` is excluded, so
registered-input expressions run during execution. Each record keeps the
analyzed, optimized and executed schemas, or the error condition, first message
line and failing stage.

`interval_qualifiers.rs` compares names, nullability, metadata, typed rows and
errors at batch sizes 1 and 32. Spark's own Arrow conversion maps every
day-time range to `Duration(Microsecond)` and every year-month range to
`Interval(YearMonth)`, so types are compared at that level and interval fields
are checked through generated names.

Existing differences outside the name contract keep their Spark expectations.
The test checks the current DAR result for each:

- #781: 24 year-month scaling queries fail during planning.
- #782: 42 day-time negation and division results have a nullable public field.
- #765: 14 narrowing interval CASTs keep the source value.
- #727: 12 unaliased RANGE windows keep the name `SUM(v)`.
- #947: 10 RANGE frames with DATE interval or TIMESTAMP numeric offsets return a
  running sum.

Recapture from the crate root:

```sh
JAVA_HOME=<JDK 17 or 21> SPARK_LOCAL_IP=127.0.0.1 python \
    tests/fixtures/interval-qualifiers/capture_interval_qualifiers_spark.py \
    "$(mktemp -d)/interval-qualifiers-spark.jsonl"
```
