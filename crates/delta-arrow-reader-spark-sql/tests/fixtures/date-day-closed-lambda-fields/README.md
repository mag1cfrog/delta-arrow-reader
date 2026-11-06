# DATE arithmetic with closed lambda day operands

`date-day-closed-lambda-fields-spark.jsonl` records 362 observations from Spark
4.2.0, revision `32f7299601108917fb01920a54e084595b7b3bf8`, Java 17.0.20.1 and
Python 3.12.14 (#831). The header stores the build properties, capture script hash,
settings and the registered `day_inputs` and `empty_days` tables. Two captures were
byte-identical.

The 181 queries run in both ANSI modes in UTC. DATE_ADD, DATEADD and DATE_SUB take
eleven day operands: closed higher-order STRING expressions (`element_at` over
`transform`, `filter` and comparator `array_sort`, a cast INT result, a NULL, an
invalid string and a missing index), closed INT results from `transform` and
`aggregate`, and STRING and INT literal controls. Each runs with a literal start, a
NULL start, the registered nullable DATE column, an empty table and the day before
the DATE maximum. Controls add closed higher-order results outside DATE arithmetic
(including `exists`, `forall`, comparator `array_sort` and `slice` over a lambda),
lambdas whose body ignores their parameter, failing constant lambda bodies, the
issue's reproduction verbatim, `array_sort` without a comparator and a LIMIT over a
lambda.

Spark never folds a lambda function call, even over constant arrays or with a
constant body: these results keep their analyzed nullability through optimization
and execution, and errors that depend on the call surface while collecting rows.
Constant subexpressions inside a lambda body still fold, so an invalid constant cast
there fails during optimization in ANSI mode. The capture excludes
`ConvertToLocalRelation`, so column expressions run during execution rather than
optimization. Rows come from Spark's internal rows: DATE values are epoch days.
Each record keeps the analyzed, optimized and executed schemas, or the error
condition, Java exception class, first message line and failing stage.

`date_day_closed_lambda_fields.rs` compares every record at batch sizes 1 and 32.
Existing differences keep their Spark expectation in the fixture; the test replaces
only the retained part with the current DAR result and checks each count:

- #1033: 9 ANSI calls with an out-of-bounds `element_at` index fail while collecting
  with `element_at: the index is out of bounds` instead of
  `INVALID_ARRAY_INDEX_IN_ELEMENT_AT`.
- #1038: `array_sort` without a comparator folds during optimization. In 3
  records its optimized and executed fields become nonnullable. In 1 legacy-mode
  DATE_ADD it returns day 1 where Spark rejects the day operand with
  `DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE` during analysis.
- #1039: 2 LIMIT clauses over a lambda fail during physical planning with
  `Unsupported LIMIT expression` instead of
  `INVALID_LIMIT_LIKE_EXPRESSION.IS_UNFOLDABLE` during analysis.

Recapture from the crate root:

```sh
JAVA_HOME=<JDK 17 or 21> SPARK_LOCAL_IP=127.0.0.1 python \
    tests/fixtures/date-day-closed-lambda-fields/capture_date_day_closed_lambda_fields_spark.py \
    "$(mktemp -d)/date-day-closed-lambda-fields-spark.jsonl"
```
