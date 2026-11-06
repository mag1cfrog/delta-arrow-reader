# DATE arithmetic with closed lambda day operands

`date-day-closed-lambda-fields-spark.jsonl` records 314 observations from Spark
4.2.0, revision `32f7299601108917fb01920a54e084595b7b3bf8`, Java 17.0.20.1 and
Python 3.12.14 (#831). The header stores the build properties, capture script hash,
settings and the registered `day_inputs` and `empty_days` tables. Two captures were
byte-identical.

The 157 queries run in both ANSI modes in UTC. DATE_ADD, DATEADD and DATE_SUB take
ten day operands: closed higher-order STRING expressions (`element_at` over
`transform` and `filter`, a cast INT result, a NULL, an invalid string and a
missing index), closed INT results from `transform` and `aggregate`, and STRING and
INT literal controls. Each runs with a literal start, a NULL start, the registered
nullable DATE column, an empty table and the day before the DATE maximum. Controls
add closed higher-order results outside DATE arithmetic, lambdas whose body ignores
their parameter and the issue's reproduction verbatim.

Spark never folds a lambda, even over constant arrays or with a constant body: these
results keep their analyzed nullability through optimization and execution, and
errors inside them surface while collecting rows. The capture excludes `ConvertToLocalRelation`, so
column expressions run during execution rather than optimization. Rows come from
Spark's internal rows: DATE values are epoch days. Each record keeps the analyzed,
optimized and executed schemas, or the error condition, first message line and
failing stage.

`date_day_closed_lambda_fields.rs` compares every record at batch sizes 1 and 32.
One existing difference keeps its Spark expectation in the fixture; the test
replaces only the error with the current DAR message and checks its count:

- #1033: 9 ANSI calls with an out-of-bounds `element_at` index fail while collecting
  with `element_at: the index is out of bounds` instead of
  `INVALID_ARRAY_INDEX_IN_ELEMENT_AT`.

Recapture from the crate root:

```sh
JAVA_HOME=<JDK 17 or 21> SPARK_LOCAL_IP=127.0.0.1 python \
    tests/fixtures/date-day-closed-lambda-fields/capture_date_day_closed_lambda_fields_spark.py \
    "$(mktemp -d)/date-day-closed-lambda-fields-spark.jsonl"
```
