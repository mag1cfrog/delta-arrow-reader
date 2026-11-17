# Preceding SELECT aliases

The source-only requests cover scalar chains, source-column precedence, nested
STRUCT access, aggregate and window outputs, HAVING, stars and a correlated
name collision in both ANSI modes. The Rust tests compare values and executed
types with Spark 4.2.0 and check that private alias references leave no pending
parameters in the returned plan.

The supplemental tuple-subquery requests check alias operands of IN and NOT IN.
Capture them with `capture_spark.py OUTPUT tuple-subquery-requests.json`; the
original 52 reference records remain in `spark-reference.jsonl`.

The reference layout compares seeded RAND values exactly. The second layout
checks alias reuse, types and row preservation under the existing random-sample
boundary in #1248. A volatile host UDF also checks final alias-value reuse across
one and three input partitions; its evaluation counts remain observations.
