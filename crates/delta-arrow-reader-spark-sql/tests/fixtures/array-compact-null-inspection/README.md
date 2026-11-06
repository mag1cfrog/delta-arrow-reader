# ARRAY_COMPACT NULL-inspection boundaries

`spark-reference.jsonl` preserves 20 independent observations from Spark 4.2.0
revision `32f7299601108917fb01920a54e084595b7b3bf8`, in UTC and both ANSI modes.
Its SHA256 is
`fc0749b2b0438990c61e855c87be09f643b51c1bd10dd733ce79acaf8e25352e`.
The header records the exact build, capture and input-manifest hashes and the
registered provider's complete schema and rows. Each record retains available
schema stages, plans, rows, failure phase and Spark error parameters.

In ANSI mode, VALUES-backed NULL checks retain their early cast failures.
Provider-backed required siblings report `required` in either output order;
direct and STRUCT consumers report the first required cast, `first`.
Discarded scalar-subquery and volatile-array operands return required BOOLEAN
false. All ten scenarios succeed with ANSI disabled. These cases distinguish
the early local conversion boundary from later NULL propagation.
Spark runs [early local conversion](https://github.com/apache/spark/blob/32f7299601108917fb01920a54e084595b7b3bf8/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/optimizer/Optimizer.scala#L191)
before [NULL propagation and constant folding](https://github.com/apache/spark/blob/32f7299601108917fb01920a54e084595b7b3bf8/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/optimizer/Optimizer.scala#L120).

`subquery-preparation/spark-reference.jsonl` retains two complementary
observations. A constant bad cast in a provider-backed scalar subquery fails
during optimization in ANSI mode, before the outer NULL check can discard it.
With ANSI disabled, the result is required BOOLEAN false. The supplement's
SHA256 is
`da0f4252d7ff23eacc82d4285cde214d20bfe7ff8102197e547d00af1769daca`.

Both capture scripts are self-contained. The source-reuse entries in their
input manifests identify provenance relative to the original capture workspace;
they are not runtime imports. The original #890 `constant_bad_cast_is_null`
records remain unchanged in `../array-compact-fields-spark.jsonl`.

With the pinned PySpark environment active, validate the input-only manifest
and capture to a new output path:

```sh
python capture_spark.py --self-check
python capture_spark.py /absolute/path/to/new-reference.jsonl
python subquery-preparation/capture_spark.py --self-check
python subquery-preparation/capture_spark.py /absolute/path/to/new-subquery-reference.jsonl
```

Parser failures, provider mismatches and unexpected error conditions abort
capture. Allowed conditions restrict captured errors; they do not specify
expected outcomes. From the crate directory, replay the complete fields, rows,
error phases and first cast inputs in 44 strict comparisons at batch sizes 1
and 32:

```sh
cargo test --locked --workspace --test compact_null_inspection -- --nocapture
```
