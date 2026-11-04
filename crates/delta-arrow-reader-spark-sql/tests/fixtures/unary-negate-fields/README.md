# Unary negation result fields

These references capture Spark 4.2.0 at revision
`32f7299601108917fb01920a54e084595b7b3bf8`, in UTC with ANSI enabled and disabled.
The input-only manifest contains 36 SQL scenarios and required/nullable INT
providers, including source metadata and a provider with zero rows. The capture
checks provider schemas and rows before observing SQL.

The immutable capture files have these SHA256 values:

- `capture_spark.py`: `2a04add6ba8bcb197a6bc4f332248dded5e3ebff86ec16ef0b8bf5dde3887634`
- `manifest.json`: `537aecac2327930fc34cd20d395ce95408d2b80336db83f6a62d8519d1710870`
- `spark-reference.jsonl`: `bf0b3d3dbdb00e180926707b04da054b96c8848b81bdfe0ebabdeae2cd66b670`

Run the input and provenance checks without importing Spark:

```sh
python3 tests/fixtures/unary-negate-fields/capture_spark.py --self-check
```

To capture again, use the pinned PySpark build and pass a new output path to the
same script. The checked-in raw reference stays unchanged.

`unary_negate_result_fields.rs` checks all 72 reference identities at batch sizes
1 and 32. It compares public, optimized and physical fields, complete rows,
ordered rows, error conditions and failure stages. Direct, STRUCT and NULL
predicate consumers distinguish required and nullable negation results. Source
and BIGINT controls retain their declarations. Ten native DataFusion SQL
comparisons check configuration isolation.

`inherited-controls.jsonl` retains 12 ANSI identities owned by #896, used at both
batch sizes. Each control contains its complete frozen Spark record and complete
DAR observation from the recorded candidate head. Only the batch label is
normalized. Their raw `matches` and `error_matches` flags remain false: live
overflow errors lack Spark's condition, and constant overflow also fails at the
wrong stage. Field and value comparisons remain exact. Guard tests reject
changed fields, metadata, rows, errors, phases, reference content and extra keys.

## Historical replay corrections

`historical/` contains five queries selected from the frozen
`accepted-runtime.jsonl.gz` archive, SHA256
`4a6de91a60bd9d6db19e8c373e33e45bb393f9f767316d88c41547edd612a271`.
Its input-only manifest identifies each original query and ANSI mode. The
correction test verifies those identities, SQL, unchanged row multisets and full
replacement outcomes against the original archive.

The historical capture files have these SHA256 values:

- `historical/capture_spark.py`: `58dcd47d10a8689ee7131d2af6acc199051786c8f2941a9082eab55d46761cf1`
- `historical/manifest.json`: `00aa90aa2bad307dd14cb8065e23497e54d1626725aa39b7b5fbfd099eeb6ff2`
- `historical/spark-reference.jsonl`: `f4f0fd667aa9f284fef9d068618bb5d6d34ee22ee6ef3eaa3fdbde185a7ff8f5`

For all five queries, every schema stage retains the name `r`, BIGINT type and
empty metadata, with nullability set to false. The full-stage
replay requires exact matches at batch sizes 1 and 32. The five reviewed corpus
corrections change only nullability; the compressed source and every existing
correction stay unchanged.
