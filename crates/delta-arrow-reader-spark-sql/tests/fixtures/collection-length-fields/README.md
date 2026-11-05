# Collection-length result fields

Run the complete field and count comparisons from the Spark SQL crate:

```sh
cargo test --locked --workspace --test collection_length_fields -- --nocapture
```

`spark-reference.jsonl` contains 144 independent observations from Spark 4.2.0,
revision `32f7299601108917fb01920a54e084595b7b3bf8`, in UTC with case-sensitive
resolution disabled. The header records the full provider schemas, metadata
and rows. Its SHA256 is
`3d4f384bef83fad9aaaac8e9d6c2044bd2943801ec17c0084475b53155c049b6`.
The original capture is unchanged; `provenance.json` records its source and
helper hashes.

The test compares complete analyzed, optimized and executed fields, including
names, types, nullability and metadata, and exact count rows at batches 1 and
32. The cases cover SIZE, CARDINALITY and ARRAY_SIZE under both ANSI modes and
both legacy NULL-size settings. Required and nullable array/map containers,
NULL children, nested arrays, empty collections, untyped NULL, mixed providers,
NULL-only providers and empty providers have separate assertions. CASE,
COALESCE, filters and aggregates check consumers of those counts.

Input-fidelity checks compare the complete recorded provider fields and raw
rows before query evaluation. A separate Map check preserves entry order and
duplicate keys, distinguishes NULL from empty Maps/lists, and rejects NULL
entries, keys and required values.

Forty references require planning rejection for the recorded argument type.
Six require a bad-string-to-INT cast failure during Spark's collect action.
That action includes lazy optimization and physical preparation. The Rust
comparison requires the matching typed native cause during resolution for
type rejection, or during physical preparation or collection for the cast.
It logs each raw native phase, error and available schema without changing
the reference phase. A matching error message in an unrelated error class
does not satisfy the assertion.

The original six `scalar_consumer` records are replayed directly from the
unchanged 92-record `nested-lambda-captures-original-spark.jsonl` fixture.
They check SIZE over TRANSFORM with nullable captured values on mixed,
NULL-only and empty providers. The four unaliased Map-name controls in that
fixture remain separately tracked under #823.

Native DataFusion controls compare complete raw fields and rows in configured
and unconfigured sessions before and after Spark planning. They retain native
UInt64 counts, dimension lengths and deep array cardinality. Session options
must remain unchanged. The existing `collection_size` target covers argument
arity, Arrow views/slices/fixed arrays, checked INT overflow and exactly-once
input evaluation.

To recapture with Python 3.12, PySpark 4.2.0 and Java 21, write to a new path:

```sh
collection_length_capture_dir="$(mktemp -d)"
PYTHONPATH="$PWD/tests/fixtures${PYTHONPATH:+:$PYTHONPATH}" \
    python tests/fixtures/collection-length-fields/capture_size_result_nullability_spark.py \
    "$collection_length_capture_dir/spark-reference.jsonl"
```

The capture checks the exact Spark revision and refuses to replace an existing
output. Rust tests replay committed references and do not run Spark.
