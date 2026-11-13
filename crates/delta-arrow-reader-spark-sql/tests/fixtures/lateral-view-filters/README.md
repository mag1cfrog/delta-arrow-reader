# LATERAL VIEW filter references

`requests.json` defines 24 SQL shapes in both ANSI modes. Filters read generated
array items and positions, map keys and values, qualified names and outputs from
chained views. OUTER cases distinguish NULL and empty collections from NULL
elements or map values. Input-only predicates, SELECT without LATERAL VIEW,
empty providers and predicates matching no items provide controls.

The registered inputs and reported query come from the #156 nested result sweep.
The reported query has the same independent Spark rows and executed schema in
both captures. Only primitive ARRAY/MAP and scalar columns are read here; the
unused nested input columns preserve the original source definition.

`spark-results.jsonl` retains the full Spark 4.2.0 capture, including SQL, inputs,
source/runtime hashes, settings, raw rows, row multisets, schemas and plans. The
maintained driver owns and reaps its JVM and reads no expected output. The local
adapter decodes ARRAY/MAP inputs and encodes MAP values as entry pairs.

`../../lateral_view_filters.rs` compares full rows and executed types across
provider/input/target/output layouts `(1,1,1,1)`, `(3,2,4,3)` and `(2,32,2,32)`.
Direct scans check source partition counts and values before optimization.
Ordered queries retain sequence; others use row multisets with duplicates.
Nullability annotations, metadata and enclosing output labels are excluded.
Actual Arrow batches still validate. Separate controls keep unresolved filter
columns as errors and compare native SQL before and after those failures.

Use Python 3.12 with PySpark 4.2.0 and select Java 21 with `JAVA_HOME`.
From this directory, write a fresh capture with:

```sh
TZ=UTC SPARK_LOCAL_IP=127.0.0.1 python3.12 capture_spark.py /tmp/lateral-view-filters.jsonl
```

The sibling `/tmp/lateral-view-filters.lifecycle.json` records owned-JVM cleanup.

From the repository root, run the focused Rust replay with:

```sh
cargo test --locked --manifest-path crates/delta-arrow-reader-spark-sql/Cargo.toml --test lateral_view_filters
```
