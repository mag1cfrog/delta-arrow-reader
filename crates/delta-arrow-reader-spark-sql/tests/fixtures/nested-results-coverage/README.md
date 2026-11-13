# Ordinary nested result references

`requests.json` defines 38 SQL shapes in both ANSI modes using ordinary ARRAY,
STRUCT and MAP literals and registered columns. Inputs distinguish NULL and
empty containers, NULL STRUCT parents and children, NULL elements and map
values, missing and NULL lookup keys, and duplicate rows and array elements.
Access includes dotted and bracketed fields, computed subscripts and keys,
nested collection values and outer consumers. Nested ARRAY/STRUCT and MAP/ARRAY
generators add SELECT, LATERAL VIEW and OUTER cases.

`spark-results.jsonl` retains the full independent Spark 4.2.0 capture: source
and runtime hashes, inputs, settings, raw rows, row multisets, schemas and plans.
The maintained capture driver owns and reaps its JVM and reads no expected
output. The local adapter adds ARRAY/MAP input decoding and MAP entry-pair
encoding. It keeps map positions and key/value associations.
The source receipts include the delegated encoder and input registration helper.

`../../nested_results.rs` also replays the 34 existing primitive generator
shapes from `../array-map-explode/` against their unchanged exact references.
The combined 144 ANSI cases compare full rows and executed types across
provider/input/target/output layouts `(1,1,1,1)`, `(3,2,4,3)` and `(2,32,2,32)`.
Direct provider scans verify source partition counts and values before query
optimization. Ordered queries compare sequence; others compare row multisets
without dropping duplicates. Nullability annotations, result metadata and
enclosing output labels are excluded. Nested field names and collection
positions remain part of the comparison, and actual Arrow batches validate.

Of the 432 main layout contexts, 428 compare exact Spark rows and types. The
retained seeded-RAND outer-generator case compares exact rows in the original
single-partition layout. Its four other layout contexts check exact types,
every source id once in query order, and only `(NULL,NULL)` or `(0,'x')`
position/value pairs.
Those contexts do not require identical random draws across execution layouts.
The 16 hidden-parent contexts compare exact Spark rows and types separately.

A separate storage control puts non-NULL children below a valid Arrow NULL
STRUCT parent and compares the same visible input with Spark. Native SQL has
its own unchanged-session comparison. Existing invalid-input and storage guards
run in their original test binaries.

Use Python 3.12 with PySpark 4.2.0 installed and select Java 21 with `JAVA_HOME`.
From this directory, write a fresh capture with:

```sh
TZ=UTC SPARK_LOCAL_IP=127.0.0.1 python3.12 capture_spark.py /tmp/nested-results.jsonl
```

The sibling `/tmp/nested-results.lifecycle.json` records owned-JVM cleanup.

From the repository root, run the focused Rust replay with:

```sh
cargo test --locked --manifest-path crates/delta-arrow-reader-spark-sql/Cargo.toml --test nested_results
```
