# NULL STRUCT parent references

`requests.json` defines 26 SQL shapes in both ANSI modes. Registered values
distinguish a NULL STRUCT parent from a non-NULL STRUCT with NULL children.
Fields include primitives, ARRAY, MAP, nested STRUCT and VOID, with duplicate
rows and ARRAY elements. Queries cover dotted, bracketed and mixed access,
predicates, derived aliases, grouping, outer references, wildcard expansion,
ARRAY-of-STRUCT fields and lambda consumers.

`spark-results.jsonl` retains the full Spark 4.2.0 capture: SQL, logical inputs,
source and runtime hashes, settings, raw rows, row multisets, schemas and plans.
The maintained driver owns and reaps its JVM and reads no expected outputs.
The local adapter decodes ARRAY/MAP inputs and encodes MAP entry pairs; its
delegated encoding and registration sources are explicitly hashed.

`../../null_struct_parent.rs` compares these results with ordinary Arrow inputs
and a second representation of the same logical inputs. That representation
stores non-NULL children under NULL STRUCT parents, including nested parents
and ARRAY elements. Direct scans verify identical visible rows and actual
source partition counts. Both representations run across provider/input/target/
output layouts `(1,1,1,1)`, `(3,2,4,3)` and `(2,32,2,32)`, giving 312 comparisons.
Ordered queries retain sequence; others compare row multisets with duplicates.
Executed types retain nested field names while excluding nullability, metadata
and enclosing output labels. Actual Arrow inputs and outputs validate.

Separate controls check that a volatile nullable parent runs once per required
source row, including shared consumers, and never runs during planning.
Progressively longer dot, bracket and mixed paths check linear expression
growth and their NULL/value results. Sliced dictionary parents and children
retain native types and value buffers. Native SQL keeps its existing field behavior, and
invalid fields still fail even when every parent is NULL. Existing Delta
provider tests retain the top-level file-pruning checks.

A separate Arrow preservation control compares run encoding and Union layouts
with the prior native CASE guard, including slices. It checks existing Arrow
types, values, logical NULLs and run compression without claiming Spark Union
type compatibility.

Use Python 3.12 with PySpark 4.2.0 and select Java 21 with `JAVA_HOME`.
From this directory, write a fresh capture with:

```sh
TZ=UTC SPARK_LOCAL_IP=127.0.0.1 python3.12 capture_spark.py /tmp/null-struct-parent.jsonl
```

The sibling `/tmp/null-struct-parent.lifecycle.json` records owned-JVM cleanup.
From the repository root, run the focused replay with:

```sh
cargo test --locked --manifest-path crates/delta-arrow-reader-spark-sql/Cargo.toml --test null_struct_parent
```
