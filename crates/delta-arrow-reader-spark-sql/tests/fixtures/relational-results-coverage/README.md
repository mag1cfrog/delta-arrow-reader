# Ordinary relational result references

The source-only requests cover the original 64 SQL shapes and 21 additional
ordinary shapes, each in both ANSI modes. The original 128-record fixture at
`../relational-queries-spark.jsonl` stays unchanged. `legacy-requests.json`
replays its SQL and registered inputs on the current pinned Spark runtime.
`requests.json` supplies the 42 new records in `spark-results.jsonl`.

The new cases cover registered DISTINCT, deterministic LIMIT/OFFSET, empty-right
ON/USING joins, empty-inner WHERE subqueries, skewed multi-column EXCEPT ALL and
qualified NATURAL join keys. Existing CTE, scalar and set-operation cases stay
in the original corpus. Focused wildcard, repeated-column and retained-binding
regressions run in their own test binaries.

The Rust test compares complete rows, NULLs, multiplicity and executed types
across three provider/input/target/output layouts: `(1,1,1,1)`, `(3,2,4,3)` and
`(2,32,2,32)`. Ordered queries retain their sequence; other queries compare row
multisets. Result labels, metadata and nullability annotations are excluded.
Physical batch validity and the existing scalar-many validation remain checked.

The four projected IN/NOT IN cases retain both their raw Spark references and
the approved SQL three-valued truth tables. Their 24 layout contexts are
reported separately from Spark matches; four of their eight raw records differ
from the approved truth table. The six scalar-many contexts check DAR's
own cardinality validation. The remaining 480 contexts compare successful
Spark rows and executed types.

Use Python 3.12 with PySpark 4.2.0 installed and select Java 21 with `JAVA_HOME`.
From this directory, write fresh captures with:

```sh
TZ=UTC SPARK_LOCAL_IP=127.0.0.1 python3.12 capture_spark.py /tmp/relational-new.jsonl
TZ=UTC SPARK_LOCAL_IP=127.0.0.1 python3.12 capture_spark.py /tmp/relational-original.jsonl legacy-requests.json
```

Both commands record SQL, inputs, settings, runtime/JAR/source hashes, schemas,
plans and owned-JVM cleanup. They read no expected outputs. Compact new records
retain their capture header, ordered raw rows or unordered row multiset, and
executed schema without value/type normalization.

The original fixture uses decimal/nan/zero text for floating values. Its Rust
encoder keeps that convention. Current full captures use the shared protocol's
floating bit encoding; the replay evidence explicitly converts that display
encoding when comparing with the original fixture.
