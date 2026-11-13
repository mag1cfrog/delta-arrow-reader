# Retained EXISTS binding references

The fixture contains 80 successful Spark 4.2.0 records in both ANSI modes. The
test compares complete rows, duplicates, NULLs and executed types across three
provider/input/target/output layouts. Ordered records retain their captured
order. Nullability, metadata and top-level result labels are excluded from the
result comparison.

| File | Records | Source |
| --- | --- | --- |
| `original-spark-results.jsonl` | 4 | Original computed and inner-capture guard oracles from #800 |
| `spark-results.jsonl` | 50 | Missing NOT EXISTS, empty, projected, inner-payload and row-presence controls |
| `outer-key-spark-results.jsonl` | 2 | Independent review's outer-key capture oracle |
| `outer-payload-spark-results.jsonl` | 4 | Independent review's selective outer-payload EXISTS/NOT EXISTS oracle |
| `supplement-spark-results.jsonl` | 20 | Missing outer IN/scalar/empty/projected and signed-predicate controls |

The seed, main and supplement request files anchor SQL and ordering for all 80
records. `seed-requests.json` contains the 10 original/review case definitions;
its expected rows and types remain in the independent Spark captures.

Every result file keeps its actual capture header, including runtime, JAR and source
hashes. Compact records retain SQL, ANSI mode, ordering, status, executed schema
and either ordered raw rows or an unordered row multiset without value/type
normalization. The test requires identical declarations for shared provider
names before registering the union of their inputs.

Use Python 3.12 with PySpark 4.2.0 installed, and set `JAVA_HOME` to a Java 21
installation. From this directory, capture the maintained request sets with:

```sh
TZ=UTC python3.12 capture_spark.py /tmp/retained-exists-main.jsonl
TZ=UTC python3.12 capture_spark.py /tmp/retained-exists-supplement.jsonl supplement-requests.json
```

These commands write full captures, including all schema stages and plans.
The retained original/review records preserve their earlier source labels.
Replay records current driver hashes; it does not overwrite historical capture
provenance. The optional request argument was added after the main capture and
does not change its SQL, registered values or decoder.

The signed-key control makes `outer_k > 0` observable. Inner payload `w` and
outer payload `n` differ from the main key and use selective allowed-key data,
so dropping a nested predicate or binding it to the wrong column changes rows.
Structural tests separately check shared producer counts, projection widths,
capture availability and a bound derived from the input expressions and stages.
