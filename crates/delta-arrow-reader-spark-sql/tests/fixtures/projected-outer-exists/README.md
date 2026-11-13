# Projected outer EXISTS references

`spark-results.jsonl` contains 52 Spark 4.2.0 value/type references. The records
cover both ANSI modes, and the test replays them across three provider and batch
layouts. Four ordered records provide 12 comparisons of the exact `ORDER BY l.k NULLS FIRST,l.n`
results; other records compare duplicate-preserving row multisets.

The capture executes all 56 cases in `requests.json`. Its header's
`expected_record_count` describes that full capture. `focused_selection` lists
the 52 retained records and these four excluded identities:

| Case ID | ANSI modes | Boundary |
| --- | --- | --- |
| `800_direct_global_aggregate_empty` | `false`, `true` | [#1305](https://github.com/mag1cfrog/delta-arrow-reader/issues/1305) |
| `800_direct_global_aggregate_empty_not` | `false`, `true` | [#1305](https://github.com/mag1cfrog/delta-arrow-reader/issues/1305) |

The native DataFusion optimizer loses global-aggregate row presence on these
queries. Their original Spark expectations remain in the full capture; they
grant no mismatch allowance. Live shared computed outputs and flags from nested
queries are tracked separately in [#1309](https://github.com/mag1cfrog/delta-arrow-reader/issues/1309).

Use Python 3.12 with PySpark 4.2.0 installed, and set `JAVA_HOME` to a Java 21
installation. Capture the full reference from this directory:

```sh
TZ=UTC python3.12 capture_spark.py /tmp/projected-outer-exists-raw.jsonl
```

The compact fixture retains the runtime/provenance header plus the explicit
selection. Each selected record keeps its SQL, ANSI mode, ordering and status,
the executed schema, and `rows_raw` for ordered cases or `rows_multiset` for
unordered cases. The comparison removes nullability, metadata and top-level
result labels; it preserves executed data types and nested field names.
