# Native MAP_ENTRIES result fields (#1559)

MacBook Pro (Mac14,6), Apple M2 Max, native arm64 macOS 26.7.3 (25G342).
Rust 1.96.0. These were functional SQL probes.

The linked CLI's native function declared a list of structs with `key` and
`value` fields. It passed the original Arrow entry struct to the list
constructor, which encountered `keys` and `values` and panicked on the
mismatch. The fixed function keeps the declared types and names. It reuses
the existing child arrays, offsets and NULL bitmap, including when it has
to rebuild the entry struct with the declared fields. The checked Arrow
constructors return an error if a layout is invalid.
[Baseline registry and panic](native-registry-1559.md)

The fix vendors `datafusion-functions-nested` 54.1.0 and changes its
`map_entries.rs` implementation. The other 62 retained files match the
published archive. This adds the seventeenth dependency patch. The release
notices use the same pinned DataFusion commit as the other 54.1.0 forks.
[Source provenance](../../source-provenance.json)

## Linked CLI replay

| Result | Before | After |
| --- | ---: | ---: |
| Returned rows | 112 | 113 |
| Error | 8 | 8 |
| Panic | 1 | 0 |

All 121 registered names used the same statements and input table. The
other 120 probe records matched. The original `map_entries` statement
returned four rows after the fix.
[Fixed probe records](native-registry-1559-fixed-probes.csv) and [linked proof branch](https://github.com/mag1cfrog/delta-arrow-reader/tree/chore/1559-linked-map-entries-proof)

The fixed CLI SHA-256 is
`18583aefc5cbf1be5b8789925cd397d8eef8c8942f9534734680ebf31b48a093`.

## Values and executed types

The native regression runs the statement with the Arrow builder's plural
entry fields and with the canonical singular names that already worked.
The Spark regression uses the same SQL and row values with Spark map
fields. It compares the full executed schema and rows against Spark
4.2.0 at revision `32f7299601108917fb01920a54e084595b7b3bf8` with ANSI on
and off. The capture covers NULL and empty maps, a NULL value and entry
ordering. [Spark capture](../../tests/fixtures/map-entries-native/spark-reference.json)
