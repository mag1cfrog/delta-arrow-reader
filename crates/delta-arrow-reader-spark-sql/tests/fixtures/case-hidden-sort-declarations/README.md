# Hidden sort output declarations

This fixture replays 86 independent Spark records at DAR batch sizes 1 and 32.
It covers nested CASE declarations with hidden ORDER BY columns, their no-ORDER
counterparts, scalar results, absent scalar rows, metadata, generated names,
required and optional STRUCT fields, and numeric coercion.

## Replay

From the repository root, run:

```sh
cargo test -p delta-arrow-reader-spark-sql --test case_hidden_sort_declarations -- --nocapture
cargo test -p delta-arrow-reader-spark-sql --test case_reachable_fields -- --nocapture
```

The second test retains all 324 original CASE records at both batch sizes.
The hidden-sort test checks analyzed fields, both recorded physical schemas,
complete rows, Arrow batch validity, and declared nonnullable fields. It also
compares the raw row sequence for records carrying `ordered: true`.

## Raw sources and selection

`delivery-packages.json` lists the six raw files in `references/`, their hashes,
and the selected IDs. Files remain whole and unchanged. Selection happens in the
test; expected names, types, nullability, metadata, rows and errors are read from
the original records. The package name and complete record distinguish identities
that share a short ID.

| Package | Selected records | SHA-256 of raw file |
| --- | ---: | --- |
| original-parents | 48 | `d856c4702eeb6363fca199acbddb30af62b4eb55871792124a6bf4e7d63d4661` |
| scalar-case-literal | 4 | `90002192e6381674a7914bd7fef4301d086c8dcd203a6f75c1bdcb35e71158fb` |
| scalar-case-folded | 4 | `fcd1037b0866637ffb9a095c28b788008a55eef160c5d97cfde847b430905b8f` |
| absent-scalar-original | 4 | `3fa86a006fe1d8cbaebec4b1c23a107b2bd670f07b9cf4d8b30b62979d5cf6eb` |
| numeric-pruning-original | 2 | `71e6c66aa31123954e0638bf6d09746178db9ccd8cdc0230220dcce2e55eb745` |
| new-boundaries | 24 | `9445afefef5d3d1237760350e14d365d20072cea7af97a5e59f5e95a3377518e` |

Historical sources store rows as multisets and do not retain Spark's raw row order
or batch layout. The 24 new records include explicit order markers for hidden-sort
queries. Their capture preserves the collected sequence when `ordered` is true.

## Reproduce the new capture

Use PySpark 4.2.0 built from revision
`32f7299601108917fb01920a54e084595b7b3bf8`. With that environment active, run
from this directory:

```sh
python capture_spark.py /tmp/case-hidden-sort-fresh.jsonl --manifest cases.json
```

The output must be a fresh path. The script verifies Spark's version, build
revision and UTC timezone, registers the recorded providers, and captures both
ANSI modes. `cases.json` supplies the 12 definitions and exact provider data.
Its preparatory `reference_status` text is preserved in the raw header;
`spark-capture-v1-result.json` records the completed capture, and
`spark-independent-audit-v1.json` checks its 24 records and 96 stage schemas.

`source-inventory.json` and `provenance/capture-source.json` record the capture
script and historical raw-source provenance. Their absolute source paths identify
the original capture workspace; replay uses only files committed here.
