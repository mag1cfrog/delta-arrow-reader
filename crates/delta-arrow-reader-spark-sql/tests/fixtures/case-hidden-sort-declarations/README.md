# Hidden sort output declarations

This fixture replays 86 independent Spark records at DAR batch sizes 1 and 32.
It covers nested CASE declarations with hidden ORDER BY columns, their no-ORDER
counterparts, scalar results, absent scalar rows, metadata, generated names,
required and optional STRUCT fields, and numeric coercion.

## Replay

From the repository root, run:

```sh
cargo test --locked --workspace --manifest-path crates/delta-arrow-reader-spark-sql/Cargo.toml --test case_hidden_sort_declarations -- --test-threads=1 --nocapture
cargo test --locked --workspace --manifest-path crates/delta-arrow-reader-spark-sql/Cargo.toml --test case_reachable_fields -- --test-threads=1 --nocapture
```

The second test retains all 324 original CASE records at both batch sizes.
The hidden-sort test checks analyzed fields, both recorded physical schemas,
complete rows, Arrow batch validity, and declared nonnullable fields. It also
compares the raw row sequence for records carrying `ordered: true`.

## Inherited differences

The replay reports 156 strict Spark matches and 16 inherited differences. It emits
the original `matches: false` results, then checks the exact differing paths and
values below. All other schema fields, rows, required order, errors and completion
phases must match. Missing metadata keys remain distinct from JSON null values.

| Case | Required inherited difference | Owner | Observations |
| --- | --- | --- | ---: |
| `array_numeric_pruned_value_coercion` | Analyzed `/fields/0/type/containsNull`: Spark `true`, DAR `false`; both physical schemas match. | [#828](https://github.com/mag1cfrog/delta-arrow-reader/issues/828) | 4 |
| `generated_output_name_no_order`, `generated_output_name_hidden_order` | All three compared stages retain DAR's `subquery` label where Spark uses `scalarsubquery(required)`, at `/fields/0/name`. | [#790](https://github.com/mag1cfrog/delta-arrow-reader/issues/790) | 8 |
| `numeric_dead_wide_arm_no_order` | Both physical stages retain `/fields/0/type/fields/0/metadata/source = "case-id"`; Spark has no such key. The analyzed schema matches. | [#867](https://github.com/mag1cfrog/delta-arrow-reader/issues/867) | 4 |

Each case runs in both ANSI modes and at both batch sizes. The generated-name
hidden-sort case must also match Spark's nested `nullable: true` declaration;
its name difference cannot hide a nullability regression. A changed or resolved
inherited difference fails the test and requires reviewing its assertion.

## Raw sources and selection

`delivery-packages.json` lists the pinned Spark build, the six raw files in
`references/`, their hashes and counts, the selected IDs, and the capture script
and manifest hashes. Files remain whole and unchanged. Selection happens in the
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
Its preparatory `reference_status` text is preserved verbatim in the raw header.
The reference contains the completed capture's 24 records and 96 stage schemas.
Historical headers retain their original capture paths and labels; replay uses
only files committed here.
