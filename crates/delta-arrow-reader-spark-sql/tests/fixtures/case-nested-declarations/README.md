# Nested CASE declarations

This fixture retains 98 independent Spark records: 74 nested result-field
records, 22 pruning records and two numeric-pruning records. The replay uses all
records at DAR batch sizes 1 and 32, for 196 comparisons. It checks analyzed
fields, both physical schemas, complete rows, actual NULLs, Arrow batch validity,
required error conditions and failure phases through the shared observer.

The cases cover Boolean and DATE arrays, STRUCT children, nested arrays,
true/false/NULL and column conditions, missing ELSE, empty providers, selected
and dead error branches, and numeric widening. The pruned BIGINT/INT array case
requires both the common public declaration and the selected branch's BIGINT
value conversion. Native DataFusion CASE uses registered nested columns in three
rows and an empty provider. Ten expressions run at both batch sizes, before and
after a Spark query in the configured session, for 80 complete native/configured
observation pairs. Each native query must succeed and return the provider's row
count; every field, value, error and completion phase must match its configured
counterpart.

## Replay

From the repository root, run:

```sh
cargo test --locked --workspace --manifest-path crates/delta-arrow-reader-spark-sql/Cargo.toml --test case_nested_declarations -- --test-threads=1 --nocapture
```

The `required_condition_error` record in ANSI mode has an independently inherited
phase difference: Spark raises `CAST_INVALID_INPUT` while obtaining its optimized
schema, corresponding to DAR's `create_physical_plan`; historical DAR observations
raise the same condition at `collect`. The replay keeps the original expectation
and reports the exact comparison. Its owner is constant-condition evaluation in
Spark expression preparation, outside the nested declaration paths. No generic
OneRow or shared constant-folding repair belongs to this fixture's source slice.

## Sources and reproduction

`delivery-packages.json` records the pinned Spark build, whole reference files,
counts, SHA-256 hashes and the original capture manifests. All three reference
files and their capture inputs remain byte-for-byte unchanged. There is no
selection filter or rewritten expectation.

| Reference | Records | SHA-256 |
| --- | ---: | --- |
| `spark.jsonl` | 74 | `659f04a7fea99b4cff95a4e6320ecd17ef4cfb6e515823be6fb68514c992b971` |
| `pruning-spark.jsonl` | 22 | `9dab6d28be3d5197a3fd48b8299b2f12af1467020c246a1a6cf47b1a15210b29` |
| `numeric-pruning-spark.jsonl` | 2 | `71e6c66aa31123954e0638bf6d09746178db9ccd8cdc0230220dcce2e55eb745` |

Use PySpark 4.2.0 built from revision
`32f7299601108917fb01920a54e084595b7b3bf8`. From this directory, with that
environment active, run each capture to a fresh output path:

```sh
python capture_spark.py /tmp/case-nested-fields-fresh.jsonl --manifest cases.json
python capture_spark.py /tmp/case-nested-pruning-fresh.jsonl --manifest pruning-boundaries.json
python capture_spark.py /tmp/case-nested-numeric-fresh.jsonl --manifest numeric-pruning.json
```

The script verifies Spark's version, revision and UTC timezone, registers the
recorded providers, and captures both ANSI modes. Expected rows are multisets;
the original capture does not record Spark's batch layout.
