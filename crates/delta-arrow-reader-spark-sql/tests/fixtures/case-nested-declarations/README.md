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

The replay requires 194 strict Spark matches and two exact inherited observations
for [#870](https://github.com/mag1cfrog/delta-arrow-reader/issues/870). They are
`fields/required_condition_error`, input `literal`, ANSI `true`, at batches 1 and
32. Spark raises `CAST_INVALID_INPUT` while obtaining its optimized schema,
corresponding to DAR's `create_physical_plan`; DAR raises the same condition at
`collect`. The complete records are unchanged between the test-only baseline
`da3be4b9` and source candidate `1e4451c8`.

The replay retains the original Spark expectation and emits `matches: false` for
both observations. It then checks every observed field, row value, error, phase,
comparison flag and identity against the pre-change record. All three observed
schemas equal Spark's recorded analyzed schema. A changed or resolved inherited
difference fails the assertion and requires review. Issue #870 owns eager
constant-condition evaluation in Spark expression preparation, outside the nested
declaration paths. No generic OneRow or shared constant-folding repair belongs to
this source slice.

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
