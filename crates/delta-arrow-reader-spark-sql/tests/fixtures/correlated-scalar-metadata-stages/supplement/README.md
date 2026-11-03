# Supplemental scalar metadata stages

This source package prepares independent Spark observations for #818. No engine
has run for this package. The repository's test-only commit is
`d1e7a104049e6589ce40d05beafb771e072248c4`; its product source remains
`5bce7951cf8665b9a6d44c1c57d36a8425fc83af`.

The capture reads 68 explicit cases and nine unchanged provider definitions
from `supplement-inputs-and-cases.json`:

| Group | Observations | Purpose |
| --- | ---: | --- |
| Required source | 18 | Replace only `q.v` with required `q.id` in each of the three correlated shapes, preserving the three providers and both ANSI modes. |
| Bare scalar guard | 24 | Replay every original SQL statement, including volatile siblings and incidental CAST controls. |
| CASE scalar guard | 12 | Replay every original CASE statement and input. |
| Nonnullable scalar guard | 8 | Retain the exact required-source nullability controls. |
| Extension scalar guard | 4 | Recreate the exact Rust guard's SQL, rows, nullable STRING and required INT fields, and all source/extension metadata. |
| Native extension SQL as a Spark control | 2 | Observe the guard's exact real-relation statement in both Spark ANSI modes. This does not replace its native DataFusion comparison. |

The old bare, CASE and nonnullable raw files remain byte-exact in `sources/`.
Their complete original observations also remain under `original_observation`
in the planned cases and new output. The extension provider is transcribed from
the frozen `single_row_correlated.rs` source in that directory. No existing
guard or expected field has been changed.

Each result retains public, analyzed, optimized and executed schemas, before
and after collection, plus plans and complete duplicate-preserving rows. A
failure retains completed stages and its actual condition, message and phase.
The volatile UDF has the original nondeterministic behavior; planning and
execution counts are recorded separately. Public field, row and evaluation
comparisons with old raw observations are reported without discarding a new
stage difference. Extension strings stay strings, including the literal
`"null"` and the actual NULL row.

Root can run the frozen capture with its existing Spark 4.2.0 environment:

```sh
/home/hanbo/.local/share/delta-arrow-reader-review/spark-array-null-2026-10-31/reference-venv/bin/python capture_correlated_scalar_metadata_stages.py supplement-spark.jsonl
```

Use this directory as the working directory and a fresh output filename. The
script validates source hashes and refuses an existing destination. The Spark
revision must equal `32f7299601108917fb01920a54e084595b7b3bf8`. The outer
`manifest.json` freezes the capture script, case manifest and source snapshots.

The repository's complete red target runs from its standalone Spark SQL
workspace:

```sh
cd /home/hanbo/repo/delta-arrow-reader-spark-correlated-metadata-stages/crates/delta-arrow-reader-spark-sql
cargo +1.96.0 test --locked --workspace --test generated_alias_metadata -- --nocapture --test-threads=1
```

After capture, retain the original public observations and add the new executed
observations. Correct a physical-equals-public assertion only when these
independent fields disprove it. Keep exact metadata, type, nullability, names,
values, batch validation and native guards; no global wrapper lifetime change
is justified merely by a new test passing.
