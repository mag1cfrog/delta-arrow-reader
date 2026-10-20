---
title: Compare selective reads with deletion vectors
description: Pair unchanged Parquet files with actual deletion vectors and a feature-only control across five pinned readers.
---

# Compare selective reads with deletion vectors

These cases measure selective reads on tables with deletion vectors (DVs). They
extend the no-DV comparisons with the same queries and Parquet bytes. File
organization, predicate complexity, projection width, and within-file pruning
remain separate factors; DVs are one supported table feature.

| No-DV case | Paired case | Deletion rule |
| --- | --- | --- |
| `li.clustered.date7-full` | `li.clustered.date7-full.dv` | Public logical key hash |
| `li.shuffled.date7-full` | `li.shuffled.date7-full.dv` | Public logical key hash |
| `wide.clustered.eq2-in20` | `wide.clustered.eq2-in20.dv` | Public logical key hash; 69 output columns |
| `li.clustered.date7-limit` | `li.clustered.date7-limit.dv` | Public logical key hash; unordered LIMIT 100 |
| `row-groups.select` | `row-groups.select.dv` | Deterministic nonmatching rows |
| `pages.localized` | `pages.localized.dv` | Deterministic nonmatching rows |
| `row-groups.select` | `row-groups.select.feature-only` | No deleted rows or descriptors |

The first four pairs use the TPC-H-derived data. For each logical key, hash the
UTF-8 string `l_orderkey/l_linenumber` with SHA-256. Interpret its first eight
bytes as an unsigned little-endian integer and delete the row when its remainder
modulo 1,000 is zero. The clustered and shuffled tables therefore delete the
same logical rows, despite their different physical positions. Qualifying rows
can be deleted, so the report must retain the changed result volume.

The synthetic controls delete `event_id = 'other' AND row_id % 1000 = 0`. Their
qualifying rows and output values stay identical. The feature-only control
enables the DV protocol feature without attaching any DV descriptors.

## Generate paired snapshots

First prepare [public fixtures](selective-read-fixtures.md) and the
[within-file controls](selective-read-within-file.md). Match the public profile
to the requested DV profile. Control geometry does not depend on the profile.
From the repository root:

```sh
RUSTFLAGS='-C target-cpu=x86-64' CARGO_TARGET_DIR=target/selective-read \
  cargo +1.98.1 run --release --locked -j8 \
  --manifest-path benches/selective_read/Cargo.toml -- \
  --profile development \
  --dv-from ../selective-read-development \
  --dv-from ../selective-read-controls \
  --output ../selective-read-dv
```

The output contains five base tables, five `.dv` tables, and one
`.feature-only` table. The two clustered original queries share a table.
`--dv-from` is repeatable and mutually exclusive with `--controls` and
`--repack-from`. Preparation verifies parent hashes, reserves half the disk
budget for the later MinIO copy, and never overwrites an output directory.

Each variant starts with a no-DV version 0. Version 1 enables reader version 3,
writer version 7, and the `deletionVectors` reader/writer feature. Its metadata
sets `delta.enableDeletionVectors` to `true`. Affected files replace their
original Add with one referring to the same Parquet bytes and a DV descriptor.
Unaffected files have no descriptor. Physical `numRecords` and conservative
bounds remain valid; DV-bearing Adds set `tightBounds` to `false`.

The pinned Delta Kernel 0.25.0 writer produces one portable Roaring DV per
affected file. Table and DV UUIDs are deterministic. The manifest retains both
transaction logs, deleted logical keys, physical row ordinals, descriptor
offset/size/cardinality, payload hashes, density, and file coverage.

## Prepare independent live-row references

Use the [pinned oracle environment](selective-read-oracle.md):

```sh
set -- --case row-groups.select.feature-only
for base in li.clustered.date7-full li.shuffled.date7-full wide.clustered.eq2-in20 \
  li.clustered.date7-limit row-groups.select pages.localized; do
  set -- "$@" --case "$base" --case "$base.dv"
done
cargo +1.98.1 run --locked -p xtask -- selective-read prepare-reference \
  --python ../selective-read-oracle-venv/bin/python --fixtures ../selective-read-dv \
  --output ../selective-read-dv-references "$@"
```

The oracle checks the versioned logs, protocol, metadata, unchanged Parquet
inventory, descriptors, and DV envelope/checksum. It independently derives
deletions from source rows and verifies each saved physical ordinal against
the full decoded file. Expected results use those checked deletion lists,
without calling any tested Delta reader.

References retain physical and live row counts, deleted qualifying rows,
candidate files from conservative statistics, and files with live matches.
Predicate-step counts describe the physical rows before deletions. For LIMIT,
the reference retains every qualifying live row and accepts any 100 distinct
qualifying live rows, or all available live rows if fewer than 100 exist.

## Run all five pinned adapters

Prepare the [five builds](selective-read-runners.md), start
[dedicated MinIO](selective-read-storage.md), and upload the complete fixture:

```sh
cargo +1.98.1 run --locked -p xtask -- selective-read upload-storage \
  --state ../selective-read-storage --fixtures ../selective-read-dv \
  --output ../selective-read-dv-upload.json

set -- --state ../selective-read-storage --fixtures ../selective-read-dv \
  --upload ../selective-read-dv-upload.json --output ../selective-read-dv-campaign \
  --python ../selective-read-oracle-venv/bin/python --no-sessions
for reader in dar delta-rs duckdb polars daft; do
  set -- "$@" --binary "../selective-read-$reader-build/selective-read-$reader"
done
for base in li.clustered.date7-full li.shuffled.date7-full wide.clustered.eq2-in20 \
  li.clustered.date7-limit row-groups.select pages.localized; do
  for case in "$base" "$base.dv"; do
    set -- "$@" --case "$case" --reference "../selective-read-dv-references/$case"
  done
done
cargo +1.98.1 run --locked -p xtask -- selective-read run-campaign "$@" \
  --case row-groups.select.feature-only \
  --reference ../selective-read-dv-references/row-groups.select.feature-only
```

Finish generation, reference checks, and compilation before timing. The
campaign probes each actual snapshot with every pinned adapter and preserves
all 65 reader/case entries. Evidence-backed capability rejections remain
`unsupported`; a reader that silently returns deleted rows fails correctness.
No adapter substitutes a direct Parquet scan for its native Delta path.

Read `summary.json` alongside the references and raw observations. Compare each
DV result to its no-DV pair, including output-row changes. Compare both row-group
variants to the base to separate feature enablement from bitmap loading.
Separate I/O diagnostics classify Delta logs, Parquet, and DV payloads, retain
request timestamps, and record requests starting or ending after the final
stream event. A response spanning that event has no exact post-event byte split;
the observer leaves that byte counter null with a reason.

Missing LIMIT pushdown alone does not prove a full scan. Use observed touched
files, transferred bytes, requests, and post-stream activity to describe what
happened. Source-level DV guards explain why a comparison is useful, not its
runtime result. The required many-file wide compound DV pair is a separate
follow-up; this slice adds no CI job or performance timing step.
