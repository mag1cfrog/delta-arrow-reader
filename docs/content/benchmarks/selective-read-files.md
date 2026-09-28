---
title: Compare file organizations
description: Repack identical SF1 rows into 64 and 4096 files and measure selective reads with all five readers.
---

# Compare file organizations

Use this experiment to measure how selective reads change when the same
clustered SF1 lineitem rows occupy 64 or 4,096 files. It runs the empty-date
predicate and the date/mode/20-part-key predicate on both organizations, plus
ten compound queries with a reused 4,096-file snapshot. All five readers use
the same objects within each organization.

Repacking also changes the number of footers, the granularity of file
statistics, and row-group/page boundaries. Interpret it as a file-organization
experiment, not an isolated file-count effect or a reproduction of the
historical 1.2-TiB table. These are TPC-H-derived scan queries under
[selective-read-v1](selective-read-protocol.md), not official TPC-H results.

## Repack the public rows

First [generate the development profile](selective-read-fixtures.md) outside
the Git checkout. Reuse that directory below. The generator checks its source
objects and copies the original reference files into the new directory; it
does not rerun TPC-H generation or sorting.

```sh
RUSTFLAGS='-C target-cpu=x86-64' CARGO_TARGET_DIR=target/selective-read \
  cargo +1.98.1 run --release --locked -j8 \
  --manifest-path benches/selective_read/Cargo.toml -- \
  --profile development --repack-from ../selective-read-development \
  --output ../selective-read-files
```

For `N` rows and `F` files, file `i` contains clustered ordinals
`[floor(i*N/F), floor((i+1)*N/F))`. The public writer settings apply, with
groups split at file boundaries. The output contains `files64`, `files4096`,
the original reference files, a pinned lockfile, and a completion manifest.
It retains every column and the source's frozen SQL and IN list.

Each file descriptor includes its ordinal range, byte size, SHA-256,
Delta statistics, and actual group/page metadata. The generator rereads all
written columns to verify their statistics. `repacked_from` records the parent
manifest hash, while `generator` records the repacking executable and source.
The parent lockfile remains identified by the parent manifest; it need not be
byte-identical to the repacker's lockfile.

Repacking streams bounded batches without sorting or spilling. Half of the
preparation disk allowance is reserved for the later MinIO copy. Use new output
directories: a failed run has no completion manifest. For a smaller data check,
use `--profile smoke` with an existing smoke parent. That still creates exactly
64 and 4,096 files, but the rows and IN literals are from SF0.01.

## Verify rows and pruning opportunity

Use the [pinned oracle environment](selective-read-oracle.md):

```sh
../selective-read-oracle-venv/bin/python -B \
  benches/selective_read/file_organizations.py prepare \
  --parent ../selective-read-development --fixtures ../selective-read-files \
  --output ../selective-read-files-references
```

The check reads every column with PyArrow and compares exact ordered values
against the parent's clustered table, across different file and batch
boundaries. It checks the file count and ordinal ranges, then uses the
independent full-read oracle to prepare all four query references. The two
organizations must produce identical reference results.

The empty predicate must exclude every file and return no rows. The compound
predicate must match rows while retaining at most 1% of files under the
independent conservative statistics check (at most one for the 64-file case).
This is a regression guard on the input geometry; it does not impose a reader
performance threshold. No reader timing influences fixture selection.

`file-organizations.json` is written last. It records the full-row verification,
table sizes, and each query's independently expected candidate and matching
file sets. These are different sets: min/max bounds can retain files with no
matching row. The per-case `reference.json` files also retain predicate-step
row counts, exact SQL, object checksums, and the reference database hash.

## Run the five readers

Prepare all five [native reader builds](selective-read-runners.md) from the
current checkout and start the [pinned MinIO server](selective-read-storage.md).
Upload this fixture directory once:

```sh
python3 -B benches/selective_read/storage.py upload \
  --state ../selective-read-storage --fixtures ../selective-read-files \
  --output ../selective-read-files-upload.json

python3 -B benches/selective_read/campaign.py \
  --state ../selective-read-storage --fixtures ../selective-read-files \
  --upload ../selective-read-files-upload.json \
  --output ../selective-read-files-campaign \
  --binary ../selective-read-dar-build/selective-read-dar \
  --binary ../selective-read-delta-rs-build/selective-read-delta-rs \
  --binary ../selective-read-duckdb-build/selective-read-duckdb \
  --binary ../selective-read-polars-build/selective-read-polars \
  --binary ../selective-read-daft-build/selective-read-daft \
  --case files64.empty --case files64.eq2-in20 \
  --case files4096.empty --case files4096.eq2-in20 \
  --reference ../selective-read-files-references/files64.empty \
  --reference ../selective-read-files-references/files64.eq2-in20 \
  --reference ../selective-read-files-references/files4096.empty \
  --reference ../selective-read-files-references/files4096.eq2-in20 \
  --session reuse.files4096
```

The [common campaign](selective-read-campaign.md) validates every reader, keeps
unsupported/failure records, and freezes its runnable subsets before timing.
With all five readers eligible, each job has ten independent timed samples.
A reuse sample is one process containing initialization and ten queries.
It is not ten independent samples. Detailed request tracing follows the timing
campaign, with two separate diagnostic observations per reader and job.

## Inspect the comparison

```sh
../selective-read-oracle-venv/bin/python -B \
  benches/selective_read/file_organizations.py report \
  --campaign ../selective-read-files-campaign \
  --references ../selective-read-files-references \
  --output ../selective-read-files-report
```

The report checks frozen campaign inputs, the reference identities, and the
summary against raw observations. Its CSV has 25 rows, including every reader
for every job. Clock columns contain medians in nanoseconds. Its JSON retains
quartiles, IQR, ratios, failure statuses, resource use, native-plan locations,
diagnostic request/byte classifications, and distinct Parquet GET/HEAD objects.
The fixture manifest holds the full physical metadata and object checksums.

For open-and-query, `open_query_ns` includes snapshot loading, planning, and
complete output consumption. For reuse, inspect `initialization_ns`,
`initialization_plus_query1_ns`, and `initialization_plus_all_queries_ns`, as
well as each query interval. A cheap repeated query does not erase its initial
metadata cost.

Native plans and their available metrics are saved without translating internal
tasks or partitions into file counts. The report leaves a comparable
reader-planned file counter and isolated planning time null, with a reason.
`diagnostic_snapshot_open_ns` spans the native snapshot-open call through the
first query-start event in a separate diagnostic run. Lazy APIs can defer
metadata work into planning; this interval is not a comparable total planning
cost and does not replace the timed reuse initialization.

Compare independent candidate sets with observed Parquet objects and traffic.
A footer request counts as touching an object. Equal pruning is a useful
result: investigate log parsing, planning, request patterns, and reading costs
when latency still differs. Keep ties and losses. The diagnostic bytes and
headline timings come from separate runs and must remain labeled that way.

The generator's bounded Rust check covers fractional file boundaries, exact
values, reproducible hashes, and missing/extra rows. The oracle checks cover
changed values, invalid pruning geometry, and both file-organization query
shapes. This experiment adds no CI workflow or benchmark timing job.
