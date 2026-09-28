---
title: Public selective-read benchmark protocol
description: Frozen inputs, query families, reader settings, and measurement contracts for the public selective-read comparison.
---

# Public selective-read benchmark protocol

Protocol ID: `selective-read-v1`. Frozen on September 27, 2026, for
[#313](https://github.com/mag1cfrog/delta-arrow-reader/issues/313), under
the [benchmark roadmap](https://github.com/mag1cfrog/delta-arrow-reader/issues/312).
This is the implementation contract for that roadmap. It contains no new
performance results. Generators, resolved runner lockfiles, instrumentation,
and measurements are delivered by the subsequent native sub-issues.

The comparison tests how much work each reader avoids through file skipping,
within-file pruning, predicate decoding, projection, and early termination.
Its primary matrix has 30 public cases without deletion vectors (DVs), followed
by seven no-DV controls and seven DV extensions. Three metadata-reuse session
profiles are reported separately. No minimum speedup is a completion criterion.

## Questions and existing evidence

The [selective S3 case study](selective-s3.md#table-shape) motivates the public
query shapes: two equality predicates plus `IN`, projections around 70 columns,
thousands of active files, and few selected files. Historical Q2 projected 69
columns and retained five of over 18,000 files; its reported transfer was 23.459
MiB against 418.6 MiB of selected-file size. Q4 projected 71 columns and retained
six of over 3,000 files. The corresponding historical speedups over the tested
delta-rs revision were 71.75x and 30.48x.

Those artifacts do not identify per-table DV status or provide equivalent
delta-rs byte counters. They cannot assign those speedups to DVs or a particular
pruning layer. Public fixtures reproduce the workload shapes and questions;
they do not reproduce the private data or promise its speedups.

Report predicate structure, actual selectivity, projected column count, and
physical organization as separate dimensions. More predicates may expose more
skipping opportunities or add evaluation cost. This experiment does not test
the general claim that arbitrary, more complex SQL gives DAR a larger advantage.
It is a scan comparison derived from TPC-H, not a TPC-H score or a join and
aggregation benchmark.

## Pinned sources and build

| Component | Pin |
| --- | --- |
| DAR starting source | Version 0.6.1, commit `d5557f36bc51831abadc7d9a98c3b24381530b69`; root `Cargo.lock` |
| delta-rs | Rust release `1.0.0`, commit `41f1ce23377f088298a3ed196ad6c8e40cb9bfed`; `deltalake = "=1.0.0"`, default features off, `datafusion,s3,rustls` on |
| Common Rust toolchain | `1.98.1`, target `x86_64-unknown-linux-gnu` |
| Source generator | `tpchgen = "=3.0.0"`, upstream commit `4f6bf4c5ab40511c8fdef5888fc8d022e5e546d7` |
| Fixture writer | Arrow and Parquet Rust crates `=58.4.0`; use the generator's core row API, without an Arrow 59 dependency |
| Storage server | MinIO `RELEASE.2025-09-07T16-13-09Z`, commit `07c3a429bfed433e49018cb0f78a52145d4bedeb` |

The [generator source](https://github.com/datafusion-contrib/tpcgen-rs/tree/4f6bf4c5ab40511c8fdef5888fc8d022e5e546d7),
[delta-rs release source](https://github.com/delta-io/delta-rs/tree/41f1ce23377f088298a3ed196ad6c8e40cb9bfed),
and [MinIO release](https://github.com/minio/minio/releases/tag/RELEASE.2025-09-07T16-13-09Z)
are immutable source references. MinIO may be built from that source with Go
`1.24.7`, `CGO_ENABLED=0`, `GOOS=linux`, `GOARCH=amd64`, and `GOAMD64=v1`;
save the build command and executable SHA-256 before using it. Do not depend
on a mutable container tag or a personal cached executable.

Build two thin Rust runners in separate dependency graphs. DAR enables its
`datafusion` feature and uses its normal provider. delta-rs uses its released
provider. The starting graphs use DataFusion 54.1.0 / Arrow 58.4.0 for DAR and
DataFusion 55 / Arrow 59 for delta-rs. Matching their major versions would
require changing a product and is outside this comparison.

Both runners use `--release --locked`, `opt-level=3`, `codegen-units=16`,
`lto=false`, `debug=false`, `strip=none`, `panic=unwind`, and
`incremental=false`. Set `RUSTFLAGS=-C target-cpu=x86-64`; do not use native CPU
tuning, PGO, or a different allocator on one side. Use eight build jobs. Save
`rustc -vV`, Cargo version, target/linker details, feature lists, `cargo tree`,
lockfile hashes, executable hashes, and the full measured DAR and harness Git
commits. The starting DAR pin is not a claim that later runner code already
exists at that commit. Product changes require a separately identified revision.

## Data and physical layout

### Profiles and schema

| Profile | Original lineitem | Wide derivative | Purpose |
| --- | --- | --- | --- |
| `smoke` | SF 0.01 | SF 0.01 | Bounded public-data validation, separate smoke literal rule below |
| `development` | SF 1 | SF 1 | Validate all public query shapes |
| `report` | SF 10 | SF 1 | Published primary comparison |

File-count derivatives always use original SF1 data. Mechanism controls have
the fixed synthetic geometry below and no TPC-H scale factor. Profiles do not
multiply the 44 report case definitions into additional reported cases.

Generate rows with `tpchgen::generators::LineItemGenerator::new(sf, 1, 1)`, using
the pinned default distributions, seeds, and text pool. Save source rows in
generator order as a full-read reference before sorting or adding payloads.
Generation parallelism must not change the resulting logical rows or ordering.

All 16 original fields are non-null. Their order and Arrow logical types are:

| Columns, in source order | Type |
| --- | --- |
| `l_orderkey`, `l_partkey`, `l_suppkey` | `Int64` |
| `l_linenumber` | `Int32` |
| `l_quantity`, `l_extendedprice`, `l_discount`, `l_tax` | `Decimal128(15,2)` |
| `l_returnflag`, `l_linestatus` | UTF-8 string |
| `l_shipdate`, `l_commitdate`, `l_receiptdate` | `Date32` |
| `l_shipinstruct`, `l_shipmode`, `l_comment` | UTF-8 string |

Preserve original values, including text and trailing spaces. Convert quantity
to unscaled integer `quantity * 100`; convert the other Decimals from the
generator's exact base-10 values, without a floating-point intermediate. Dates
are days since 1970-01-01. Delta types are `long`, `integer`, `decimal(15,2)`,
`string`, and `date`, respectively. Reader string/view representations may
differ while retaining the same logical type. `(l_orderkey, l_linenumber)` is
the unique logical row key; validate uniqueness.

The `lineitem-wide` derivative retains these fields and appends nullable `Int64`
columns `payload_00` through `payload_63`, in ascending index order. For column
index `j`, form UTF-8 `dar-wide-v1/{orderkey}/{linenumber}/{j:02}` using decimal
integers, no padding on the keys, no newline. Let `u` be the first eight SHA-256
bytes interpreted as unsigned little-endian. The value is null if `u % 17 == 0`;
otherwise it is `(u & 0x7fffffffffffffff) - 0x4000000000000000`.
These 64 fields are synthetic, not TPC-H columns.

### Ordering and writer settings

Both layouts contain the same logical row multiset:

- `clustered`: ascending `l_shipdate, l_shipmode, l_partkey, l_orderkey,
  l_linenumber`. Compare strings by UTF-8 bytes.
- `shuffled`: ascending SHA-256 digest bytes of UTF-8
  `dar-shuffle-v1/20260927/{orderkey}/{linenumber}`, then ascending row keys
  to break hash ties. This fixes the seed and permutation without relying on
  a library-specific random-number generator.

For a given source scale, original and wide tables use the same row order and
file boundaries. No Hive partitions or partition columns are present.

| Writer property | Fixed value for public tables |
| --- | --- |
| Rows per group / groups per file | 131,072 / 8; final group and file may be shorter |
| Input Arrow batch size | 8,192 rows, split at file boundaries |
| Parquet writer version | `PARQUET_1_0` |
| Data-page byte target / row limit | 1,048,576 bytes / 20,000 rows |
| Writer batch size | 1,024 rows |
| Compression | Zstd level 3 |
| Dictionary encoding / dictionary page limit | Enabled / 1,048,576 bytes |
| Statistics | `EnabledStatistics::Page`; no statistics or column-index truncation |
| Offset indexes / Bloom filters | Enabled / disabled |
| Maximum row-group bytes | Unset; row-count boundary controls flushing |
| `created_by` | `dar-selective-read-v1 parquet-rs 58.4.0` |

Use `part-00000.parquet`, `part-00001.parquet`, and so on. Write groups and files
in the specified order, independent of task completion order. Save the resolved
writer options and lockfile, including compression dependencies. Inspect actual
page/group boundaries, encodings, indexes, sizes, and statistics; page targets
are not promises about the physical result. Regeneration must reproduce object
SHA-256 values. A change in writer output is a new fixture revision.

Write snapshot version 0 as one JSON transaction, with reader/writer protocol
versions 1/2, empty table configuration, no DVs, and no checkpoints. Include
physical `numRecords`, exact `minValues`, `maxValues`, and `nullCount` for all
columns in every Add action, including all 80 wide-table fields. Do not use
the common first-32-columns statistics limit. Use metadata `createdTime=0` and
Add `modificationTime=0`; IDs are UUIDv5 with the URL namespace and name
`https://github.com/mag1cfrog/delta-arrow-reader/selective-read-v1/{profile}/{fixture-id}`.
Save schemas, relative object paths, sizes, log bytes, and hashes. Location and
upload timestamps are recorded separately and do not affect fixture identity.

Fixture IDs are `li.clustered`, `li.shuffled`, `wide.clustered`, `wide.shuffled`,
`files64`, `files4096`, `row-groups`, `pages.localized`, and `pages.scattered`.
DV and feature-only fixtures append `.dv` and `.feature-only` to their base
fixture IDs. Reference files use the same public writer settings under
`sf{scale}/source/part-00000.parquet`, and so on, in original generator order.
The scale directory keeps the report profile's SF10 and SF1 references separate.
See [Generate public fixtures](selective-read-fixtures.md) for preparation commands.

### Preparation budget

Generate one fixture at a time and reuse saved source rows. The following are
hard preparation ceilings, not measured resource estimates:

| Profile | Generator/sort memory | Data, spill, and MinIO-copy disk budget |
| --- | --- | --- |
| `smoke` | 4 GiB | 8 GiB |
| `development` | 16 GiB | 64 GiB |
| `report`, including SF1 derivatives and controls | 16 GiB | 192 GiB |

Allow a separate 64 GiB build directory and 16 GiB build-memory budget. The
generator must stream or spill within its limit, preflight available disk, and
stop with a recorded error before exceeding the configured disk budget. Do not
silently reduce scale, payload width, or file counts. Bulk data stays outside
Git. Record actual peak memory, temporary disk, and final sizes in preparation
artifacts; preparation is never timed as a reader query.

## Query and case IDs

Register each selected snapshot as `bench`. SQL is exactly
`SELECT {projection} FROM bench`, followed by ` WHERE {predicate}` when present
and ` LIMIT 100` only where specified. Expand projection lists explicitly in
the stated order. Do not add ordering or aggregates to the measured SQL.

| Projection name | Ordered columns |
| --- | --- |
| `keys` | `l_orderkey, l_linenumber` |
| `original` | The 16 original columns in schema order |
| `q6-output` | `l_orderkey, l_linenumber, l_extendedprice, l_discount` |
| `wide69` | `l_orderkey, l_linenumber, l_shipdate, l_shipmode, l_partkey`, then `payload_00` through `payload_63` |
| `control17` | `row_id`, then `payload_000` through `payload_015` |

These predicate names expand to the following SQL:

| Name | SQL |
| --- | --- |
| `empty` | `l_shipdate < DATE '1990-01-01'` |
| `date7` | `l_shipdate >= DATE '1995-03-15' AND l_shipdate < DATE '1995-03-22'` |
| `eq1` | `l_shipdate = DATE '1995-03-15'` |
| `eq2` | `l_shipdate = DATE '1995-03-15' AND l_shipmode = 'AIR'` |
| `eq2-in1` | `{eq2} AND l_partkey IN ({first_literal})` |
| `eq2-in20` | `{eq2} AND l_partkey IN ({twenty_literals})` |
| `q6` | `l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1995-01-01' AND l_discount BETWEEN CAST('0.05' AS DECIMAL(15,2)) AND CAST('0.07' AS DECIMAL(15,2)) AND l_quantity < CAST('24.00' AS DECIMAL(15,2))` |
| `control-match` | `event_id = 'match'` |

Before either reader is timed, take the first 20 distinct ascending `l_partkey`
values from source rows satisfying `eq2`, independently for each scale. The
one-value case takes the first of those values. Render decimal integer literals
in ascending order and save both the list and fully expanded SQL. Development
and report generation fails if fewer than 20 exist. Both layouts, the wide
derivative, and repacked files at the same scale share that list. Never select
literals by observed reader performance.

Only `smoke` uses `min(20, available_distinct_values)` for the `eq2-in20` shape;
it still fails on an empty list. Label its SQL `smoke`, record the actual count,
and never report it as a 20-value performance case. As an untimed correctness
check, repeat the first literal at the end of each list and verify that duplicate
`IN` literals do not duplicate output rows.

### Primary public cases: 30 without DVs

Each original query expands to two IDs, `li.clustered.{query}` and
`li.shuffled.{query}`. Each wide query expands to `wide.clustered.{query}` and
`wide.shuffled.{query}`. Use the profile's original/wide scale and snapshot 0.

| Original query | Projection | Predicate | Limit |
| --- | --- | --- | --- |
| `all-keys` | `keys` | None | None |
| `all-full` | `original` | None | None |
| `empty` | `original` | `empty` | None |
| `date7-full` | `original` | `date7` | None |
| `date7-keys` | `keys` | `date7` | None |
| `date7-limit` | `original` | `date7` | 100 |
| `q6-scan` | `q6-output` | `q6` | None |
| `eq2-in1` | `original` | `eq2-in1` | None |
| `eq2-in20` | `original` | `eq2-in20` | None |

The older planning name `F7` means the seven-day predicate above. It is not a
TPC-H query number. Use `date7-full`, `date7-keys`, and `date7-limit` in artifacts.

| Wide query | Projection | Predicate | Limit |
| --- | --- | --- | --- |
| `eq1` | `wide69` | `eq1` | None |
| `eq2` | `wide69` | `eq2` | None |
| `eq2-in1` | `wide69` | `eq2-in1` | None |
| `eq2-in20` | `wide69` | `eq2-in20` | None |
| `eq2-in20-keys` | `keys` | `eq2-in20` | None |
| `all-wide` | `wide69` | None | None |

The first runnable milestone is `li.clustered.eq2-in20` and
`li.shuffled.eq2-in20`, at SF1, passing the independent oracle with both readers.
The wide predicate progression reports row counts after each predicate step.
The matched narrow/wide pair reads the same wide files with the same filter.

### File and within-file controls: seven without DVs

| Case ID | Fixture | Projection | Predicate |
| --- | --- | --- | --- |
| `files64.empty` | Clustered original SF1, 64 files | `original` | `empty` |
| `files64.eq2-in20` | Same 64 files | `original` | `eq2-in20` |
| `files4096.empty` | Clustered original SF1, 4,096 files | `original` | `empty` |
| `files4096.eq2-in20` | Same 4,096 files | `original` | `eq2-in20` |
| `row-groups.select` | Synthetic row-group fixture | `control17` | `control-match` |
| `pages.localized` | Synthetic localized-page fixture | `control17` | `control-match` |
| `pages.scattered` | Synthetic scattered-page fixture | `control17` | `control-match` |

For exactly `F` files and `N` clustered source rows, file `i` receives sorted
ordinals `[floor(i*N/F), floor((i+1)*N/F))`, with zero-based `i`. Retain the
public writer settings, splitting groups at file boundaries. Repacking changes
footer count, statistics granularity, and group/page geometry along with file
count; do not label this a one-variable file-count experiment.

All synthetic controls use non-null `row_id: Int32`, non-null `event_id: Utf8`,
and 16 nullable UTF-8 payload columns. Reuse the
[existing page fixture's payload rule](https://github.com/mag1cfrog/delta-arrow-reader/blob/d5557f36bc51831abadc7d9a98c3b24381530b69/benches/page_index.rs):
`payload_{j:03}` is null when `(row_id + j) % 17 == 0`; otherwise its string is
`payload-{j:03}-{row_id:08}-` followed by 12 repetitions of
`abcdefghijklmnopqrstuvwxyz0123456789`. Rows appear in ascending `row_id` order.

For `row-groups.select`, write 16 files, each with 16 groups of 4,096 rows.
Global `row_id = ((file_index * 16 + group_index) * 4096) + row_in_group`.
`event_id` is `match` in group index 7 and `other` elsewhere. Use the public
writer settings except for group/file geometry. All files survive file-level
statistics; exactly one group per file qualifies. Expected output is 65,536
rows. The predicate column is excluded from the 17-column output.

For each page fixture, write one file with two groups of 4,096 rows and 128-row
pages. Localized matches have `row_in_group < 32`; scattered matches have
`row_in_group % 128 == 0`. Other rows have `event_id='other'`. Both yield 64
rows and retain every file and group. Override compression to uncompressed,
dictionary encoding to false, and writer-batch/page-row limits to 128, matching
the existing control. Keep indexes enabled and validate actual boundaries.

Existing DAR narrow/wide predicate-decode and indexed/unindexed experiments
remain separate mechanism A/Bs, not delta-rs substitutes or extra public cases.
Use the [range-planning experiment](range-planning.md) to explain byte/request
tradeoffs without introducing a new cross-reader network matrix.

### DV extensions: seven cases

The following six case IDs append `.dv` to their no-DV base ID:

| Base case | Deletion rule |
| --- | --- |
| `li.clustered.date7-full` | Public logical-row rule |
| `li.shuffled.date7-full` | Public logical-row rule |
| `wide.clustered.eq2-in20` | Public logical-row rule |
| `li.clustered.date7-limit` | Public logical-row rule |
| `row-groups.select` | Controlled nonmatching-row rule |
| `pages.localized` | Controlled nonmatching-row rule |

Public rule: SHA-256 the UTF-8 string `{l_orderkey}/{l_linenumber}` with decimal
keys and no newline. Delete when the first eight digest bytes, unsigned
little-endian, modulo 1,000 equal zero. This preserves the deletion set across
layouts. Controlled rule: delete rows with `event_id='other' AND row_id % 1000=0`.
Controlled paired outputs therefore stay identical.

Each extension has its own table prefix, the same Parquet bytes as its base,
and snapshot version 1 adding reader/writer protocol 3/7 with `deletionVectors`
in both feature lists and `delta.enableDeletionVectors=true` in table
configuration. Attach descriptors only to files with a nonempty
deletion set. Use the Delta portable Roaring bitmap format supported by pinned
Kernel 0.25.0, with sorted physical ordinals and one on-disk DV per affected
file. Use deterministic UUIDv5 names from the table ID and relative Parquet
path, and record payload bytes, hashes, offsets, sizes, and cardinalities.
Retain physical `numRecords`; maintain conservative statistics and set
`tightBounds=false` on DV-bearing Add actions. Save logical deleted keys and
their zero-based per-file physical ordinals for the independent oracle.

The seventh case, `row-groups.select.feature-only`, uses a separate version-1
snapshot with DV protocol features enabled and no DV descriptors. It keeps
the same enabled table property, original row set, and Parquet files. Record actual deletion density,
DV-bearing file coverage, and changed output volume for each DV case. Add
untimed checks for matching-row deletions and page/group/batch boundaries.

The delta-rs source audit is context for these extensions. Version 1.0.0
[retains Kernel file skipping](https://github.com/delta-io/delta-rs/blob/41f1ce23377f088298a3ed196ad6c8e40cb9bfed/crates/core/src/delta_datafusion/table_provider/next/scan/plan.rs#L268-L296).
Its [Parquet predicate and limit setup](https://github.com/delta-io/delta-rs/blob/41f1ce23377f088298a3ed196ad6c8e40cb9bfed/crates/core/src/delta_datafusion/table_provider/next/scan/mod.rs#L699-L772)
depends on DVs in surviving files. These guards do not establish a full scan
or explain DAR's no-DV performance; compare runtime work before attributing cost.

## Reader configuration and cache policy

Use the same immutable objects and explicit snapshot version per case. No
pre-filtered file list, per-case tuning, SQL rewrite, or unmeasured table open
is allowed. Stream batches to completion and count rows without collecting,
hashing, sorting, printing, or retaining them in measured mode.

| Shared setting | Value |
| --- | --- |
| Reader CPU budget | Eight logical CPUs, fixed affinity for both readers |
| Tokio worker / maximum blocking threads | 8 / 64 |
| DataFusion target partitions / batch size | 8 / 8,192 |
| Reader process memory / DataFusion pool | 8 GiB cgroup limit / 4 GiB pool |
| `execution.parquet.pruning` / `enable_page_index` | `true` / `true` |
| `execution.parquet.pushdown_filters` / `reorder_filters` | `true` / `true` |
| `execution.parquet.schema_force_view_types` | `true` |
| `execution.parquet.metadata_size_hint` | 65,536 bytes |
| `optimizer.repartition_file_scans` | `true` |
| DataFusion file metadata cache | 64 MiB limit per new runtime |
| DataFusion file-statistics / list-files caches | Disabled with zero limits |
| Result cache / extra object-data cache | None |

DataFusion settings above use the `datafusion.` prefix when serialized. Keep
other options at the pinned versions' defaults and save their complete resolved
values. Provider-specific implementations may ignore generic Parquet settings;
record effective behavior and plans instead of assuming that equal option names
mean equal execution.

DAR uses `DeltaTableBuilder`, `datafusion::register_table`, and the Direct
Parquet backend. Set `ScanOptions.target_partitions=Some(8)` and retain
`WhenBelowTarget` intra-file repartitioning and Arrow view types. Fix scan-wide
active reads at 24, per-partition reads at 3, output buffering at 1 batch,
prefetch depth at 2, footer hint at 65,536 bytes, and full-file buffering off.
Use `WarmupMode::None` for open-and-query and `QueryPlanning` for reuse.

delta-rs uses the public `delta_datafusion::DeltaScanNext::builder()` and its
`TableProviderBuilder` path with a registered object store and session. For
open-and-query, supply the log store and explicit version, allowing the builder
to load the ordinary snapshot. For reuse, load an `EagerSnapshot` and use
`with_eager_snapshot`, including that load in initialization. Pass the configured
session with pushdown explicitly enabled: `DeltaScanConfig::new_from_session`
copies that option, and a default DataFusion session would disable it.
Verify that pushdown and view types
are enabled in the resulting config. Keep its normal supported pruning paths.

Every process starts with fresh client connections, runtime, and application
caches. Reuse sessions retain their snapshot/provider/session and native caches
for ten executions; each execution plans the SQL anew. Do not reuse a prepared
physical plan. DAR's Direct backend does not gain a footer cache merely by
setting DataFusion's cache limit. Disclose which caches each provider actually
uses and any remaining metadata requests.

Use a dedicated single-node, single-data-directory MinIO on local Linux,
bound to loopback HTTP, with a fixed bucket and path-style requests. Assign the
server two logical CPUs and 4 GiB process memory, separate from reader CPUs;
place a diagnostic observer on a separate CPU. Do not share physical-core SMT
siblings between reader and server/observer sets. Record masks, CPU model,
SMT topology, kernel, RAM, storage/filesystem, power policy, server build and
settings, and any resource contention. A smaller host is a separately named
environment, not the report profile.

Restart MinIO once before the campaign, verify uploaded checksums, and retain
it between runs. Do not flush OS caches or restart the server between readers.
The profile is **fresh client with reused server/OS caches**, not a cold-disk
test or a promise that every object remains cached. Warmup history is recorded.
Describe results as local S3-compatible reads, not measurements of AWS S3.

## Correctness gate

The oracle reads saved source rows or all reference Parquet rows, bypassing
both tested Delta providers' pruning and DV paths. Evaluate the fixed predicates
and projections independently using exact Decimal/date semantics. Apply the
saved logical deletion set for DV cases. Compute qualifying rows, selectivity
after each predicate step, expected matching files, and statistics-based
candidate files separately; candidate files can contain no matching rows.

Compare ordered output field names and logical types, full values, nulls, and
duplicate multiplicity. Accept equivalent UTF-8/view/dictionary representations;
record reported nullability separately, require the expected null values, and
reject lossy type/Decimal conversions. Use exact unordered multiset comparison,
sorting externally for large results rather than defining a probabilistic
fingerprint in this version. All selected projections contain a unique row key,
but the checker must reject duplicated keys/rows as well as missing rows.

For unordered `LIMIT 100`, require `min(100, qualifying_live_rows)` rows with
valid membership, multiplicity, and complete values. Different valid subsets
are allowed. Equal row counts alone never pass. Include small negative checks
for a changed value at the same row count, wrong Decimal scale, changed nulls,
missing/duplicated rows, and wrong snapshots. Full logical results must agree
across layouts for unlimited queries.

Validation and diagnostic plans are separate invocations. Save oracle status
against the protocol, fixture, SQL, and executable hashes; a changed input or
build invalidates the gate. Failed or unsupported cases remain visible and
cannot produce speedups.

## Timing and observations

### Open-and-query and reuse boundaries

Create the process/runtime and parse arguments before starting the monotonic
query clock, with no table I/O. Start `open_query_ns` immediately before opening
the snapshot; include provider construction, registration, SQL parsing/planning,
and consumption through end-of-stream. `first_batch_ns` uses that same start
and stops at the first nonempty batch. It is null for empty results. Count rows
as batches pass and drop each batch before requesting the next.

For reuse, start `initialization_ns` before snapshot load and end it after eager
metadata/provider registration. Then execute ten queries with that registration,
recording each interval from SQL planning through end-of-stream and its first
nonempty batch time. Report initialization plus query 1 and initialization plus
all ten queries. Also retain the enclosing session elapsed time so bookkeeping
gaps are visible. Ten queries in one process are one independent session sample.

| Session ID | Repeated no-DV case | Report data |
| --- | --- | --- |
| `reuse.li` | `li.clustered.eq2-in20` | Original SF10 |
| `reuse.wide` | `wide.clustered.eq2-in20` | Wide SF1 |
| `reuse.files4096` | `files4096.eq2-in20` | Original SF1 repacked |

After end-of-stream, drop the stream, finish pending task/runtime cleanup, and
wait for process exit before the next run. Record `cleanup_ns` separately.
Storage capture lasts through cleanup, including canceled or post-LIMIT work.
Do not hide that work by cutting off observations at the final output batch.
The query clock excludes process startup/shutdown; whole-process CPU and peak
RSS include them and are labeled accordingly.

### Repetition and cache history

For each case or session, execute one unreported warmup pair A/B followed by
one B/A pair, each using fresh processes. Then measure ten fresh-process pairs,
alternating A/B and B/A, yielding ten samples per reader and five of each
order. A is DAR and B is delta-rs. Reuse `benches/run_order.py`'s two-candidate
orders when constructing the schedule; persist the expanded schedule before
running. Visit cases in lexical case-ID order, then sessions in lexical order.

Do not run queries concurrently across processes. Each query and each reuse
initialization has a 1,800-second deadline; cleanup has a 60-second deadline. Keep partial observations,
timeouts, failures, and stderr. Do not discard outliers or retry failed samples
into their original slots. A rerun gets a new campaign ID. Compilation,
generation, verification, charting, and unrelated load stay outside the campaign.

Report median, Q1, Q3, and `IQR=Q3-Q1`, using linear interpolation at index
`(n-1)*p` in sorted samples. Compute a speedup only for matching cases that
passed correctness checks and have all ten successful samples on both sides:
`median(delta-rs) / median(DAR)`. Retain losses and ties; durations below timer
resolution do not yield a numeric speedup.

### Common I/O and metric definitions

Timing runs have no detailed request tracing. Run two separate diagnostic pairs
(A/B, then B/A) per case/session after the timing campaign, with the same settings
and one preceding warmup pair in each order. Give them distinct run IDs. This
keeps trace overhead out of headline timings. Report diagnostic latency and
observer overhead on the same queries, but do not join their bytes and timings
as though they belonged to a single observation.

At the common storage boundary, capture operation, relative object key, Range,
status, actual response-body bytes, retries, and request start/end. Separate
Delta log/checkpoint, Parquet, and DV traffic. Verify full GET, range GET, HEAD,
LIST, retries, and interrupted responses with known requests. Content-Length
is not evidence that the complete body reached the measured boundary.

| Metric | Definition |
| --- | --- |
| Active files | Data files in the selected snapshot before query pruning |
| Candidate files | Files retained by a stated planning layer, with counter/source identified |
| Matching files | Files containing at least one qualifying live row, from the oracle |
| Touched Parquet objects | Distinct Parquet keys with observed requests; also report GET and HEAD subsets |
| Requests | Actual operations/attempts, including retries; split by type/status/object class |
| Response bytes | Actual body bytes crossing the documented observer boundary, including partial/canceled transfers |
| Selected-file bytes | Sum of full sizes of identified candidate files; not transferred bytes |
| Output | Rows and projected columns; distinguish qualifying rows before LIMIT |
| Decode work | Groups/pages/rows decoded only when a reliable, defined counter exists |
| Resources | Whole-process user/system CPU time and peak RSS bytes; separate from query timers |

An object opened for a footer is touched even if no data pages are needed.
Internal tasks are not files or requests. A low byte count alone does not prove
which layer pruned the data. Use plans, independent expected candidates, common
I/O, and available internal counters together. Missing or unsupported metrics
are null with a reason, never zero.

Each raw JSONL observation must identify protocol revision/hash, campaign and
run ID, timing/diagnostic mode, case/profile/session and query index, repetition
and order, reader/source/build/config identity, fixture manifest/hash, snapshot,
expanded SQL/hash, correctness prerequisite, status/error, output rows, timers,
resource metrics, and linked diagnostic artifacts. Store nanoseconds and bytes
as integers. Preserve raw logs and complete resolved settings without secrets.

## Changes and delivery gate

The frozen inventory is `18 + 12 + 4 + 1 + 2 + 6 + 1 = 44` cross-reader cases,
plus three reuse profiles. #314 supplies the generator and physical manifests;
#315 the oracle; #316 the locked runners; #317 common I/O; and #318 scheduling.
The remaining native sub-issues integrate query families, controls, DVs,
reproduction, and publication in the order recorded by the parent.

Generated literals, object checksums, actual page metadata, dependency
inventories, and machine identities are outputs of these fixed rules, not
values to guess in advance. Freeze and validate those artifacts before timing.
If a rule cannot be implemented or a fixture misses its intended geometry,
stop that case and amend this protocol and the parent before using new results.
Record the reason, changed revision, and invalidated artifacts. Never replace
a slow or unsupported case silently. Review each completed sub-issue as one PR
before starting the next; product fixes remain separate changes.
