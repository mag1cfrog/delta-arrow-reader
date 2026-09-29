---
title: Q2/Q4-derived selective-read workloads
description: The main workload definition, its relationship to the private S3 queries, and the preparation still required.
---

# Q2/Q4-derived selective-read workloads

The main benchmark follows the published shapes of the private S3 case study's
Q2 and Q4. It measures selecting a few files from thousands, then retrieving
hundreds of matching rows across about 70 output columns. DAR, delta-rs,
DuckDB, Polars and Daft remain the five candidates, with ordinary and real-DV
snapshots. Reader timing does not determine which queries are included.

This is the current workload direction under
[#312](https://github.com/mag1cfrog/delta-arrow-reader/issues/312). It supersedes
using the 58-file SF10 compound query or the shuffled date30 scan as the main
selective-read evidence. Those results remain controls. The historical 4,096-file
64 MiB median requirement and automatic scale ladder no longer govern the new
workloads. Frozen protocol files and existing artifacts retain their identities.

The definitions, source-layout planner and bounded
[Delta generator](selective-read-production-fixtures.md) are implemented.
Each input still needs a completed generation manifest with physical geometry
checks. Exact references and native campaigns for these new cases are pending.
A successful layout plan or generation is not a performance result.

## Relationship to the historical queries

| Property | Historical Q2 | Public Q2 derivative | Historical Q4 | Public Q4 derivative |
| --- | --- | --- | --- | --- |
| Active files | Over 18,000 | 18,432 | Over 3,000 | 4,096 |
| Stored columns | Over 400 | 416 | About 90 | 90 |
| Projected columns | 69 | 69 | 71 | 71 |
| Filter shape | Two equalities and IN1 | Two equalities and IN1 | Two equalities and IN1 | Two equalities and IN1 |
| Selected files | 5 | 5-15 candidates | 6 | 6-18 candidates |
| Matching output | 718 rows | 300-2,000 rows, exact count recorded | 668 rows | 300-2,000 rows, exact count recorded |
| Active bytes | About 1.2 TiB | Measure the SF10 derivative | About 200 GiB | Measure the SF10 derivative |

The historical inputs and SQL remain private. These derivatives reproduce
published workload characteristics, not the private values, exact file sizes,
compression or reported speedups. The source remains TPC-H lineitem at SF10;
no repeated SF1 rows, synthetic empty files or padded files count as scale.
The historical measurements are in the
[S3 case study](selective-s3.md#table-shape).

## Query and stored columns

Both public queries use this fixed predicate:

```sql
WHERE l_shipdate = DATE '1995-03-15'
  AND l_shipmode = 'AIR'
  AND l_linenumber IN (1)
```

The one membership value follows historical Q2/Q4. The public mapping uses the
first line of an order rather than picking 20 rare part keys. Each condition
must reduce the SF10 source, and the untimed source check records every stage's
row count. These literals are chosen before any native reader timing.

Q2 returns `l_orderkey, l_linenumber, l_shipdate, l_shipmode, l_partkey` and the
existing 64 numeric payload columns. Q4 returns the same columns followed by
`l_suppkey, l_quantity`. Both consume the full result without COUNT or LIMIT.
The planner exports the complete explicit SELECT lists in `sql.json`.

Both tables preserve all 16 original columns and the existing 64 nullable
Int64 payloads. Q2 adds 336 stored Int64 columns named `metric_000` through
`metric_335`; Q4 adds the first 10. For zero-based column `j`, the value is null
when `(l_orderkey + l_linenumber + j) % 17 = 0`; otherwise it is
`(l_partkey + (j + 1) * l_suppkey + l_linenumber) % 1024`.
These are explicitly synthetic numeric dimensions with a small value domain.
They test a wide stored schema with a smaller projection; their compressibility
does not represent the undisclosed private columns. The 64 output payloads keep
their existing values and entropy.

## File membership and work inside files

Q2 has five independently clustered stripes; Q4 has six. Assign each original
row to `l_orderkey % stripe_count`. Within a stripe, sort by
`l_shipdate, l_shipmode, l_linenumber, l_orderkey`. Stripe `s` gets
`files // stripes + int(s < files % stripes)` files. For a zero-based ordinal
`r` among `N` rows in that stripe, its file index is `r * stripe_files // N`.
Every source row occurs once. The stripe is a layout rule, not a Hive partition
or an extra query predicate.

Conservative per-column statistics can retain a file containing no matching
row. Keep and report these false positives instead of arranging every candidate
file to contain a result. Record candidate and actually matching files separately.

The localized variant preserves this order. The scattered variant keeps the
same file and row-group membership, then orders rows within each group by
SHA-256 of UTF-8 `dar-production-scatter-v1/{orderkey}/{linenumber}`, breaking
ties by the unique source keys. Both return identical values. This pair tests
whether sparse matches leave output pages unread; it must remain in the report
when scattering eliminates the saving.

Use 4,096 rows per group, a 256-row data-page limit, 256-row writer batches and
a 1 MiB page-byte target, with the existing Zstd, dictionary, statistics and
offset-index settings.
Real pages can be shorter than their limits. Validate actual groups, pages,
match positions and file statistics from the written objects. Do not label a
planned page count or a selected-file size as observed reader work.

The core inventory is eight cases: Q2/Q4 x localized/scattered x no-DV/real-DV.
Pair DV snapshots on the same Parquet bytes, use the existing deterministic
logical deletion rule, and verify both deleted and surviving matches. Keep
nonempty DVs in selected and excluded files using the existing shared logical
deletion-union approach. Native reuse applies to these same eight cases.

## Capacity and timing checks before publication

Generate one representative file per stripe first with the actual pinned
writer. Include a boundary file containing matches and an ordinary interior
file when they differ. Use their measured sizes, footer/index sizes and row
counts to budget the full derivative, then check actual bytes as it is written.
Prepare one layout at a time under the existing 192 GiB allowance, with source,
sort spill, MinIO, exact references and validation exports accounted for.
Budget builds separately. A failed budget check leaves that case pending;
it does not authorize smaller file counts, narrower schemas or a larger SF.

First run two independent queries per supported reader on Q4 localized, then
the other declared cases. Record opening, initialization and two-query native
reuse separately. Formal sampling remains five independent samples. The
previous 58-file millisecond results and date30 scan cannot complete this pilot.

The historical DAR queries took about 1-4 seconds, including cases whose
delta-rs baseline took tens or hundreds of seconds. Do not force every reader
to take a minute. Accept input geometry and correctness independently of the
winner; if the new queries remain very short, report that and investigate the
measured work before making claims about overhead or representative latency.
Do not stretch a query with loops, sleeps, disabled optimizations or throttled
CPUs, or select literals after inspecting speedups.

Local MinIO is the storage control. Main remote-read claims additionally need
a shared object-store transport with recorded request latency and throughput.
Use the same endpoint and conditions for every reader; record real remote
storage or identify a controlled transport as emulation. Do not present
localhost timings as reproducing the historical public-S3 path. A transport
profile is fixed before timing and cannot be chosen to make DAR win.

Reports show log, Parquet and DV requests/bytes, candidate and touched files,
actual page/group geometry, output rows and widths, plus each timing boundary.
Missing comparable decode counters remain unavailable. Preserve failures,
unsupported capabilities, ties and losses, and both warm-source and first-open
observations. No new CI jobs or performance thresholds are needed.

## Check the public source mapping

Use the existing oracle environment and a new output directory:

```sh
../selective-read-oracle-venv/bin/python -B \
  benches/selective_read/production_shapes.py \
  --source ../selective-read-calibration-345/sf10 \
  --output ../production-shape-plan
```

The command verifies source-object identities, independently counts the three
predicate stages with Arrow, and computes proposed file membership with a
bounded DuckDB sort. Its output has `native_campaign_ready: false` and
`publication_ready: false`. It does not generate Delta files, read a reader's
query plan, measure Parquet traffic, or fill formal sample slots.
