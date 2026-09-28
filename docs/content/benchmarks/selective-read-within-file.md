---
title: Compare pruning inside files
description: Generate fixed row-group and page controls, validate their geometry, and compare all five readers.
---

# Compare pruning inside files

Use these three no-DV controls to measure work inside files that survive file
pruning. They supplement the TPC-H-derived query matrix with synthetic rows
whose physical placement is fixed by the
[public protocol](selective-read-protocol.md#file-and-within-file-controls-seven-without-dvs).

Every query filters on `event_id = 'match'` and returns `row_id` plus 16 nullable
string payload columns. The predicate column is absent from the output. This
separates the cost of finding matches in one small column from reading and
decoding the much wider output.

| Case | Files | Groups per file | Matching rows | Intended skipping opportunity |
| --- | ---: | ---: | ---: | --- |
| `row-groups.select` | 16 | 16 | 65,536 | All files survive; only group 7 in each file matches |
| `pages.localized` | 1 | 2 | 64 | All groups survive; matches occupy the first of 32 pages per group |
| `pages.scattered` | 1 | 2 | 64 | All groups survive; one match in every page |

Groups contain 4,096 rows. Both page cases use 128-row pages, uncompressed data,
and no dictionary encoding. The row-group case uses the public Zstd/dictionary
settings. Column and offset indexes are present in every case. The page cases
have equal qualifying counts but different row IDs and payload values.

## Generate and check the controls

From the repository root, write the fixtures outside the checkout:

```sh
RUSTFLAGS='-C target-cpu=x86-64' CARGO_TARGET_DIR=target/selective-read \
  cargo +1.98.1 run --release --locked -j8 \
  --manifest-path benches/selective_read/Cargo.toml -- \
  --controls --profile smoke --output ../selective-read-controls

for case in row-groups.select pages.localized pages.scattered; do
  ../selective-read-oracle-venv/bin/python -B benches/selective_read/oracle.py prepare \
    --fixtures ../selective-read-controls --case "$case" \
    --output "../selective-read-control-references/$case"
done
```

Use the [pinned oracle environment](selective-read-oracle.md). `--controls`
generates only these three tables and is mutually exclusive with `--repack-from`
and `--dv-from`.
Their geometry is identical under smoke, development, and report profiles; the
profile selects preparation resource limits. These synthetic tables have no
TPC-H scale factor.

The generator streams bounded batches through the existing Parquet/Delta writer.
It rereads every column to check Delta statistics and saves actual footer,
row-group, column-index, offset-index, and page-boundary metadata with object
checksums. Writer overrides are recorded on each table. No output directory is
overwritten, and a failed generation has no completion manifest.

The independent oracle derives expected row IDs, strings, and null positions
from the protocol's formulas. It full-reads the files with PyArrow, verifies
match placement and row order, and compares all qualifying values in SQLite.
It also checks that file statistics retain every file, the row-group case has
exactly one candidate group per file, and the page cases retain both groups.
Every column in a page fixture must have 32 indexed pages of 128 rows.

Each `reference.json` contains `within_file_geometry`, the exact SQL, output
projection, counts, and reference/object hashes. Candidate groups/pages describe
what the input permits a reader to skip. They are not measurements of what a
reader actually decoded.

## Run the five readers

Prepare the five [pinned native builds](selective-read-runners.md), then start
the [dedicated MinIO server](selective-read-storage.md). Finish preparation and
compilation before the timing campaign:

```sh
python3 -B benches/selective_read/storage.py upload \
  --state ../selective-read-storage --fixtures ../selective-read-controls \
  --output ../selective-read-controls-upload.json

../selective-read-oracle-venv/bin/python -B benches/selective_read/campaign.py \
  --state ../selective-read-storage --fixtures ../selective-read-controls \
  --upload ../selective-read-controls-upload.json \
  --output ../selective-read-controls-campaign \
  --binary ../selective-read-dar-build/selective-read-dar \
  --binary ../selective-read-delta-rs-build/selective-read-delta-rs \
  --binary ../selective-read-duckdb-build/selective-read-duckdb \
  --binary ../selective-read-polars-build/selective-read-polars \
  --binary ../selective-read-daft-build/selective-read-daft \
  --case row-groups.select --case pages.localized --case pages.scattered \
  --reference ../selective-read-control-references/row-groups.select \
  --reference ../selective-read-control-references/pages.localized \
  --reference ../selective-read-control-references/pages.scattered \
  --no-sessions
```

`--no-sessions` selects only these isolated open-and-query cases. The existing
[campaign](selective-read-campaign.md) checks every reader's complete output
before freezing its runnable subset and schedule. All five readers remain in
the inventory, including capability rejections and failures. With five eligible
readers, each case gets ten independent samples per reader. Result checking,
hashing, and detailed diagnostics remain outside those query intervals.

## Interpret latency, bytes, and requests together

Read `summary.json` for medians, quartiles, IQR, resource use, failure statuses,
and separate I/O diagnostic observations. `observations.jsonl` links the native
plans and raw records. Storage observations report requests and bytes by object
class, including Delta logs, and distinguish full/range GETs and HEADs. A footer
request counts as touching a file even if most of its data was skipped.

The adapters do not expose comparable decoded-group or decoded-page counters.
The reference metadata leaves those fields null with a reason. Native plans,
candidate geometry, and request traces remain available; none is silently
converted into a decoded-page count.

Few predicate columns can keep predicate decoding small. Many wide output
columns make the subsequent page reads more expensive. Localized matches leave
most output pages unused, while scattered matches require every output page
even at the same row selectivity. Keep the scattered case when indexes save no
bytes or add latency.

Fewer bytes can require more requests. The existing
[range-planning experiment](range-planning.md) explains the tradeoff between
coalescing nearby ranges and transferring unused bytes under different request
latencies. Compare request counts alongside bytes and time; these controls do
not add another network-latency matrix.

The separate DAR [predicate-projection A/B](row-filter.md) deliberately gives a
predicate either its referenced columns or extra payload columns. The
[offset-index A/B](page-index.md) compares indexed and unindexed files. These
artificial controls explain mechanisms and are not delta-rs baselines. Their
local scan/decode timers also differ from the five-reader open-and-query timer.

Retain the generated Parquet objects for the
[paired DV experiments](selective-read-deletion-vectors.md). The
bounded generator check exercises actual group/page boundaries, upper-level
survival, result geometry, and reproducible bytes. This work adds no CI job or
benchmark timing step.
