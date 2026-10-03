---
title: Generate Q2/Q4-derived Delta fixtures
description: Probe compressed sizes, then generate one bounded SF10 layout with verified physical geometry.
---

# Generate Q2/Q4-derived Delta fixtures

This prepares the inputs defined in the [Q2/Q4 workload contract](selective-read-production-shapes.md).
Use a completed SF10 source manifest and the pinned oracle Python environment
from [fixture preparation](selective-read-fixtures.md). These commands generate
data and check its physical layout. They do not run a reader benchmark.

## Build and plan

Build the existing fixture binary with its locked dependencies:

```sh
CARGO_TARGET_DIR=target/selective-read cargo +1.98.1 build --release --locked \
  --manifest-path benches/selective_read/Cargo.toml

../selective-read-oracle-venv/bin/python -B \
  benches/selective_read/production_shapes.py \
  --source ../selective-read-calibration-345/sf10 \
  --file-target-mib 512 --page-rows 20000 \
  --output ../production-plan
```

The planner checks the original source and computes file boundaries before any
reader timing. Keep its output directory: the writer checks its hashes against
the current workload definition, source and planner. The default 512 MiB target
uses 60 Q4 files or 130 Q2 files. `--file-target-mib 256` doubles those counts.
These are approximate compressed-size targets; the probe records actual sizes.
The main setting is now `--page-rows 20000`; keep `--page-rows 2048` in a
separate plan directory as a sensitivity control. This choice follows the Q4
page comparison and supersedes the initial page recommendation in the frozen
shape contract. The writer can produce 20,480-row pages at batch boundaries.
Neither option changes source rows, query literals or projections. Replaying
an older plan requires its recorded planner, contract and writer; changing a
default does not relabel existing fixtures.

## Probe the compressed size

Start with Q4. The probe writes every candidate file, including false positives,
and one ordinary interior file per stripe, in both localized and scattered form:

```sh
../selective-read-oracle-venv/bin/python -B \
  benches/selective_read/production_fixtures.py probe \
  --source ../selective-read-calibration-345/sf10 \
  --plan ../production-plan/plan.json --shape q4 \
  --writer target/selective-read/release/selective-read-fixtures \
  --output ../production-q4-probe
```

The probe sorts the complete original source with DuckDB 1.5.5, streams its 16
columns to the Rust writer, and expands only the selected files. It uses the
existing payload function with 131,072-row groups, plain encoding, Zstd level 3
and the plan's page setting. It reads every written file back and compares all values and
nulls. It also checks Delta statistics and records actual page boundaries and
matching row positions. Probe samples have no Delta transaction log.

## Generate one complete layout

```sh
../selective-read-oracle-venv/bin/python -B \
  benches/selective_read/production_fixtures.py generate \
  --source ../selective-read-calibration-345/sf10 \
  --plan ../production-plan/plan.json --shape q4 \
  --writer target/selective-read/release/selective-read-fixtures \
  --probe ../production-q4-probe --layout localized \
  --output ../production-q4-localized \
  --disk-limit-mib 196608 --elapsed-limit-seconds 7200
```

Use a new output directory for every attempt. `capacity.json` records the
preflight estimate. It uses the largest measured bytes per row within each
stripe and adds 25% headroom. The estimate includes original and copied source,
the retained probe, geometry metadata, a bounded 16 GiB sort spill, and a later
MinIO copy. Other retained layouts or DV copies need an additional budget;
the estimate covers one layout. Builds have a separate allocation.

The producer and writer each have an 8 GiB address-space limit and a hard elapsed
deadline. DuckDB uses up to 2 GiB of managed memory. Native file writes share a
byte ceiling; exceeding it aborts the run. The writer prepares independent files
in parallel, capped by Rust's available CPU count and an estimate of per-file
memory. The estimate reserves space for original rows, one expected wide file,
encoder/readback buffers and metadata within a 7 GiB worker allowance. The
8 GiB process limit still applies. Full-value readback compares batches without
concatenating a second wide file in memory. The writer assembles the transaction
log in source-file order. It
records time spent expanding columns, writing and checking each file; these are
preparation measurements, summed across workers, not reader timings.
A failed run can leave files for
inspection but has no completed `manifest.json`. Do not treat those files as
a valid fixture or automatically rerun into the same directory.

A successful run writes a native Delta version-zero log, object hashes, source
identity, and per-file physical evidence. It preserves each source row once.
Scattering changes order within each row group only. To prepare that variant,
use `--layout scattered`; to prepare Q2, repeat the probe and generation with
`--shape q2`. The probe and generation must use the same writer binary and plan.
A probe for a different file target or page setting is rejected.

`matching_output_pages` counts stored projected pages that contain a matching
row. It describes the opportunity for selective reads, not observed reader
decoding or network traffic. The manifest keeps `native_campaign_ready` and
`publication_ready` false: exact source-derived references, real DV pairs and
the five native reader campaigns are separate steps.

## Validate and run the new workloads

[Comparison revision 5](selective-read-production-workloads.md) connects these
inputs to the existing oracle, five native adapters, scheduler and report.
Build the readers with the commands in the
[runner guide](selective-read-runners.md), using the current harness and the
same engine locks. Older executables reject revision 5 requests.

Pair the localized and scattered layouts of one shape at a time. For Q4:

```sh
../selective-read-oracle-venv/bin/python -B \
  benches/selective_read/production_pairs.py \
  --fixtures ../production-q4-localized --fixtures ../production-q4-scattered \
  --binary target/selective-read/release/selective-read-fixtures \
  --output ../production-q4-pairs --disk-limit-gib 192
```

Each supplied shape must include both layouts, from the same source and plan
settings. The helper preserves Parquet bytes and computes one shared logical
deletion union across the supplied base inventory, recorded in the manifest.
Both layouts therefore delete the same logical rows. Q2 can be paired later;
the full eight-case inventory remains required for formal sampling.

Immutable source, Parquet and geometry objects use hard links on the same
filesystem. The disk ceiling still counts their full logical sizes; the free
space check reserves only new writes. Cross-filesystem copies require space
for all bytes. Treat every linked fixture as immutable.

A probe directory already contains both layouts and can be supplied alone.
The helper creates native Delta logs and real DV snapshots for it, but retains
its `probe` identity. Reduced probes cannot stand in for full SF10 tables.

Freeze a pilot for a completed full Q4 table:

```sh
../selective-read-oracle-venv/bin/python -B \
  benches/selective_read/production_workloads.py \
  --fixtures ../production-q4-localized \
  --binary ../build-polars/selective-read-polars \
  --binary ../build-daft/selective-read-daft \
  --output ../production-q4-workload

../selective-read-oracle-venv/bin/python -B benches/selective_read/oracle.py \
  prepare --fixtures ../production-q4-localized \
  --case production.q4.localized \
  --workload ../production-q4-workload/workload.json \
  --output ../production-q4-reference
```

The workload always lists all eight core cases and all five readers. Inputs not
supplied to this freeze remain `not_prepared`. Use the paired probe directory
instead to prepare references for all eight bounded cases. Native validation
uses the same `--workload` argument as the campaign.

Upload the completed fixture with the existing
[storage commands](selective-read-storage.md), then pass `--workload`, the
reference, and all five binaries to the
[campaign runner](selective-read-campaign.md). The pilot schedules two timed
invocations per reader and execution mode. Reuse contains initialization and
two queries. `--stage formal` requires all eight full cases and selects five
samples. The existing `large_workloads.py report` command audits both revisions.
