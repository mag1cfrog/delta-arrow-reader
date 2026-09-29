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
  --output ../production-plan
```

The planner checks the original source and computes file boundaries before any
reader timing. Keep its output directory: the writer checks its hashes against
the current workload definition, source and planner.

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
existing payload function and Parquet settings, with the contract's smaller
groups and pages. It reads every written file back and compares all values and
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
in parallel using Rust's available CPU count, which respects affinity and CPU
quotas, and assembles the transaction log in source-file order. It
records time spent expanding columns, writing and checking each file; these are
preparation measurements, summed across workers, not reader timings.
A failed run can leave files for
inspection but has no completed `manifest.json`. Do not treat those files as
a valid fixture or automatically rerun into the same directory.

A successful run writes a native Delta version-zero log, object hashes, source
identity, and per-file physical evidence. It preserves each source row once.
Scattering changes order within each row group only. To prepare that variant,
use `--layout scattered`; to prepare Q2, repeat the probe and generation with
`--shape q2`. The probe and generation must use the same writer binary.

`matching_output_pages` counts stored projected pages that contain a matching
row. It describes the opportunity for selective reads, not observed reader
decoding or network traffic. The manifest keeps `native_campaign_ready` and
`publication_ready` false: exact source-derived references, real DV pairs and
the five native reader campaigns are separate steps.
