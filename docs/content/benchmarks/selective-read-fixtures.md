---
title: Generate public selective-read fixtures
description: Reproduce the lineitem inputs, layouts, Delta snapshots, and public SQL for the selective-read comparison.
---

# Generate public selective-read fixtures

The fixture generator prepares the public inputs for
[selective-read-v1](selective-read-protocol.md). It saves original TPC-H
lineitem rows, writes clustered and shuffled Delta tables, and adds a wide
derivative with 64 deterministic nullable columns. Each layout contains the
same logical rows. These inputs let later measurements distinguish file
skipping from the cost of reading and returning columns.

This step creates ordinary snapshots without deletion vectors. Reader runners,
an independent query oracle, file-count controls, and measured comparisons have
separate roadmap issues under
[the benchmark plan](https://github.com/mag1cfrog/delta-arrow-reader/issues/312).
Preparation times are not reader benchmark results.

The comparison now includes DAR, delta-rs, DuckDB, Polars, and Daft. Its paired
wide 4,096-file workload, with and without DVs, is prepared in a later slice
from these same public rows. This generator supplies the base inputs. Comparison
revision 2 retains their physical generation rules and checksums; keep the
manifest's original protocol hash and record the active comparison revision
separately when validating and measuring them.

## Generate a profile

Run these commands from the repository root on `x86_64-unknown-linux-gnu`.
Install Rust `1.98.1` with rustup if needed. A C/C++ toolchain is needed to
build the pinned compression libraries. The first build downloads public Cargo
packages; data generation requires no private inputs or network service.

Each command writes to a new sibling directory, outside the Git checkout.
Change `--output` to a new directory on a filesystem with enough free space.
The generator rejects an existing output directory.

Smoke, with original and wide SF0.01 data:

```sh
RUSTFLAGS='-C target-cpu=x86-64' CARGO_TARGET_DIR=target/selective-read \
  cargo +1.98.1 run --release --locked -j8 \
  --manifest-path benches/selective_read/Cargo.toml -- \
  --profile smoke --output ../selective-read-smoke
```

Development, with original and wide SF1 data:

```sh
RUSTFLAGS='-C target-cpu=x86-64' CARGO_TARGET_DIR=target/selective-read \
  cargo +1.98.1 run --release --locked -j8 \
  --manifest-path benches/selective_read/Cargo.toml -- \
  --profile development --output ../selective-read-development
```

Report, with original SF10 and wide SF1 data:

```sh
RUSTFLAGS='-C target-cpu=x86-64' CARGO_TARGET_DIR=target/selective-read \
  cargo +1.98.1 run --release --locked -j8 \
  --manifest-path benches/selective_read/Cargo.toml -- \
  --profile report --output ../selective-read-report
```

The generator is a separate Cargo package in `benches/selective_read`, with
its own committed lockfile. Its dependencies do not change the reader library's
dependency graph or Rust requirement. Use `--locked` when reproducing inputs.

## Read the output

A successful preparation writes `manifest.json` last. A directory without a
valid manifest whose `status` is `complete` is incomplete; do not use it for
measurements. Errors go to stderr, and partial output remains for inspection.
After inspecting a failure, remove that output or choose a new directory to
retry. The generator does not resume partial preparations.

| Path | Contents |
| --- | --- |
| `sf0.01/source/`, `sf1/source/`, or `sf10/source/` | Original 16-column reference Parquet files in generator order, before sorting or adding payloads |
| `li.clustered/`, `li.shuffled/` | Original lineitem Delta tables at the profile's original scale |
| `wide.clustered/`, `wide.shuffled/` | Original fields plus `payload_00` through `payload_63` at the profile's wide scale |
| `<table>/_delta_log/00000000000000000000.json` | Snapshot 0, protocol 1/2, no partitions, exact statistics for every column |
| `generator-Cargo.lock` | Resolved generator, sort, writer, and compression dependencies |
| `manifest.json` | Input identity, physical layout, frozen SQL, checksums, and preparation resource usage |

The report profile saves both SF10 and SF1 reference inputs. Reference files
use the same pinned Parquet settings as the tables. Preserve them for the
independent full-read oracle.

The manifest records:

- Generator, source, executable, protocol, and lockfile identities; row keys,
  sorting and shuffle rules, and the nullable payload formula.
- Schemas, scales, snapshot versions, file paths, row counts, byte counts,
  writer settings, and SHA-256 hashes for data and Delta log files.
- Actual row-group boundaries and per-column page locations, row counts,
  statistics, encodings, compression, dictionary offsets, and index metadata.
- A literal list and expanded SQL under each source's `in_literals`, `queries`,
  and `wide_queries`. The generator derives the list from the saved source
  values before any reader timing. Smoke may have fewer than 20 literals.
- Memory and disk limits, process peak RSS, elapsed preparation time, and
  native sort spill counts and bytes. Spilled bytes are cumulative writes,
  not peak simultaneous temporary disk usage. `spill_at_completion` records
  the native disk manager's remaining temporary bytes and file count.

For each written Parquet file, preparation reads the footer and both page
indexes, checks their physical boundaries, and reads every column back to
recompute the Delta `numRecords`, `minValues`, `maxValues`, and `nullCount`.
It fails if these differ from the values accumulated while writing. Wide
tables include statistics for all 80 columns.

The manifest records actual page sizes because the writer's 1 MiB target and
20,000-row setting do not guarantee exact physical page boundaries.

## Stay within preparation limits

The protocol sets these ceilings for preparation. They are limits, not
estimates of required memory or final output size.

| Profile | Process address-space limit | Default sort pool | Data, spill, and later storage-copy ceiling |
| --- | --- | --- | --- |
| `smoke` | 4 GiB | 512 MiB | 8 GiB |
| `development` | 16 GiB | 4 GiB | 64 GiB |
| `report` | 16 GiB | 4 GiB | 192 GiB |

Compilation is separate: allow up to 16 GiB of build memory and 64 GiB of
build disk. Generated bulk data belongs outside Git.

Preparation streams source rows and generates one table at a time. DataFusion
sorts only the original columns and spills within its memory pool. Wide tables
add payloads after sorting. At the same scale, they reuse the ordered original
files so that row and file boundaries match.

The CLI sets Linux `RLIMIT_AS` before creating worker threads. This limits
virtual address space, which is stricter than a resident-memory limit. A
smaller inherited limit still applies. Native allocation failures can terminate
the process; a partial output never has a completion manifest.

Before writing data, the generator checks the destination filesystem. Its disk
budget is the smaller of the profile ceiling and free space minus 512 MiB.
It reserves the sort pool size plus 64 MiB for spill-write headroom, then gives
two thirds of the remainder to permanent output and one third to active spill
files. Output writes check their byte budget; DataFusion checks its spill
quota. These checks do not reserve filesystem space against other processes.
Later storage copies must fit in the remaining protocol budget too.

Append `--sort-memory-mib N` or `--disk-limit-mib N` to a generation command
to lower the preparation allowance. The sort pool must be at least 16 MiB
and at most half the profile's process limit. The disk allowance cannot exceed
the protocol ceiling. Smaller sort pools can spill more often; they must not
change row order, file boundaries, or object hashes. An exhausted allowance
fails preparation without reducing the data scale or payload width.

## Check reproducibility

Run the same bounded check that CI uses:

```sh
RUSTFLAGS='-C target-cpu=x86-64' CARGO_TARGET_DIR=target/selective-read \
  cargo +1.98.1 test --release --locked -j8 \
  --manifest-path benches/selective_read/Cargo.toml
```

The check generates smoke twice in temporary directories, once with a 512 MiB
sort pool and once with 16 MiB. It requires the smaller pool to spill, compares
all source and table manifests including object hashes, and verifies every
original value, row key, layout order, payload, and null against the reference
rows. It also rejects incorrect Delta statistics and an exhausted output budget.

To compare two manual preparations of the same profile, compare the `sources`
and `tables` fields in their manifests. All data and Delta-log SHA-256 values
must match. Preparation elapsed time, resource usage, and free space can differ.
Executable identity can also differ across build locations; it does not change
the fixture object checksums. A change to those object checksums requires a new
fixture revision under the protocol.
