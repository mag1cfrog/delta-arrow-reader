# Selective-read benchmark

The [performance report](https://mag1cfrog.github.io/delta-arrow-reader/benchmarks/selective-read-results/)
compares five readers on 416-column and 90-column Delta tables generated from
TPC-H lineitem. It includes timings, I/O measurements, test conditions, and
downloadable results. These are custom scan queries, not TPC-H query numbers
or a TPC-H score.

## Reproduce a run

The [evidence release](https://github.com/mag1cfrog/delta-arrow-reader/releases/tag/selective-read-benchmarks-2026-10-07)
contains the complete recorded campaigns, reference outputs, frozen sources,
and checksums. [Verify the archive or run a new campaign](EVIDENCE.md).
The archive audit regenerates the published report and CSVs without the full tables.

Run commands from the repository root and keep generated tables outside Git.
The recorded runs use pinned inputs, reader builds, and execution contracts.
Follow the version, input, and execution requirements in each guide.

The current tools check compatibility through format and comparison revisions,
queries, projections, layout settings, and independently verified Arrow values.
Source, controller, and protocol-document hashes record provenance; editing a
comment or rebuilding the controller does not invalidate an otherwise compatible
plan, workload, or reference. Historical guides describe the checks used for
their recorded runs.

Checksums still verify data files, exports, and packaged reader artifacts.
A timing certificate applies to the reader build, configuration, query, and
inputs that passed validation. A different reader build must pass validation
before its timings can be accepted.

New requests and certificates carry a structured workload contract and reader
artifact identity. They compare queries, layout settings, inputs, runtime and
executable artifacts directly. Updating Git metadata or source provenance alone
does not require new references or correctness checks. Historical records that
only contain checksums still require their original immutable inputs. Revalidate
exports with the current tools to produce certificates with structured identities.

Build the native controller before preparing references, running readers or
checking the independent Python oracle:

```sh
cargo build --locked -p xtask
export SELECTIVE_READ_XTASK="$(pwd)/target/debug/xtask"
```

With `CARGO_TARGET_DIR`, point `SELECTIVE_READ_XTASK` at that directory's
`debug/xtask` instead. The controller handles schedules, timing summaries,
result validation, watchdog deadlines, and process accounting. It checks frozen
inputs and recomputes summaries from the journal before reporting results. It is
never compiled during a reader invocation.

1. [Generate the public source](../../docs/content/benchmarks/selective-read-fixtures.md),
   then [write the wide tables and deletion-vector pairs](../../docs/content/benchmarks/selective-read-production-fixtures.md).
2. [Build the readers](../../docs/content/benchmarks/selective-read-runners.md),
   including [Spark and Delta Lake](../../docs/content/benchmarks/selective-read-spark.md).
3. [Prepare independent reference results](../../docs/content/benchmarks/selective-read-oracle.md)
   and [configure storage and request observation](../../docs/content/benchmarks/selective-read-storage.md).
4. [Run the comparison](../../docs/content/benchmarks/selective-read-campaign.md),
   then [generate and audit the report](../../docs/content/benchmarks/selective-read-production-report.md).

The linked guides describe each preparation stage. The reproduction guide pins
the reader and harness revisions and records the extent of delivery checks.

## Workload definitions

These contracts preserve earlier revisions as well as the final comparison.
Older Daft references, page settings, and pending-work notes are historical;
use the [reproduction guide](EVIDENCE.md) and public report for revision 6.
The frozen documents remain part of the historical evidence archive.

The current checkout provides DAR, delta-rs, DuckDB, Polars, and Spark adapters.
The Daft adapter, dependency lock, and checks are available in the
[historical reader checkout](https://github.com/mag1cfrog/delta-arrow-reader/tree/eb1f58b4be8dc0d8ec3bee1615f1777a4c41bd9f/benches/selective_read/runners/daft).
Use the matching frozen harness for older comparisons. Their reader identities,
expression snapshots, and published results remain available for validation.
New workload definitions use revision 6. The old Daft-dependent creation
commands live in the archived harness. Export audited batches with
`cargo +1.98.1 run --locked -p xtask -- selective-read report-workload`.

- [Table layouts and queries](../../docs/content/benchmarks/selective-read-production-shapes.md)
- [Execution contract](../../docs/content/benchmarks/selective-read-production-workloads.md)
- [Spark comparison contract](../../docs/content/benchmarks/selective-read-spark-matrix.md)
- [Base protocol](../../docs/content/benchmarks/selective-read-protocol.md)
- [Sampling](../../docs/content/benchmarks/selective-read-sampling.md)
- [Combined diagnostics](../../docs/content/benchmarks/selective-read-combined-diagnostics.md)
- [Validation as storage warmup](../../docs/content/benchmarks/selective-read-gate-warmup.md)
- [Diagnostics during validation](../../docs/content/benchmarks/selective-read-gate-diagnostics.md)

Frozen case IDs use `production.q2.*` for the 416-column table and
`production.q4.*` for the 90-column table. The `localized` layout is called
"grouped" in the public reports; `scattered` keeps the same name. Keep these
case IDs and workload contents unchanged when replaying recorded runs.
Use table width, match layout, and
deletion-vector state as names in current explanations.

## Additional experiments

- [Partial-page reads and network validation](../../docs/content/benchmarks/intra-page-reads.md)
- [String and binary partial reads in the production reader](delta-length-reader.md)
- [Predicates and projection width](../../docs/content/benchmarks/selective-read-matrix.md)
- [File organization](../../docs/content/benchmarks/selective-read-files.md)
- [Pruning inside files](../../docs/content/benchmarks/selective-read-within-file.md)
- [Deletion vectors](../../docs/content/benchmarks/selective-read-deletion-vectors.md)
- [Large-workload planning](../../docs/content/benchmarks/selective-read-large-workloads.md)
- [Recorded capacity check](../../docs/content/benchmarks/selective-read-pilot-capacity.md)

For the isolated raw Zstd block discovery diagnostic, prepare and upload a `long`
Zstd fixture, attach the network proxy, then run:

```sh
cargo xtask selective-read zstd-probe \
  --fixture /path/to/long-zstd-fixture \
  --state /path/to/minio-state \
  --output /path/to/new-probe-results
```

The Rust probe compares complete column reads with sequential block-header
discovery in three alternating pairs. Both modes use curl and at most 32 worker
threads. Each request range, checksum, discovered block and proxy count is checked.
The output records the controller hash because timings include client overhead.
This diagnostic measures discovery alone; it does not execute queries.

## Maintain the report charts

The README charts use the four deletion-vector cases from
`selective-read-current-timings.csv`. All five readers are shown, with bar lengths
proportional to median query time. The two columns use separate linear scales
starting at zero: 0-80 seconds for 416 columns and 0-200 for 90 columns.
Regenerate them with `cargo xtask render-selective-read-chart`, or
add `--check` to verify the checked-in SVGs. Update the README's pinned image URLs
when publishing changed chart assets.
