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
Follow the version and hash requirements in each guide.

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
Keep the frozen documents unchanged when checking recorded hashes.

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
IDs, document bytes, and source paths unchanged when replaying recorded runs:
the harness binds them by hash. Use table width, match layout, and
deletion-vector state as names in current explanations.

## Additional experiments

- [Partial-page reads and network validation](../../docs/content/benchmarks/intra-page-reads.md)
- [Predicates and projection width](../../docs/content/benchmarks/selective-read-matrix.md)
- [File organization](../../docs/content/benchmarks/selective-read-files.md)
- [Pruning inside files](../../docs/content/benchmarks/selective-read-within-file.md)
- [Deletion vectors](../../docs/content/benchmarks/selective-read-deletion-vectors.md)
- [Large-workload planning](../../docs/content/benchmarks/selective-read-large-workloads.md)
- [Recorded capacity check](../../docs/content/benchmarks/selective-read-pilot-capacity.md)

## Maintain the report charts

The README charts use the four deletion-vector cases from
`selective-read-current-timings.csv`. Bar lengths show median query time on
linear axes starting at zero: 0-80 seconds for 416 columns and 0-200 for 90 columns.
Regenerate them with `python -B benches/render_selective_read_chart.py`, or
add `--check` to verify the checked-in SVGs. Update the README's pinned image URLs
when publishing changed chart assets.
