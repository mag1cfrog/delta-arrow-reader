# Selective-read benchmark

The [performance report](https://mag1cfrog.github.io/delta-arrow-reader/benchmarks/selective-read-results/)
compares five readers on 416-column and 90-column Delta tables generated from
TPC-H lineitem. It includes timings, I/O measurements, test conditions, and
downloadable results. These are custom scan queries, not TPC-H query numbers
or a TPC-H score.

## Reproduce a run

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

The complete raw campaign archive and a single clean-checkout reproduction
package are not yet published. The linked guides describe the individual stages;
the report's CSV and provenance downloads preserve the published measurements.

## Workload definitions

- [Table layouts and queries](../../docs/content/benchmarks/selective-read-production-shapes.md)
- [Execution contract](../../docs/content/benchmarks/selective-read-production-workloads.md)
- [Spark comparison contract](../../docs/content/benchmarks/selective-read-spark-matrix.md)
- [Base protocol](../../docs/content/benchmarks/selective-read-protocol.md)
- [Sampling](../../docs/content/benchmarks/selective-read-sampling.md)
- [Combined diagnostics](../../docs/content/benchmarks/selective-read-combined-diagnostics.md)
- [Validation as storage warmup](../../docs/content/benchmarks/selective-read-gate-warmup.md)
- [Diagnostics during validation](../../docs/content/benchmarks/selective-read-gate-diagnostics.md)

Raw case IDs use `q2` for the 416-column table and `q4` for the 90-column
table. Keep those IDs, document bytes, and source paths unchanged when replaying
recorded runs: the harness binds them by hash. Use table width, match layout,
and deletion-vector state as names in user-facing reports.

## Additional experiments

- [Predicates and projection width](../../docs/content/benchmarks/selective-read-matrix.md)
- [File organization](../../docs/content/benchmarks/selective-read-files.md)
- [Pruning inside files](../../docs/content/benchmarks/selective-read-within-file.md)
- [Deletion vectors](../../docs/content/benchmarks/selective-read-deletion-vectors.md)
- [Large-workload planning](../../docs/content/benchmarks/selective-read-large-workloads.md)
- [Recorded capacity check](../../docs/content/benchmarks/selective-read-pilot-capacity.md)

## Maintain the report charts

The README charts use the four deletion-vector cases from the published summary
CSV. Regenerate them with `python -B benches/render_selective_read_chart.py`, or
add `--check` to verify the checked-in SVGs. Update the README's pinned image URLs
when publishing changed chart assets.
