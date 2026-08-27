# Scan Planning

Scan planning turns the files in a Delta snapshot into a bounded set of tasks.
It happens after the reader knows which columns and rows the query needs, but
before it opens any Parquet data files.

If you only need to run a query, the defaults are a good place to start. This
page is for readers who want to understand or tune how the work is divided.

## Choose a partition target

The partition target is the number of independent groups the reader tries to
create. It is a target, not a promise: a scan with fewer files may produce fewer
groups, and an empty scan produces none.

You can set the target explicitly:

- The direct API uses `DeltaScanBuilder::with_target_partitions`.
- The DataFusion adapter uses `DeltaDataFusionScanOptions::target_partitions`.

An explicit target must be greater than zero. It wins over the automatic target
and its resource caps.

Without an explicit value, the reader starts with the process's available
parallelism. It then caps that number with the values it can determine cheaply:

- DataFusion's session partition target, for a DataFusion scan
- the Unix soft file-descriptor limit, allowing 16 descriptors per partition
- available memory, allowing 256 MiB per partition

The result is always at least one. If a host signal is unavailable, the reader
leaves out that cap instead of failing the query. It does not run network,
storage, or stress probes while planning a scan.

## Select the files

Delta Kernel walks the snapshot metadata, removes files that are no longer
active, and applies partition and data-statistics pruning. Each selected file
becomes a scan task with its path, size and row estimates when available,
partition values, schema transforms, and deletion-vector information.

Choosing the partition target before this step keeps host policy separate from
table shape. Parquet file size is not a reliable estimate of decoded Arrow
memory, and choosing a larger target cannot by itself split one physical file.

## Group whole files

The reader first plans with whole files:

- When every selected task has a known size and their total is greater than
  zero, it assigns the largest tasks first and repeatedly places the next task
  in the lightest group.
- When any size is missing or the total is zero, it divides the files as evenly
  as possible by count while keeping their order.

The reader creates no more groups than the target or the number of files. It
does not add empty groups.

This whole-file plan is the final plan for the direct API. DataFusion has one
optional step that can divide files more finely.

## Split files with DataFusion

Intra-file repartitioning is available only for direct Parquet scans through the
DataFusion adapter. `DeltaFileRepartitioning` controls when the reader offers
its whole-file groups to DataFusion:

- `FillMissingParallelism`, the default, does so only when whole-file planning
  produced fewer groups than the target.
- `Rebalance` also allows DataFusion to reconsider a plan that already reached
  the target. This can help when a few large files make the groups uneven, but
  it may introduce more ranged reads.

DataFusion's `repartition_file_scans` setting must also be enabled. Its
`repartition_file_min_size` value is the minimum total input size needed before
repartitioning is attempted; it is not the size of each resulting range.

When repartitioning runs, DataFusion flattens the current groups and aims for
`ceil(total input bytes / target partitions)` bytes per new group. If file sizes
are unavailable or DataFusion finds no useful split, the reader keeps the
whole-file plan.

## Keep row groups intact

A ranged task never reads half of a Parquet row group. The range containing a
row group's first column-page offset owns that whole row group. The reader then
intersects range ownership with normal Parquet statistics pruning.

Every ranged task keeps the original Delta file metadata, including partition
values, transforms, and deletion-vector coordinates. A scan therefore returns
the same rows whether it uses whole-file tasks or ranged tasks.

See [read scheduling](read-scheduling.md) for what happens when these tasks
start running.
