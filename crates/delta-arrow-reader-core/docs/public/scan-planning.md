# Scan planning

Before reading rows, the reader decides which files a query needs and how to
share the work. It creates groups of file tasks that can run in parallel.
These groups are called *execution partitions*. They do not change the table's
files or its partition columns.

The defaults choose the amount of parallel work for you. This page explains
those choices; [Configuration](https://mag1cfrog.github.io/delta-arrow-reader/configuration/)
shows the settings to use when you need to adjust them.

## Choose a partition target

The target is the number of independent groups the reader tries to create.
It starts from available CPU parallelism, then applies memory and open-file
limits it can determine cheaply. DataFusion's session target can also cap it.
A smaller scan may need fewer groups, and an empty scan has none.

Python's `to_reader()` and `scan()` accept a positive `target_partitions`.
Python SQL uses the automatic choice. In Rust, use
`DeltaScanBuilder::with_target_partitions` for direct scans or
`datafusion::ScanOptions::target_partitions` for SQL. An explicit target overrides
the automatic resource caps and skips the host probes. It does not reserve memory
or guarantee that a query fits in memory.

<details markdown="1">
<summary>Automatic limits and Linux memory accounting</summary>

Without an explicit value, the reader starts with the process's available
parallelism. It then caps that number with the values it can determine cheaply:

- DataFusion's session partition target, for a DataFusion scan
- the Unix soft file-descriptor limit, allowing 16 descriptors per partition
- available memory, allowing 256 MiB per partition

The result is always at least one. If a host signal is unavailable, the reader
leaves out that cap instead of failing the query. Choosing the partition target
does not run network or storage performance probes.

On Linux, the memory hint is the smaller of host availability from
`/proc/meminfo` and the process's remaining cgroup memory allowance. The reader
supports cgroup v1 and v2, discovers the process's group and its mounted path,
and checks accessible ancestor limits. Each group uses its own usage and cache
counters, so ancestor estimates include sibling groups. A v1 ancestor
contributes only when hierarchical accounting is enabled.

The estimate credits inactive file cache after deducting dirty and writeback
pages. Every subtraction floors at zero:

```text
cache credit = inactive file - dirty - writeback
headroom = hard limit - (current usage - cache credit)
```

The required `memory.stat` counters depend on the accounting mode:

| Accounting mode | Inactive file | Dirty | Writeback |
| --- | --- | --- | --- |
| v2 | `inactive_file` | `file_dirty` | `file_writeback` |
| v1, `memory.use_hierarchy = 1` | `total_inactive_file` | `total_dirty` | `total_writeback` |
| v1, `memory.use_hierarchy = 0` | `inactive_file` | `dirty` | `writeback` |

All dirty and writeback pages are deducted, even if they overlap or belong to
other reclaim lists. Shmem and tmpfs use anonymous lists, so they receive no
credit. Active file cache and swap allowance are also excluded. V1's inactive
file counter can include lazy-free anonymous pages. See the kernel's
[v2 memory counters](https://docs.kernel.org/admin-guide/cgroup-v2.html#memory)
and [v1 statistics](https://docs.kernel.org/admin-guide/cgroup-v1/memory.html#stat-file).

Missing, unreadable, malformed, or duplicate counters give no cache credit. The
same applies if a counter exceeds current usage or the v1 hierarchy mode is
unknown. The group keeps its raw headroom estimate. Unlimited limits, unusable
limit or usage files, and unsupported layouts add no cgroup hint. Other readable
hints still apply, with host availability as the fallback. Ancestors hidden by
the cgroup namespace or mount are not observable. The local environment
diagnostic reports the effective available-memory hint; its total memory field
still describes host physical memory.

These counters are sampled separately during planning and can change between
reads. Inactive pages may not be immediately reclaimable; reclaim can add
latency and force later reads from storage. The partition cap is a planning
heuristic: it does not reserve memory, enforce a process memory limit, or
guarantee that a scan avoids OOM.

</details>

## Select the files

The reader uses the Delta table's file information and the query's conditions
to remove files that cannot match. With a prepared table it reuses that
information; otherwise it builds it for this query. Delta Kernel applies the
partition values and file statistics to make the selection.

Each remaining file becomes a task carrying its location, size and row estimates
when available, partition values, schema changes, and deletion-vector information.
A deletion vector marks rows that must stay excluded from the result.

The reader chooses its partition target before selecting files. Compressed
Parquet file size does not reliably predict memory use after decoding, and a
larger target alone cannot split one physical file.

## Group whole files

The reader first plans with whole files:

- When every selected task has a known size and their total is greater than
  zero, it assigns the largest tasks first and repeatedly places the next task
  in the lightest group.
- When any size is missing or the total is zero, it divides the files as evenly
  as possible by count while keeping their order.

The reader creates no more groups than the target or the number of files. It
does not add empty groups.

This whole-file plan is the final plan for the streaming API. DataFusion has one
optional step that can divide files more finely.

## Split files with DataFusion

If there are too few files to fill the target, DataFusion can split large files
into ranged tasks. This is available for direct Parquet reads through the
DataFusion adapter. Streaming API scans keep whole-file tasks.

<details markdown="1">
<summary>Repartitioning settings and split sizes</summary>

Intra-file repartitioning is available only for direct Parquet scans through the
DataFusion adapter. `datafusion::IntraFileRepartitioning` controls when the
reader offers its whole-file groups to DataFusion:

- `WhenBelowTarget`, the default, does so only when whole-file planning
  produced fewer groups than the target.
- `Always` also allows DataFusion to reconsider a plan that already reached
  the target. This can help when a few large files make the groups uneven, but
  it may introduce more ranged reads.

DataFusion's `repartition_file_scans` setting must also be enabled. Its
`repartition_file_min_size` value is the minimum total input size needed before
repartitioning is attempted; it is not the size of each resulting range.

When repartitioning runs, DataFusion flattens the current groups and aims for
`ceil(total input bytes / target partitions)` bytes per new group. If file sizes
are unavailable or DataFusion finds no useful split, the reader keeps the
whole-file plan.

</details>

## Keep row groups intact

Parquet stores a file in groups of rows called row groups. A split assigns each
group to one task so the same rows are not read twice or left out.

<details markdown="1">
<summary>How a range owns a row group</summary>

A ranged task never reads half of a Parquet row group. The range containing a
row group's first column-page offset owns that whole row group. The reader then
intersects range ownership with normal Parquet statistics pruning.

Every ranged task keeps the original Delta file metadata, including partition
values, transforms, and deletion-vector coordinates. A scan therefore returns
the same rows whether it uses whole-file tasks or ranged tasks.

</details>

## Choose a warmup mode

Preparing a table changes when file information is loaded, rather than which
rows a query returns. The default prepares supported S3 tables while opening;
other stores remain lazy. Start with the
[metadata lifecycle](https://mag1cfrog.github.io/delta-arrow-reader/delta-metadata-lifecycle/)
for an explanation of reuse across queries.

<details markdown="1">
<summary>Preparation APIs and incomplete network checks</summary>

For a practical comparison and Python or Rust examples, start with
[configuration](https://mag1cfrog.github.io/delta-arrow-reader/configuration/).

`DeltaTableBuilder::load_table` defaults to `WarmupMode::Automatic`. For supported
S3 stores, the Direct backend prepares reusable metadata and samples the
network before the first query. Other stores, explicit range policies and the
Delta Kernel backend do no warmup. Disabling partial-page reads on the table
builder also disables automatic warmup.

Choose `WarmupMode::None` to skip preparation. Each scan then performs Delta
log/checkpoint replay before Delta Kernel applies its predicate and selects files.

`DeltaTableBuilder::with_warmup(WarmupMode::QueryPlanning)` instead materializes
a query-unfiltered set of reconciled active `add` metadata during table
initialization. It retains all available file statistics, so later scan builds
can still make query-specific data-skipping decisions. Those builds pass the
retained metadata to Delta Kernel through `Scan::scan_metadata_from`; Delta
Kernel remains responsible for applying the query predicate and selecting
files.

These modes produce the same file tasks. Partition values, schema transforms,
and deletion-vector information follow the selected files in every mode.

Query-planning warmup adds initialization time and retains metadata in memory.
Use the [metadata comparison instructions](https://github.com/mag1cfrog/delta-arrow-reader/blob/main/docs/content/benchmarks/eager-metadata.md)
to measure whether it reduces total time for your queries.

To see how this initialization choice plays out across several queries, follow
the [metadata warmup lifecycles](https://mag1cfrog.github.io/delta-arrow-reader/delta-metadata-lifecycle/).

The [partial-page guide](https://mag1cfrog.github.io/delta-arrow-reader/partial-page-reads/#measure-the-connection)
explains how network sampling selects data files, shares its profile and recovers
once after an incomplete attempt. It also covers supported stores, sampling
limits, explicit configuration and the initialization cost.

`load_snapshot` never performs warmup. Converting that snapshot with `into_table`
also leaves warmup undone; use `load_table` for automatic preparation.

</details>

## Floating-point row-group pruning

Some floating-point statistics cannot safely exclude a row group because it
might contain NaN values. The reader then reads and filters the group instead.
This costs extra I/O but preserves the query's comparisons.

<details markdown="1">
<summary>Exact NaN and min/max rules</summary>

For physical Parquet `FLOAT` and `DOUBLE` columns, the `Direct` backend requires
a usable `nan_count = 0` before using min/max bounds to prune `<`, `<=`, `>`,
`>=`, or `!=`. Missing, invalid, or nonzero counts prevent these bounds from
excluding a row group for those operators, which can mean extra reads. An
explicit zero restores these pruning opportunities when the bounds suffice.

Bounds can omit NaNs. The
[Parquet specification](https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift)
requires readers to assume NaNs may exist when `nan_count` is absent. Direct's
row evaluator uses Arrow comparisons: negative NaNs sort below non-NaN values,
positive NaNs above them. For example, bounds of `[1.5, 1.5]` do not rule out
`v > 100.0` if the group might contain a positive NaN.

Equality to a finite value can still exclude impossible matches, such as
`v = 100.0` with those bounds. Null checks with valid counts and an independently
impossible `AND` condition on another column can also exclude row groups. The
NaN guard checks physical statistics; widening integers to floats does not
require NaN counts.

</details>

For the next stage, see [read scheduling](https://mag1cfrog.github.io/delta-arrow-reader/read-scheduling/).
