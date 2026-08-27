# Read Scheduling

After planning, each scan partition contains an ordered list of whole-file or
ranged Parquet tasks. The scheduler starts those tasks gradually and passes
their Arrow batches to the caller through bounded channels.

The defaults favor bounded memory and useful overlap. Tune them only after
measuring a representative workload.

## Bound active reads

Two limits apply before a file task can start:

- The scan-wide limit covers every execution partition in one scan. By default,
  it is the partition target multiplied by the per-partition limit.
- The per-partition limit prevents one partition from taking all scan-wide
  capacity. Its default is three active file reads.

A task holds both permits until its file stream finishes or is dropped. A task
that cannot get both permits waits without opening the data file.

## Prefetch the next files

The direct Parquet reader can prepare a small number of future file streams
while the current one is being consumed. The default prefetch depth is two per
partition. A value of zero waits until the current file is drained before
starting the next one.

Prefetching preserves task order within each partition. Prepared streams still
hold read permits, so they count against both concurrency limits.

The Delta Kernel reader does not prefetch future files. Within each partition,
its synchronous iterator reads one admitted file at a time through a bounded
blocking handoff. Several partitions can still run at once.

## Apply dynamic partition pruning

The DataFusion adapter can receive partition-only filters after the physical
plan has been built. Before each not-yet-started task is admitted, the reader
takes the latest filter value and checks it against the task's Delta partition
values.

If the filter proves that a task cannot match, the scheduler skips it before it
acquires permits, opens the object store, loads a deletion vector, or performs
schema transforms. Tasks that have already started are not cancelled.

Dynamic pruning is deliberately conservative. Missing or invalid partition
values, incomplete filters, unsupported expressions, and evaluation failures
all fall back to reading the task. This can cost extra I/O, but it cannot remove
rows that might match.

## Choose between ranged and full-file reads

The direct Parquet reader normally asks the object store for the footer and data
ranges needed by the query. This is useful for narrow projections, especially
when the file is large.

For small files that are read broadly, several remote range requests can cost
more than one full request. Setting
`parquet_full_file_read_threshold_bytes` allows the reader to fetch an eligible
whole-file task once and serve its later range reads from an in-memory copy.
Tasks created by intra-file repartitioning keep using remote range requests even
when the physical file is below the threshold.

The buffer belongs to that one file stream and is released when the stream
finishes or is dropped. Its compressed-byte memory cost grows with the chosen
threshold and the number of active file reads. Files above the threshold keep
using normal object-store range requests.

This option does not change projection, row-group pruning, deletion-vector
masking, or Arrow decoding. Keep it disabled unless measurements show that
small-file request overhead matters for your workload.

## Apply backpressure

Each execution partition sends batches through a bounded output channel. Its
default capacity is one batch. When the caller stops polling, producers wait
instead of filling an unbounded queue.

Within a partition, batches stay in task order even when the `Direct` backend has
prefetched later files. DataFusion may still execute several partitions at the
same time.

## Cancel unfinished work

Dropping the output stream tells the scheduler to stop admitting new tasks and
releases queued streams and permits. Errors follow the same bounded path and
prevent unrelated later work from being drained.

Asynchronous reads stop at cancellation boundaries. Synchronous Delta Kernel
work that has already entered a dependency call may continue until its next
safe handoff, but it cannot start a later file after cancellation.

See [execution options](reference/execution-options.md) for exact defaults and
[scan metrics](reference/metrics.md) for the counters exposed by this process.
