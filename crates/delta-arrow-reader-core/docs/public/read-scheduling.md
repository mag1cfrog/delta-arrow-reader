# Read scheduling

While your application processes one batch of rows, the reader can prepare the
next files. It limits that work so reading does not run arbitrarily far ahead
of your application. When you close the stream, it stops starting new files.

[Scan planning](https://mag1cfrog.github.io/delta-arrow-reader/scan-planning/)
has already divided the query into groups of file tasks, called execution
partitions. The scheduler controls when those tasks run.

## Bound active reads

Two limits control how many file reads can run at once:

- A limit for the whole scan, shared by all its partitions. By default this is
  the partition target multiplied by the per-partition limit, up to the core's
  concurrency capacity.
- A limit for each partition, which defaults to three active file reads.

Both limits apply. A task waits until it can take a slot from each limit before
opening a data file, and holds both slots until its stream finishes or is dropped.

These limits count file tasks, rather than individual network requests or bytes.
The [configuration guide](https://mag1cfrog.github.io/delta-arrow-reader/configuration/)
shows how to reduce the number of concurrent file reads.

## Prefetch the next files

The direct Parquet reader prepares up to two upcoming file streams per partition
by default. This *prefetching* overlaps preparation with the current read.
Prepared streams count against both active-read limits; prefetching does not
add extra slots. Set the depth to zero to wait for one file to finish before
starting the next.

Batches stay in task order within a partition, even when later files have been
prepared. Separate partitions can run at the same time.

<details markdown="1">
<summary>How the Delta Kernel backend differs</summary>

The Delta Kernel reader does not prefetch future files. Within each partition,
its synchronous iterator reads one admitted file at a time through a bounded
blocking handoff. Several partitions can still run at once.

</details>

## Apply backpressure

Each partition normally queues at most one output batch for your application.
When that queue is full, the producer waits. This is *backpressure*: a slow
consumer slows the production of more batches.

Increasing the output buffer lets the reader work further ahead but uses more
memory. Batches can vary in size, and other read buffers also use memory, so a
batch count is not a limit on total memory use.

## Cancel unfinished work

Dropping a Rust stream or closing a Python reader stops new tasks and releases
queued streams and read slots. Errors also stop the scan without draining
unrelated later work.

Asynchronous reads stop at cancellation boundaries. Synchronous Delta Kernel
work already inside a dependency call may continue until its next safe handoff,
but it cannot start a later file after cancellation.

## Apply dynamic partition pruning

A SQL query can discover more filter information after planning. DataFusion
can pass it to the reader, which may skip files that have not started yet.

<details markdown="1">
<summary>Which late filters can skip a task</summary>

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

</details>

## Choose between ranged and full-file reads

Normally the reader requests only the parts of a file it needs. For small files
that a query reads broadly, downloading the whole file once can save requests,
at the cost of keeping it in memory. This option is disabled by default.

<details markdown="1">
<summary>Full-file buffering settings and memory ownership</summary>

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

</details>

## Look up settings and measurements

- [Execution options](https://mag1cfrog.github.io/delta-arrow-reader/reference/execution-options/): exact defaults and limits.
- [Scan metrics](https://mag1cfrog.github.io/delta-arrow-reader/reference/metrics/): how many files, batches, bytes, and requests a scan used.
