# Architecture

Whether you use the streaming API or DataFusion, Delta Arrow Reader follows the
same path from a Delta Lake table to Apache Arrow record batches.

## From a table to Arrow batches

At a high level, the reader does three things:

1. It loads the table. The reader normalizes the table location, resolves the
   object store, then loads one Delta snapshot with its protocol and schema.
2. It plans the scan. The reader applies the requested columns and predicate,
   removes files that cannot match, and divides the remaining files into
   partitions.
3. It runs the scan. The reader opens the Parquet files, applies deletion
   vectors and Delta schema changes, and returns batches as the application
   asks for them.

Once loaded, a `DeltaTable` always points to the same snapshot. To read a newer
snapshot, load the table again.

## Remote I/O phases

For a table in remote storage, the read path has three distinct I/O phases.
The loading mode controls whether the first two phases happen during table
initialization or during each query:

| Phase | Lazy | Eager Delta metadata | Prepared Parquet metadata |
| --- | --- | --- | --- |
| 1. Delta scan metadata | Every scan build performs Delta log/checkpoint replay and selects active files. | Table initialization performs the replay once. Later scan builds select files from the retained metadata. | Same as eager Delta metadata. |
| 2. Parquet metadata | The query reads footers and prunes row groups. | The query reads footers and prunes row groups. | Table initialization reads and parses footers and offset indexes for all active files. Each query still performs its own row-group pruning. |
| 3. Parquet data | The query reads the selected row groups. | Unchanged. | Unchanged. |

A successful eager Delta metadata load needs no later Delta log/checkpoint reads
for file discovery from that table. Experimental Parquet metadata preparation
also removes later footer and offset-index reads for the prepared files. Neither
mode moves Parquet data-page reads into initialization or changes
deletion-vector behavior.

The retained Delta scan metadata contains reconciled active `add` metadata and
available file statistics, not raw JSON commit files. It belongs to one exact
snapshot and remains in memory for the lifetime of that loaded table.
Separately loaded tables do not share this memory. There is no TTL, eviction,
persistence, incremental update, or background refresh; load another table to
observe a newer snapshot.

Prepared Parquet metadata has the same snapshot lifetime and belongs to the
loaded table. The [preparation guide](https://mag1cfrog.github.io/delta-arrow-reader/prepared-parquet-metadata/)
explains its resource limits and direct-backend requirement.

## Streaming API

With the streaming API, these stages appear as a table, a single-use scan, and a
batch stream. The reader applies the predicate, removes any columns that were
needed only for filtering, and stops at the requested row limit. Metrics remain
available after the stream finishes or is dropped.

## DataFusion adapter

The DataFusion adapter changes the front end, not the reader underneath it.
DataFusion plans the SQL query and the work that happens after the scan. Delta
Arrow Reader still handles the Delta metadata, chooses the data files, applies
deletion vectors and schema changes, and reads the Parquet data.

The adapter registers one table at a time. Applications such as Delta Funnel
remain responsible for coordinating several tables, exposing configuration,
building reports, and sending the results elsewhere.

## Parquet backends

The default `Direct` backend reads Parquet data asynchronously through
the same object store used to load the snapshot. The `DeltaKernel` backend
reads data files through Delta Kernel's synchronous iterator API.
Whichever backend you choose, the rest of the scan behaves the same.

### Predicate-aware Parquet reads

When the `Direct` backend can evaluate a row predicate exactly, it applies that
predicate as a Parquet row filter before decoding the output columns. The row
filter reads only the columns used by the predicate, so a wide projection does
not make it decode unrelated data.

For those filtered reads, the backend also requests the Parquet offset index,
the part of page-index metadata that maps row ranges to the byte locations of
their data pages. When a file contains a usable offset index, the row selection
produced by the filter lets the reader fetch only the relevant pages from the
output columns. Files without a usable offset index still work; the reader
falls back to reading complete column chunks.

Both optimizations are automatic for the `Direct` backend and behave the same
through the streaming API and the DataFusion adapter.

## Cancellation

The reader schedules file work gradually instead of starting every file at
once. When the application drops a stream, the reader stops scheduling new
files. Synchronous work that has already started may continue until it reaches
a safe stopping point.

## Out of scope

Delta Arrow Reader does not write Delta tables, manage transactions, create a
Tokio runtime, or coordinate application workflows. The
[provenance page](https://mag1cfrog.github.io/delta-arrow-reader/provenance/) records how this code was originally extracted
from Delta Funnel.

## Go deeper

- [Delta metadata lifecycle](https://mag1cfrog.github.io/delta-arrow-reader/delta-metadata-lifecycle/) follows one loaded table through
  several queries and explains when keeping its file metadata in memory can
  save planning time.
- [Scan planning](https://mag1cfrog.github.io/delta-arrow-reader/scan-planning/) explains how the reader chooses a partition
  target, groups files, and optionally splits large files.
- [Read scheduling](https://mag1cfrog.github.io/delta-arrow-reader/read-scheduling/) explains concurrency, prefetching,
  dynamic pruning, backpressure, and cancellation.
