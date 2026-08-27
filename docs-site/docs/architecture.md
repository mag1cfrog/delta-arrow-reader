# Architecture

Whether you use the streaming API or DataFusion, Delta Arrow Reader follows the
same path from a Delta Lake table to Apache Arrow record batches.

## From a table to Arrow batches

At a high level, the reader does three things:

1. It loads the table. The reader resolves the URI and object store, then loads
   one Delta snapshot with its protocol and schema.
2. It plans the scan. The reader applies the requested columns and predicate,
   removes files that cannot match, and divides the remaining files into
   partitions.
3. It runs the scan. The reader opens the Parquet files, applies deletion
   vectors and Delta schema changes, and returns batches as the application
   asks for them.

Once loaded, a `DeltaTable` always points to the same snapshot. To read a newer
snapshot, load the table again.

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

## Reader backends

The default `Direct` backend reads Parquet data asynchronously through
the same object store used to load the snapshot. The `DeltaKernel` backend
reads data files through Delta Kernel's synchronous iterator API.
Whichever backend you choose, the rest of the scan behaves the same.

## Cancellation

The reader schedules file work gradually instead of starting every file at
once. When the application drops a stream, the reader stops scheduling new
files. Synchronous work that has already started may continue until it reaches
a safe stopping point.

## Out of scope

Delta Arrow Reader does not write Delta tables, manage transactions, create a
Tokio runtime, or coordinate application workflows. The
[provenance page](provenance.md) records how this code was originally extracted
from Delta Funnel.

## Go deeper

- [Scan planning](scan-planning.md) explains how the reader chooses a partition
  target, groups files, and optionally splits large files.
- [Read scheduling](read-scheduling.md) explains concurrency, prefetching,
  dynamic pruning, backpressure, and cancellation.
