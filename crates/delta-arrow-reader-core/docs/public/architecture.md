# How a read works

Delta Arrow Reader turns a Delta table into batches of rows your application can
process. It tries to avoid reading data the query does not need, then delivers
the remaining rows a batch at a time.

A Delta table has two parts: Parquet files containing the rows, and a Delta log
that records which files belong to each version of the table. You open the table
directory; the reader uses the log to find the right files.

## From a table to Arrow batches

1. **Open a version.** Load the table's column names and types, and identify the
   version to read. A loaded table stays on that version even if someone writes
   new data.
2. **Choose the data.** Use your requested columns and filter conditions to rule
   out files and parts of files that cannot contribute to the result.
3. **Return batches.** Read the remaining data, exclude deleted rows, apply the
   table's schema, and return groups of rows in Arrow format.

The reader works at several levels. Parquet divides a file into row groups,
then divides each column's data into smaller pages:

| Level | What the reader can avoid |
| --- | --- |
| File | Files whose recorded values cannot match the filter. |
| Row group | Groups of rows that statistics inside the file rule out. |
| Page | Pages in output columns that contain no selected rows. |
| Values within a page | Unneeded values, when the file format supports partial reads and the expected saving justifies the requests. |

If the available information cannot rule data out safely, the reader reads and
filters it. A deletion vector is a list of rows marked as deleted; those rows
stay excluded whichever read strategy is chosen.

## Streaming API

In Rust, a loaded table creates a scan, and the scan produces a stream of Arrow
batches. Python's `to_reader()` exposes those batches through PyArrow. Reading
incrementally lets your application process a large result without collecting
it all in memory.

A Rust scan is single-use. It applies the filter, removes columns needed only to
evaluate that filter, and stops at the requested row limit. You can still
inspect its metrics after the stream finishes or is dropped.

## DataFusion adapter

For SQL, DataFusion plans the query and operations such as joins, sorting, and
aggregation. Delta Arrow Reader handles the Delta table reads underneath it,
including file selection, deletion vectors, and schema changes.

The adapter registers one table at a time. Your application chooses which tables
to register and how to use the results. Applications such as Delta Funnel also
coordinate multiple tables, configuration, reporting, and delivery.

## Remote I/O phases

A remote query needs information about the table's files, information inside the
selected Parquet files, and finally the selected data. Keeping a table open can
save repeated work on the first part.

By default, supported S3 tables prepare reusable file information and measure
the connection while opening. Local tables build file information during queries
and skip the connection check. The
[configuration guide](https://mag1cfrog.github.io/delta-arrow-reader/configuration/)
explains the opening cost and alternatives. The
[metadata lifecycle](https://mag1cfrog.github.io/delta-arrow-reader/delta-metadata-lifecycle/)
explains what is reused and what each query still reads.

## Parquet backends

The default `Direct` backend reads Parquet files asynchronously through the
same object store used to open the table. It can use the finer read strategies
shown above. The optional `DeltaKernel` backend delegates data-file reads to
Delta Kernel's synchronous iterator. Both preserve the scan's filtering,
projection, deletion, and schema behavior.

<details markdown="1">
<summary id="predicate-aware-parquet-reads">How filtering avoids decoding and downloading unrelated columns</summary>

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

For supported pages, the reader can also fetch just the selected values inside
a page. [Partial-page reads](https://mag1cfrog.github.io/delta-arrow-reader/partial-page-reads/) explains the layouts, network
cost decision, configuration and measured tradeoffs.

</details>

<details markdown="1">
<summary id="floating-point-row-group-pruning">Why floating-point columns sometimes need extra reads</summary>

NaN values need special handling before statistics can rule out a row group.
See the [floating-point pruning rules](https://mag1cfrog.github.io/delta-arrow-reader/scan-planning/#floating-point-row-group-pruning)
for the exact conditions.

</details>

## Cancellation

The reader starts file work gradually. Dropping the stream stops new work;
Python callers can close their reader or leave its `with` block. Synchronous
work already inside a dependency call may continue until a safe stopping point.
The [scheduling guide](https://mag1cfrog.github.io/delta-arrow-reader/read-scheduling/)
explains how reading ahead, buffering, and cancellation fit together.

## Out of scope

Delta Arrow Reader reads existing tables. It does not write Delta tables,
manage transactions, create a Tokio runtime for Rust callers, or coordinate
application workflows.

## Go deeper

- [Metadata lifecycle](https://mag1cfrog.github.io/delta-arrow-reader/delta-metadata-lifecycle/): why keeping a table open can reduce repeated work.
- [Scan planning](https://mag1cfrog.github.io/delta-arrow-reader/scan-planning/): how selected files become parallel work.
- [Read scheduling](https://mag1cfrog.github.io/delta-arrow-reader/read-scheduling/): how the reader overlaps work and reacts to a slow consumer.
- [Partial-page reads](https://mag1cfrog.github.io/delta-arrow-reader/partial-page-reads/): when fetching individual values can reduce downloads.
