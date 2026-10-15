# Reuse a loaded table

A query needs to find the files belonging to its table version before reading rows.
Repeating that preparation can take time, even when the query returns very
little data. Keeping a table open lets queries reuse information about its
files when metadata preparation is enabled.

This page explains what the reader can reuse and when you need to refresh it.
For code examples and setting names, see
[Configuration](https://mag1cfrog.github.io/delta-arrow-reader/configuration/).

## What happens before rows are read

The Delta log records changes to the table. A checkpoint summarizes that
history. The reader uses them to build a file list for one table version,
including available statistics such as the values each file might contain.
This information is the table's *scan metadata*.

Every query uses its own conditions to narrow that file list. It then reads
metadata inside the remaining Parquet files, followed by the data it needs.
A prepared table can reuse the file list and statistics. It does not cache query
results, Parquet footers, data pages, or decoded batches.

## No warmup

With `WarmupMode::None`, opening a table reads its version and column schema.
Each query then builds the file information it needs:

```text
Open table:  load version and schema
Query 1:     build file list -> select data -> read rows
Query 2:     build file list -> select data -> read rows
```

This keeps opening work small and avoids retaining a file list in memory.
Repeated queries pay for that preparation again. Local tables use this lazy
behavior by default.

## Query-planning warmup

With `WarmupMode::QueryPlanning`, opening also builds and keeps the file
information. Queries reuse it while applying their own filters:

```text
Open table:  load version and schema -> prepare file list
Query 1:     reuse file list -> select data -> read rows
Query 2:     reuse file list -> select data -> read rows
```

Opening takes longer, and the file information stays in memory while the table
is loaded. Later queries avoid rebuilding it. This can matter when filtering
leaves very little data to read and finding that data is a large part of the cost.

<details markdown="1">
<summary>What the cache contains and who owns it</summary>

`DeltaTableBuilder::with_warmup(WarmupMode::QueryPlanning)` finishes preparation
before `load_table()` returns. The cache contains reconciled Delta `add`
metadata for active files and all available file statistics, rather than the
raw commit history. Each scan passes it to Delta Kernel through
`Scan::scan_metadata_from`; Kernel still applies that query's predicate.

The cache belongs to one loaded table version. Opening the same location twice
creates separate caches. There is no TTL, eviction, disk persistence, or sharing
between independently loaded tables. The reader does not choose additional
tables to warm automatically. Keeping a cache does not change row selection,
Parquet I/O, or deletion-vector handling.

</details>

## Which mode should you use?

The modes apply whether you read batches directly or use DataFusion SQL:

| Mode | Work when opening | Work on later queries |
| --- | --- | --- |
| `Automatic` (default) | Prepare file information and measure the connection on supported S3 stores; other stores remain lazy. | Reuse that preparation when available. |
| `QueryPlanning` | Prepare file information, without test reads of data files. | Reuse the file information; partial-page reads stay off. |
| `None` | Load the version and schema without retaining the file list. | Build file information for each query; partial-page reads stay off. |

Automatic S3 preparation also measures whether smaller data requests will help.
The [connection check](https://mag1cfrog.github.io/delta-arrow-reader/partial-page-reads/#measure-the-connection)
has its own time and I/O costs.

Compare opening time plus all the queries your application will run. There is
no fixed query count at which preparation pays off: the table's history, file
count, statistics, storage latency, query selectivity, and available memory all
matter. Use `None` when the opening cost or retained metadata outweighs reuse;
use `QueryPlanning` when you want metadata reuse without network sampling.

## Version and refresh behavior

A loaded table stays on the version it opened. Later writes do not change it
or scans already built from it. `refresh()` opens the latest version as a new
table, leaving the original usable even if refresh fails. If the version has
not changed, the returned table reuses the snapshot, schema, and prepared cache.

Your application decides when to refresh. The reader does not poll in the
background. For SQL, refreshing a provider also rebuilds its schema when needed,
but does not replace the old registration in a DataFusion session. See the
[registration refresh example](https://mag1cfrog.github.io/delta-arrow-reader/datafusion/#refresh-a-registered-provider).

<details markdown="1">
<summary>Incremental refresh, checkpoints, and failures</summary>

With no warmup, the returned table has no retained active-file cache. Its scans
continue to assemble scan metadata when they are built. With query-planning
warmup, refresh passes the old version and retained active-file metadata to
Delta Kernel. Delta Kernel reconciles commits written after that version. If a
newer checkpoint prevents incremental reconciliation, it performs a full
metadata replay from that checkpoint instead.

Refresh returns an error rather than a partially updated table when the latest
snapshot, schema, or retained query-planning metadata cannot be loaded. The
caller can keep using the original table after such an error. A lazy table may
load a newer unsupported protocol for inspection, but its scans still reject
that protocol. Refreshing a warmed table fails if the newer protocol cannot be
used to rebuild its query-planning cache.

</details>

## Related guides

- [Configuration](https://mag1cfrog.github.io/delta-arrow-reader/configuration/): choose preparation and resource settings.
- [Repeated-query results](https://mag1cfrog.github.io/delta-arrow-reader/benchmarks/eager-metadata/): see initialization and two queries measured separately.
- [Controlled metadata experiment](https://github.com/mag1cfrog/delta-arrow-reader/blob/main/docs/content/benchmarks/eager-metadata.md): compare lazy and eager loading on your table.
- [Warmup Rust API](https://docs.rs/delta-arrow-reader/latest/delta_arrow_reader/struct.DeltaTableBuilder.html#method.with_warmup): look up the available modes.
