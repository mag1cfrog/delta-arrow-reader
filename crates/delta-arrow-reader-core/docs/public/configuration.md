---
title: Configuration
description: Choose when to prepare a table and how much work the reader can do at once.
---

# Configuration

Opening a table can prepare work for later queries. You can keep the default
preparation, skip network sampling, or defer more work until you query. You can
also limit concurrent reads and buffering to suit your application's resources.

## Choose an initialization mode

The `warmup` setting controls preparation before the table is ready to use:

| Mode | Choose it when | What happens |
| --- | --- | --- |
| `automatic` (default) | You will run several filtered queries against an S3 table. | On supported S3 connections, opening the table caches file metadata and samples the network. Later queries reuse both. |
| `query_planning` | You want reusable file metadata without network probes, including for local tables. | Opening takes longer and retains metadata in memory. Each query can skip rebuilding that metadata. |
| `none` | You want less work at startup or do not want to retain the file metadata cache. | Opening skips preparation. Each query builds the metadata it needs, and no network probes run. |

File metadata describes which files belong to the table and includes statistics
used to skip files. Caching it uses memory while the table is held open; it does
not cache query results. Even with `none`, opening still loads the table version
and schema.

Local tables do not get automatic preparation. Select `query_planning` explicitly
to cache their metadata. Network sampling requires the default `direct` backend
and a supported S3 connection.
The [partial-page guide](https://mag1cfrog.github.io/delta-arrow-reader/partial-page-reads/)
describes the supported S3 configurations and the optimization they enable.

## Understand the five-second budget

On supported S3 connections, automatic network sampling has a **five-second
budget after metadata loading**. It can finish sooner. This is neither a fixed
delay nor a limit on the whole table-loading operation: metadata loading adds
its own time. Sampling reads small portions of data files, so it also adds
requests and downloaded bytes. These measurements help the reader decide when
smaller reads will be faster.

An incomplete or failed network sample does not by itself fail table loading.
The first later query that can use partial-page reads retries sampling once,
with the same budget. That retry adds to the query's time, and concurrent
queries share it. If it also fails, the reader uses normal reads without another
sampling attempt.

Keep the loaded table for later queries to reuse its metadata and network
measurements. Registering it in a SQL session reuses that state too. Loading the
same path into a new table starts preparation again; there is no cache on disk.

Both `query_planning` and `none` skip network sampling, including the retry.
Without those measurements, partial-page reads stay inactive. File and row-group
pruning still work. When comparing modes for a short-lived application, measure
table loading plus all its queries together.

## Control concurrency and memory

A scan can read several groups of files at once. These groups are called
partitions; settings marked "per partition" apply separately to each group.

| Setting and default | When to change it |
| --- | --- |
| `max_concurrent_file_reads_per_scan`: automatic | Set a positive limit to reduce the number of files one scan reads at once. A lower limit can reduce resource use, but may make the query slower. |
| `target_partitions`: automatic | Set a smaller positive target to reduce parallel work. Usually leave it automatic so the reader can account for available CPU and memory. |
| `prefetch_files_per_partition`: `2` | Prepare upcoming files while current files are read. Set `0` to avoid preparing files ahead of demand, for example when often stopping early. |
| `output_buffer_batches_per_partition`: `1` | Increase it if reading should run further ahead of your batch processing. More queued batches use more memory. |

The file-read cap applies to each scan, not to every query in the process
combined. A file read can make several network requests. These settings control
query work, not the initialization probes, and are not a total memory limit.
Memory also includes decoding, SQL operations and batches your application keeps.

The default `direct` backend supports prefetch and partial-page reads. For
backend selection, buffering small files, and the full list of defaults, see
the [execution options reference](https://mag1cfrog.github.io/delta-arrow-reader/reference/execution-options/).

## Apply the settings

### Python

After [installing the Python package](https://mag1cfrog.github.io/delta-arrow-reader/installation/#python),
replace the location below with your table. This example keeps automatic
preparation and caps each scan at four concurrent file reads:

```python
from delta_arrow_reader import DeltaTable, ScanExecutionOptions, Session

options = ScanExecutionOptions(max_concurrent_file_reads_per_scan=4)
table = DeltaTable(
    "s3://bucket/table",
    warmup="automatic",
    execution_options=options,
)

with table.to_reader(limit=10) as reader:
    print(reader.read_all())

session = Session()
session.register_table("events", table, execution_options=options)
with session.sql("SELECT * FROM events LIMIT 10") as reader:
    print(reader.read_all())
```

Change `warmup` to `"query_planning"` or `"none"` to select another mode.
Initialization happens in `DeltaTable(...)`; registration reuses the loaded table.
SQL registrations have their own default scan settings, so pass `options` at
registration too if SQL should use your custom limits.

Direct scans inherit the table's settings. To change one scan, pass
`execution_options=ScanExecutionOptions(...)` to `to_reader()` or `scan()`.
This replaces the entire options object for that scan. Pass `target_partitions`
directly to those methods, for example `table.to_reader(target_partitions=2)`.
Changing scan options cannot undo initialization that has already happened.

### Rust

Use `DeltaTableBuilder` to choose preparation and defaults for direct scans.
This function runs inside your application's Tokio runtime:

```no_run
use delta_arrow_reader::{
    DeltaReaderError, DeltaScanExecutionOptions, DeltaTable, DeltaTableBuilder,
    WarmupMode,
};

async fn open_table() -> Result<DeltaTable, DeltaReaderError> {
    let options = DeltaScanExecutionOptions::new()
        .with_max_concurrent_file_reads_per_scan(Some(4))?;
    DeltaTableBuilder::new("s3://bucket/table")
        .with_warmup(WarmupMode::Automatic)
        .with_execution_options(options)
        .load_table()
        .await
}
```

Use `WarmupMode::QueryPlanning` or `WarmupMode::None` for the other modes.
Rust also supports a custom sampling budget. For example, replace the
`with_warmup` call above with:

```ignore
.with_warmup(WarmupMode::Network {
    max_duration: std::time::Duration::from_secs(2),
})
```

The budget applies to sampling after metadata preparation, and to the one
possible retry. Zero skips sampling but still prepares metadata. Python
currently exposes the three named modes, without a custom duration.

For Rust SQL queries, also set `datafusion::ScanOptions.execution_options` when
[registering the table](https://mag1cfrog.github.io/delta-arrow-reader/datafusion/).
For one direct scan, use `scan().with_execution_options(options)` and
`with_target_partitions(...)`. As in Python, a scan override replaces its
execution settings and leaves the loaded table's defaults unchanged.
