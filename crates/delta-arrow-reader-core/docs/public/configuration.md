---
title: Configuration
description: Start with the defaults, then adjust table opening and resource use when you need to.
---

# Configuration

Start with the defaults. You can change how much work happens before your first
query, then adjust resource use if you need to.

## Start with the defaults

After [installing the Python package](https://mag1cfrog.github.io/delta-arrow-reader/installation/#python),
open a table using its path:

```python
from delta_arrow_reader import DeltaTable

table = DeltaTable("s3://bucket/table")
```

Delta tables store their rows in data files. On supported S3 connections,
opening a table prepares two things that later queries can reuse:

1. It loads information about the table's files and keeps it in memory. Queries
   use this to decide which files they need.
2. It reads parts of data files to check the connection speed. This helps it
   decide when downloading less data will make a query faster.

The connection check can take **up to five seconds** and may finish sooner.
Loading the file information takes additional time, so opening a table can take
more than five seconds overall. Local tables skip this extra preparation.

This behavior is called `automatic`. Keep using the same table object for later
queries to reuse the work. Creating another `DeltaTable` for the same path starts
it again. The saved information stays in memory; it does not cache query results
or persist on disk.

<details markdown="1">
<summary>If the connection check does not finish</summary>

An incomplete check does not by itself fail table loading. The first query that
can use the measurements retries once, with the same five-second budget.
Concurrent queries share the retry. If it also fails, queries use normal reads
without another attempt.

The check makes real requests and downloads data. It requires the default
`direct` backend and a [supported S3 connection](https://mag1cfrog.github.io/delta-arrow-reader/partial-page-reads/).

</details>

## Want less work before the first query?

### Skip the connection check

If you want to keep the useful file information but avoid test reads of data
files, use `query_planning`:

```python
table = DeltaTable("s3://bucket/table", warmup="query_planning")
```

You still wait for the file information to load. It stays in memory for later
queries, which is useful when querying the same table repeatedly. This option
also works for local tables.

### Leave the preparation to each query

If you will query only once, or want to avoid keeping the file information in
memory, use `none`:

```python
table = DeltaTable("s3://bucket/table", warmup="none")
```

Opening still reads the table version and its column names and types. Each
query then builds the file information it needs. Repeated queries may spend
more time doing that work.

Both options skip the connection check, including any later retry. Queries can
still skip unwanted data, but [partial-page reads](https://mag1cfrog.github.io/delta-arrow-reader/partial-page-reads/)
stay off. That optimization needs the connection measurements to decide when
to download selected values within a block of data.

Compare the time to open the table **and** run your queries when choosing a mode.

## More options

<details markdown="1">
<summary>Limit how many files a query reads at once</summary>

If a query uses too many resources, try a limit on concurrent file reads. This
example allows at most four files to be read at once in one scan:

```python
from delta_arrow_reader import ScanExecutionOptions

options = ScanExecutionOptions(max_concurrent_file_reads_per_scan=4)
with table.to_reader(execution_options=options, limit=10) as reader:
    print(reader.read_all())
```

Lowering this limit can reduce resource use, but may slow the query. It applies
to each scan separately, does not cap network requests or total memory, and does
not change the connection check during table loading.

Pass `execution_options` to `DeltaTable(...)` to set defaults for its direct
scans. Passing it to `to_reader()` or `scan()` replaces the full options object
for that one scan.

Other controls let you adjust how far reading runs ahead of your application:

- `prefetch_files_per_partition=0` stops preparing upcoming files in advance.
  The default is `2` per scan partition, a group of files read together.
- `output_buffer_batches_per_partition` defaults to `1`. Increasing it lets
  reading run further ahead of batch processing and uses more memory.
- `target_partitions` controls how many groups can work in parallel. Pass a
  smaller positive target to `to_reader()` or `scan()` to reduce parallel work.
  The default chooses a target using available CPU and memory.

</details>

<details markdown="1">
<summary>Use the loaded table with Python SQL</summary>

Register the table you already opened. SQL reuses its file information and any
connection measurements. Registration does not repeat the preparation.

SQL registrations have their own default read settings. To give SQL a custom
file-read limit, pass it when registering the table:

```python
from delta_arrow_reader import ScanExecutionOptions, Session

options = ScanExecutionOptions(max_concurrent_file_reads_per_scan=4)
session = Session()
session.register_table("events", table, execution_options=options)
```

Omit `execution_options` to use SQL's defaults. Custom read settings passed to
`DeltaTable` do not automatically carry over to SQL.

</details>

<details markdown="1">
<summary>Configure a table in Rust</summary>

Choose the same behavior with `with_warmup`. This example skips the connection
check and keeps the file information for later queries:

```no_run
use delta_arrow_reader::{DeltaReaderError, DeltaTable, DeltaTableBuilder, WarmupMode};

async fn open_table() -> Result<DeltaTable, DeltaReaderError> {
    DeltaTableBuilder::new("s3://bucket/table")
        .with_warmup(WarmupMode::QueryPlanning)
        .load_table()
        .await
}
```

Call this function inside your application's Tokio runtime. Use
`WarmupMode::Automatic` for the default or `WarmupMode::None` to leave preparation
to queries.

Rust also lets you shorten or extend the connection check. To give it two
seconds, replace `with_warmup` above with:

```ignore
.with_warmup(WarmupMode::Network {
    max_duration: std::time::Duration::from_secs(2),
})
```

That limit applies to the check and its possible retry, after file information
has loaded. Zero skips the check but still prepares file information. Python
currently supports the three named modes without a custom time limit.

For Rust read limits, use `DeltaScanExecutionOptions` on the table builder or
one scan. SQL uses `datafusion::ScanOptions.execution_options` when
[registering the table](https://mag1cfrog.github.io/delta-arrow-reader/datafusion/).
Changing read settings later does not undo preparation already performed.

</details>

For backend selection, small-file buffering and the full list of defaults,
see the [execution options reference](https://mag1cfrog.github.io/delta-arrow-reader/reference/execution-options/).
