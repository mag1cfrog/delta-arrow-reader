# Read a Table as a Stream

In this quickstart, you will load a Delta table and read up to 100 rows from two
columns. The rows arrive as Arrow record batches.

## Before you start

Add the [streaming reader dependencies](https://mag1cfrog.github.io/delta-arrow-reader/installation/#streaming-reader). You will
also need the path to a Delta table that your application can read.

## Run the scan

```no_run
use delta_arrow_reader::DeltaTableBuilder;
use futures_util::TryStreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaTableBuilder::new("/tmp/example-delta-table")
        .load_table()
        .await?;
    let scan = table
        .scan()
        .with_projection(["id", "name"])
        .with_limit(100)
        .build()
        .await?;
    let mut batches = scan.into_stream();

    while let Some(batch) = batches.try_next().await? {
        println!("rows={}", batch.num_rows());
    }

    Ok(())
}
```

Loading the table and reading its rows happen at different times. `load_table`
loads an immutable Delta snapshot and its Arrow schema. Each `build` then
evaluates the Delta scan metadata to select the active files and columns it
needs. Parquet files are not read until the returned batch stream is polled.

## Reuse scan metadata across queries

The default `load_table` path is a good fit for a table that you will query once
or only occasionally. If one process will plan several queries against the same
loaded table, opt into eager Delta scan metadata initialization:

```no_run
use delta_arrow_reader::DeltaTableBuilder;
use futures_util::TryStreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaTableBuilder::new("/tmp/example-delta-table")
        .load_table_with_eager_scan_metadata()
        .await?;

    let scan = table
        .scan()
        .with_projection(["id", "name"])
        .with_limit(100)
        .build()
        .await?;
    let mut batches = scan.into_stream();

    while let Some(batch) = batches.try_next().await? {
        println!("rows={}", batch.num_rows());
    }

    Ok(())
}
```

The eager method resolves only after the active-file metadata and available
file statistics have been materialized. Later scan builds reuse that retained
metadata without another Delta log/checkpoint replay. This can help repeated
selective queries where metadata planning is a large part of latency. The
metadata remains in memory for the lifetime of the loaded table.

Eager initialization does not read Parquet footers or Parquet data. A scan
build still applies its own projection and predicate, and polling the returned
stream performs the query's Parquet I/O. The loaded table stays pinned to one
immutable snapshot; load the table again to see a newer version.

To understand what the eager method caches, and why it helps only some
workloads, read about the [Delta metadata lifecycle](https://mag1cfrog.github.io/delta-arrow-reader/delta-metadata-lifecycle/).

## Filter rows

Once the basic scan works, you can add a predicate before building it:

```ignore
use delta_arrow_reader::{DeltaComparison, DeltaPredicate, DeltaScalar};

let scan = table
    .scan()
    .with_predicate(DeltaPredicate::Compare {
        column: "id".into(),
        op: DeltaComparison::GtEq,
        value: DeltaScalar::Int64(10),
    })
    .build()
    .await?;
```

A predicate does two jobs. It helps the reader skip files that cannot contain
matching rows, and it filters the rows that are read. The final result stays
correct even when the table statistics cannot skip a file.

## Inspect scan metrics

If you want to see what the scan did, save its metrics handle before consuming
the stream:

```ignore
let mut batches = scan.into_stream();
let metrics = batches.metrics();

while let Some(batch) = batches.try_next().await? {
    println!("rows={}", batch.num_rows());
}

println!("tasks={}", metrics.snapshot().file_tasks_completed);
```

You can still inspect the handle after the stream finishes or is dropped. If
you drop the stream early, the reader stops scheduling new files.
