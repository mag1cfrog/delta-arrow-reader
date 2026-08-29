# Read a table as a stream

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

Loading the table and reading its rows are separate steps. `load_table` selects
one Delta table version and reads its Arrow schema. The loaded table stays on
that version. Each `build` evaluates the Delta scan metadata to choose the
active files and requested columns. The reader opens the Parquet files only
after you poll the returned batch stream.

## Reuse scan metadata across queries

The default `load_table` path works well when you query a table once or only
occasionally. If one process will run several queries against the same loaded
table, you can cache its reusable scan metadata during table loading:

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

`load_table_with_eager_scan_metadata` returns after it has built the cache. The
cache holds active-file metadata and available file statistics in memory.
Later scan builds reuse it instead of replaying the Delta log or checkpoint.
This reduces repeated planning work when metadata accounts for much of a
selective query's latency. The cache remains in memory as long as the loaded
table does.

The cache does not contain Parquet footers or Parquet data. Each scan still
applies its own projection and predicate, then performs its Parquet I/O when
you poll the returned stream. Load the table again when you want to read a
newer Delta version.

The [Delta metadata lifecycle](https://mag1cfrog.github.io/delta-arrow-reader/delta-metadata-lifecycle/)
explains what the eager method caches and when the tradeoff is worthwhile.

For a long-lived process that repeatedly queries Parquet files on remote
storage, the experimental
[Parquet metadata preparation guide](https://mag1cfrog.github.io/delta-arrow-reader/prepared-parquet-metadata/)
describes a second opt-in cache for file footers and offset indexes.

## Filter rows

Add a predicate before building the scan to filter its rows:

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

A predicate can help the reader skip files that cannot contain matching rows.
It also filters the rows read from the remaining files. If the table statistics
cannot rule out a file, the reader reads and filters it so the result remains
correct.

## Inspect scan metrics

Save the metrics handle before consuming the stream to inspect what the scan
did:

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
