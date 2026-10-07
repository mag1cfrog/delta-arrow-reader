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
use delta_arrow_reader::{DeltaTableBuilder, WarmupMode};
use futures_util::TryStreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaTableBuilder::new("/tmp/example-delta-table")
        .with_warmup(WarmupMode::QueryPlanning)
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

`WarmupMode::QueryPlanning` tells `load_table` to build the cache before it
returns. The cache holds active-file metadata and available file statistics in
memory. Later scan builds reuse it instead of replaying the Delta log or
checkpoint. This reduces repeated planning work when metadata accounts for
much of a selective query's latency. The cache remains in memory as long as
the loaded table does.

The cache does not contain Parquet footers or Parquet data. Each scan still
applies its own projection and predicate, then performs its Parquet I/O when
you poll the returned stream.

The [Delta metadata lifecycle](https://mag1cfrog.github.io/delta-arrow-reader/delta-metadata-lifecycle/)
explains what query-planning warmup caches and when the tradeoff is worthwhile.

## Refresh a loaded table

Call `refresh` when a long-running process is ready to use the latest Delta
version:

```no_run
use delta_arrow_reader::{DeltaReaderError, DeltaTable};

async fn refresh_table(table: &mut DeltaTable) -> Result<(), DeltaReaderError> {
    *table = table.refresh().await?;
    Ok(())
}
```

`refresh` returns a new immutable table. The original table and scans already
built from it stay on their original version. If the refresh fails, the
assignment does not run and the original table remains available.

With `WarmupMode::None`, the new table remains lazy. With
`WarmupMode::QueryPlanning`, refresh updates the retained active-file metadata
from the previous version. If the table has not changed, the new table reuses
the same cache. A checkpoint written after the previous version may require a
full metadata replay instead.

The crate does not refresh tables on a timer. Call `refresh` from the polling
or scheduling code that owns the table, then publish the returned table when
your application is ready for new scans to see it.

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
