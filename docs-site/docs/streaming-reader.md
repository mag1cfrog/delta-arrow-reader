# Read a Table as a Stream

In this quickstart, you will load a Delta table and read up to 100 rows from two
columns. The rows arrive as Arrow record batches.

## Before you start

Add the [streaming reader dependencies](installation.md#streaming-reader). You will
also need the path to a Delta table that your application can read.

## Run the scan

```rust
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
loads the Delta metadata, and `build` works out which files and columns the scan
needs. The data files are not read until the returned batch stream is polled.

## Filter rows

Once the basic scan works, you can add a predicate before building it:

```rust
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

```rust
let mut batches = scan.into_stream();
let metrics = batches.metrics();

while let Some(batch) = batches.try_next().await? {
    println!("rows={}", batch.num_rows());
}

println!("tasks={}", metrics.snapshot().file_tasks_completed);
```

You can still inspect the handle after the stream finishes or is dropped. If
you drop the stream early, the reader stops scheduling new files.
