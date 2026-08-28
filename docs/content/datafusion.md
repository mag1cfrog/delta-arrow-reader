# Query a table with DataFusion

Use DataFusion when your application already depends on it or when you want to
query a Delta table with SQL. This quickstart loads one table, registers it by
name, and runs a query that returns a small result.

## Before you start

Add the [DataFusion dependencies](https://mag1cfrog.github.io/delta-arrow-reader/installation/#datafusion-adapter). You will
also need the path to a Delta table that your application can read.

## Register and query the table

```no_run
use datafusion::prelude::SessionContext;
use delta_arrow_reader::{
    DeltaTableBuilder,
    datafusion::{ScanOptions, register_table},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let context = SessionContext::new();
    let table = DeltaTableBuilder::new("/tmp/example-delta-table")
        .load_table()
        .await?;

    register_table(
        &context,
        "orders",
        table,
        ScanOptions::default(),
    )?;

    let batches = context
        .sql("SELECT * FROM orders LIMIT 100")
        .await?
        .collect()
        .await?;
    println!("batches={}", batches.len());
    Ok(())
}
```

`load_table` selects one Delta table version and reads its Arrow schema.
`register_table` makes that loaded table available in DataFusion as `orders`.
Registration does not open any Parquet files. DataFusion plans the read and
opens those files when `collect` runs the query. The `LIMIT` keeps this first
result small.

## Reuse scan metadata across SQL queries

If the registered table will serve several SQL queries, cache its reusable scan
metadata before registration:

```no_run
use datafusion::prelude::SessionContext;
use delta_arrow_reader::{
    DeltaTableBuilder,
    datafusion::{ScanOptions, register_table},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let context = SessionContext::new();
    let table = DeltaTableBuilder::new("/tmp/example-delta-table")
        .load_table_with_eager_scan_metadata()
        .await?;

    register_table(
        &context,
        "orders",
        table,
        ScanOptions::default(),
    )?;

    let batches = context
        .sql("SELECT * FROM orders LIMIT 100")
        .await?
        .collect()
        .await?;
    println!("batches={}", batches.len());
    Ok(())
}
```

`load_table_with_eager_scan_metadata` builds the cache while loading the table.
`register_table` uses that same loaded table and does not build another cache.
All SQL queries through the registered provider can therefore reuse the cached
metadata without replaying the Delta log or checkpoint. Each query still
applies its own projection and predicate and reads its own Parquet footers and
data.

Use eager loading for a named table that will be queried repeatedly. Keep a
table on the default `load_table` path when you will query it only once or
occasionally. The crate does not maintain a table-name registry or choose a
mode automatically.

A registered table stays on the Delta version that was loaded. To read a newer
version, load the table again and replace the existing registration.

The [Delta metadata lifecycle](https://mag1cfrog.github.io/delta-arrow-reader/delta-metadata-lifecycle/)
follows this cache across several queries. For the rest of the read path, see
[how the reader works](https://mag1cfrog.github.io/delta-arrow-reader/architecture/).
The [Rust API reference](https://docs.rs/delta-arrow-reader) documents the
available scan options and metrics.
