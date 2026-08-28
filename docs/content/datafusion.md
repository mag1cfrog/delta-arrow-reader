# Query a Table with DataFusion

Use this path when your application already uses DataFusion or you want to
query a Delta table with SQL. This quickstart registers one table and reads a
small result from it.

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

The first step loads an immutable Delta snapshot and its Arrow schema.
Registering the table gives it a name in DataFusion, but does not open its
Parquet files. DataFusion plans and reads those files when `collect` runs the
query. The `LIMIT` keeps this first result small.

## Reuse scan metadata across SQL queries

Opt into eager Delta scan metadata initialization before registering a table
that will serve several SQL queries:

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

Registration does not build another cache. Every SQL query through the
registered provider shares the loaded table and its retained Delta scan
metadata without another Delta log/checkpoint replay. Parquet footer and
Parquet data I/O remain query-specific and follow each query's projection and
predicate.

Choose eager loading for named, high-use tables and keep one-shot or rarely
queried tables on the default `load_table` path. The crate does not maintain a
table-name registry or choose a mode automatically. To refresh a registered
table, load a new immutable snapshot and replace the registration with the new
table.

For a closer look at how one loaded table reuses this cache across queries, see
the [Delta metadata lifecycle](https://mag1cfrog.github.io/delta-arrow-reader/delta-metadata-lifecycle/).

Once the query works, you can read about [how the reader works](https://mag1cfrog.github.io/delta-arrow-reader/architecture/)
or use the [Rust API reference](https://docs.rs/delta-arrow-reader) to explore
scan options and metrics.
