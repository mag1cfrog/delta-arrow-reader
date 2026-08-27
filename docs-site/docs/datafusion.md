# Query a Table with DataFusion

Use this path when your application already uses DataFusion or you want to
query a Delta table with SQL. This quickstart registers one table and reads a
small result from it.

## Before you start

Add the [DataFusion dependencies](installation.md#datafusion-adapter). You will
also need the path to a Delta table that your application can read.

## Register and query the table

```rust
use datafusion::prelude::SessionContext;
use delta_arrow_reader::{
    DeltaDataFusionScanOptions, DeltaTableBuilder, register_delta_table,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let context = SessionContext::new();
    let table = DeltaTableBuilder::new("/tmp/example-delta-table")
        .load_table()
        .await?;

    register_delta_table(
        &context,
        "orders",
        table,
        DeltaDataFusionScanOptions::default(),
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

The first step loads the table's Delta metadata. Registering the table gives it
a name in DataFusion, but does not open its Parquet data files. DataFusion reads
those files when `collect` runs the query. The `LIMIT` keeps this first result
small.

Once the query works, you can read about [how the reader works](architecture.md)
or use the [Rust API reference](https://docs.rs/delta-arrow-reader) to explore
scan options and metrics.
