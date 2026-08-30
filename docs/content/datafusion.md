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
    DeltaTableBuilder, WarmupMode,
    datafusion::{ScanOptions, register_table},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let context = SessionContext::new();
    let table = DeltaTableBuilder::new("/tmp/example-delta-table")
        .with_warmup(WarmupMode::QueryPlanning)
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

`WarmupMode::QueryPlanning` builds the cache while `load_table` loads the
table. `register_table` uses that same loaded table and does not build another
cache. All SQL queries through the registered provider can therefore reuse the
cached metadata without replaying the Delta log or checkpoint. Each query
still applies its own projection and predicate and reads its own Parquet
footers and data.

Use query-planning warmup for a named table that will be queried repeatedly.
Keep the default `WarmupMode::None` when you will query it only once or
occasionally. The crate does not maintain a table-name registry or choose a
mode automatically.

A registered provider stays on the Delta version that it loaded. Refreshing a
provider does not modify the DataFusion catalog entry that points to the old
provider.

## Refresh a registered provider

Keep the concrete `DeltaTableProvider` when a long-running service needs to
refresh its registration:

```no_run
use std::sync::Arc;

use datafusion::prelude::SessionContext;
use delta_arrow_reader::{
    DeltaTableBuilder,
    datafusion::{DeltaTableProvider, ScanOptions},
};

async fn refresh_orders(
    context: &SessionContext,
    provider: &mut DeltaTableProvider,
) -> Result<(), Box<dyn std::error::Error>> {
    let refreshed = provider.refresh().await?;
    let previous = context.deregister_table("orders")?;

    if let Err(error) = context.register_table("orders", Arc::new(refreshed.clone())) {
        if let Some(previous) = previous {
            let _ = context.register_table("orders", previous)?;
        }
        return Err(error.into());
    }

    *provider = refreshed;
    Ok(())
}

# async fn setup() -> Result<(), Box<dyn std::error::Error>> {
let context = SessionContext::new();
let table = DeltaTableBuilder::new("/tmp/example-delta-table")
    .load_table()
    .await?;
let mut provider = DeltaTableProvider::try_new(table, ScanOptions::default())?;
let _ = context.register_table("orders", Arc::new(provider.clone()))?;
refresh_orders(&context, &mut provider).await?;
# Ok(())
# }
```

The returned provider keeps the existing scan options and range-read
estimates. It also rebuilds the DataFusion schema if the Delta schema changed.
The original provider remains usable, so queries that already hold it continue
against the old snapshot.

DataFusion uses separate deregistration and registration calls, so there is a
short interval with no `orders` registration. If that gap matters, pause new
query planning while the two calls run. The example restores the previous
provider if the new registration fails.

`DeltaTableProvider::refresh` does not poll in the background. The application
chooses when to refresh and when to replace its registration.

The [Delta metadata lifecycle](https://mag1cfrog.github.io/delta-arrow-reader/delta-metadata-lifecycle/)
follows this cache across several queries. For the rest of the read path, see
[how the reader works](https://mag1cfrog.github.io/delta-arrow-reader/architecture/).
The [Rust API reference](https://docs.rs/delta-arrow-reader) documents the
available scan options and metrics.
