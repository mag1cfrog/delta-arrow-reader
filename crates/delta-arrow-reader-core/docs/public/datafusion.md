# Query a table with DataFusion in Rust

Register a Delta table with DataFusion, then query it with SQL. Start with a
small result before writing a larger query.

For SQL from Python, use the [Python quickstart](https://mag1cfrog.github.io/delta-arrow-reader/python/#query-with-sql) instead.

## Before you start

Add the [DataFusion dependencies](https://mag1cfrog.github.io/delta-arrow-reader/installation/#datafusion-adapter)
to your Rust application. You also need an existing Delta table. Use its
directory, which contains `_delta_log`, rather than a single Parquet file.

## Run your first query

Replace `/path/to/table` with your table's path:

```no_run
use datafusion::prelude::SessionContext;
use delta_arrow_reader::{
    DeltaTableBuilder,
    datafusion::{ScanOptions, register_table},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let context = SessionContext::new();
    let table = DeltaTableBuilder::new("/path/to/table")
        .load_table()
        .await?;

    register_table(&context, "events", table, ScanOptions::default())?;

    context
        .sql("SELECT * FROM events LIMIT 10")
        .await?
        .show()
        .await?;
    Ok(())
}
```

This prints up to ten rows. `register_table` gives the loaded table the name
`events` inside this session. It does not copy the table or read its rows.
`show()` runs the query and prints the result.

## Select rows and columns

Once the first query works, change the SQL. For example, if your table has
`id` and `region` columns:

```sql
SELECT id, region
FROM events
WHERE id >= 100
ORDER BY id
LIMIT 10
```

Use column names from your own table. Without `ORDER BY`, row order is not
guaranteed. Register more tables under different names to join them in SQL.

## Use the result in your application

Replace `.show()` with `.collect()` to get a vector of Arrow batches:

```ignore
let batches = context
    .sql("SELECT * FROM events LIMIT 10")
    .await?
    .collect()
    .await?;
```

`collect()` keeps the whole result in memory. For larger results, use
DataFusion's `execute_stream()` and process batches as they arrive. Sorting,
grouping, and joining may still need substantial memory.

## Query the table again

Reuse the same session and registration for later queries. The registration
keeps the table version it received, even if another process writes new data.
Opening a new table or refreshing one does not replace an existing registration.

<details markdown="1">
<summary id="refresh-a-registered-provider">Refresh a registration in a long-running service</summary>

Keep the concrete `DeltaTableProvider` when a long-running service needs to
refresh its registration:

```no_run
use std::sync::Arc;

use datafusion::prelude::SessionContext;
use delta_arrow_reader::{
    DeltaTableBuilder,
    datafusion::{DeltaTableProvider, ScanOptions},
};

async fn refresh_events(
    context: &SessionContext,
    provider: &mut DeltaTableProvider,
) -> Result<(), Box<dyn std::error::Error>> {
    let refreshed = provider.refresh().await?;
    let previous = context.deregister_table("events")?;

    if let Err(error) = context.register_table("events", Arc::new(refreshed.clone())) {
        if let Some(previous) = previous {
            let _ = context.register_table("events", previous)?;
        }
        return Err(error.into());
    }

    *provider = refreshed;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let context = SessionContext::new();
    let table = DeltaTableBuilder::new("/path/to/table")
        .load_table()
        .await?;
    let mut provider = DeltaTableProvider::try_new(table, ScanOptions::default())?;
    let _ = context.register_table("events", Arc::new(provider.clone()))?;
    refresh_events(&context, &mut provider).await?;
    Ok(())
}
```

The returned provider keeps the existing scan options and range-read
estimates. It also rebuilds the DataFusion schema if the Delta schema changed.
The original provider remains usable, so queries that already hold it continue
against the old snapshot.

DataFusion uses separate deregistration and registration calls, so there is a
short interval with no `events` registration. If that gap matters, pause new
query planning while the two calls run. The example restores the previous
provider if the new registration fails.

`DeltaTableProvider::refresh` does not poll in the background. The application
chooses when to refresh and when to replace its registration.

</details>

## Diagnose a failed read

Install your application's tracing subscriber before loading the table, planning
the query, and consuming its results. A filter such as `delta_arrow_reader=warn`
includes optional warmup warnings and terminal reader failures. ERROR records
carry the error category, safe source kind, plan and execution IDs, and the
partition index without requiring DEBUG spans. The returned DataFusion error
still retains the reader error and its sources.

At INFO, `provider_scan.completed` reports that planning returned a physical
plan. `datafusion_partition_execution.completed` reports EOF for one partition.
An upstream `LIMIT` can instead produce a partition `dropped` event after it has
enough rows. Check the query's returned result for whole-query success.
See [Tracing](reference/tracing.md) for subscriber setup, event fields, and the
boundaries covered by the reader.

## Where to go next

- [Configuration](https://mag1cfrog.github.io/delta-arrow-reader/configuration/): prepare a table for repeated queries or limit concurrent file reads.
- [Metadata lifecycle](https://mag1cfrog.github.io/delta-arrow-reader/delta-metadata-lifecycle/): understand which work queries can reuse.
- [Rust API reference](https://docs.rs/delta-arrow-reader): look up the adapter and scan options.
