# Read a table with Rust

Open a Delta table and read a small result as Arrow batches. Each batch contains
a group of rows that your application can process together.

## Before you start

Add the [Rust streaming dependencies](https://mag1cfrog.github.io/delta-arrow-reader/installation/#streaming-reader)
to your application. You also need an existing Delta table. Use the table's
directory, which contains `_delta_log`, rather than a single Parquet file.

## Read your first rows

Replace `/path/to/table` with your table's path, then run this program:

```no_run
use delta_arrow_reader::DeltaTableBuilder;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaTableBuilder::new("/path/to/table")
        .load_table()
        .await?;
    println!("Columns: {:?}", table.schema());

    let scan = table.scan().with_limit(10).build().await?;
    let mut batches = scan.into_stream();

    while let Some(batch) = batches.next_batch().await? {
        println!("Read {} rows", batch.num_rows());
    }

    Ok(())
}
```

The program prints the column names and types, then the number of rows in each
batch. It reads at most ten rows in total, with all columns. Row order is not
guaranteed.

Keep the loaded `table` for later queries. Each call to `table.scan()` starts
a new query against the same table version.

## Choose columns

Once the first read works, replace the line that builds `scan` with:

```ignore
let scan = table
    .scan()
    .with_projection(["id", "region"])
    .with_limit(10)
    .build()
    .await?;
```

Use names from your table's schema in place of `id` and `region`. Batches contain
only those columns, in the requested order. Keep the batch-reading loop as it is.

## Filter rows

For a table with a signed 64-bit integer `id` column, add a condition before
building the scan:

```ignore
use delta_arrow_reader::{DeltaComparison, DeltaPredicate, DeltaScalar};

let scan = table
    .scan()
    .with_predicate(DeltaPredicate::Compare {
        column: "id".into(),
        op: DeltaComparison::GtEq,
        value: DeltaScalar::Int64(100),
    })
    .with_limit(10)
    .build()
    .await?;
```

This reads up to ten rows whose `id` is at least `100`. Match the scalar type to
your column's type. You can combine `with_predicate` and `with_projection` to
select both rows and columns. See the
[predicate API](https://docs.rs/delta-arrow-reader/latest/delta_arrow_reader/enum.DeltaPredicate.html)
for other comparisons and combinations.

## Read a larger result

Remove `with_limit(10)` when you are ready to read all matching rows. Keep
processing one batch at a time instead of collecting the whole result in memory.
Dropping the stream when you stop early stops new scan work.

<details markdown="1">
<summary id="refresh-a-loaded-table">Use a newer table version in a long-running application</summary>

A loaded table does not change when someone writes new data. Call `refresh` to
load the latest version when your application is ready to use it:

```no_run
use delta_arrow_reader::{DeltaReaderError, DeltaTable};

async fn refresh_table(table: &mut DeltaTable) -> Result<(), DeltaReaderError> {
    *table = table.refresh().await?;
    Ok(())
}
```

Refresh returns a new table. Existing scans keep their original version. If
refresh fails, the assignment does not run and the original table is still
usable. See the [metadata lifecycle](https://mag1cfrog.github.io/delta-arrow-reader/delta-metadata-lifecycle/#version-and-refresh-behavior)
for how refresh updates cached file information.

</details>

## Where to go next

- [Rust diagnostics](https://mag1cfrog.github.io/delta-arrow-reader/diagnostics/): enable logs or investigate a failed read.
- [Rust configuration](https://mag1cfrog.github.io/delta-arrow-reader/configuration/#configure-rust): choose how much preparation happens when opening a table, including the default S3 connection check.
- [DataFusion quickstart](https://mag1cfrog.github.io/delta-arrow-reader/datafusion/): query the table with SQL.
- [Scan metrics](https://mag1cfrog.github.io/delta-arrow-reader/reference/metrics/): inspect the work a query performed.
- [Rust API reference](https://docs.rs/delta-arrow-reader): look up individual methods and options.
