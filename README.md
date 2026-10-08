# Delta Arrow Reader

<h3 align="center">
  <strong>Delta Lake in. Arrow batches out. No Spark required.</strong>
</h3>

<p align="center">
  <a href="https://docs.rs/delta-arrow-reader"><img alt="Rust API" src="https://docs.rs/delta-arrow-reader/badge.svg"></a>
  <a href="https://crates.io/crates/delta-arrow-reader"><img alt="crates.io" src="https://img.shields.io/crates/v/delta-arrow-reader.svg"></a>
</p>

Delta Arrow Reader is a fast, memory-efficient Delta Lake reader.

- **Get results faster.** Spend less time downloading and processing data your
  query doesn't need.
- **Use less memory.** Process results as they arrive, without keeping the whole
  result in RAM.
- **Skip the cluster setup.** Read and filter Delta tables in your own
  application, without running Spark.

**Faster than delta-rs, DuckDB, Polars, and single-machine Spark** in all eight
of our [filtered-read benchmarks](https://mag1cfrog.github.io/delta-arrow-reader/benchmarks/selective-read-results/),
using TPC-H-derived tables with about 60 million rows each. Results compare median
query times.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/mag1cfrog/delta-arrow-reader/9c0fba18aa6ed8c6739e5dc0b74a1a081db1c241/docs/content/assets/selective-read-readme-dark.svg">
  <source media="(prefers-color-scheme: light)" srcset="https://raw.githubusercontent.com/mag1cfrog/delta-arrow-reader/9c0fba18aa6ed8c6739e5dc0b74a1a081db1c241/docs/content/assets/selective-read-readme-light.svg">
  <img alt="Five readers query Delta Lake tables with about 60 million rows derived from TPC-H. The four cases use deletion vectors to mark deleted rows, with matching rows grouped together or spread out in 416-column and 90-column tables. Dots show median seconds on a logarithmic axis; lower is faster. Labels also show each reader's time relative to Delta Arrow Reader." src="https://raw.githubusercontent.com/mag1cfrog/delta-arrow-reader/9c0fba18aa6ed8c6739e5dc0b74a1a081db1c241/docs/content/assets/selective-read-readme-light.svg" width="1000">
</picture>

<sub>Showing four cases using deletion vectors to mark deleted rows. <a href="https://mag1cfrog.github.io/delta-arrow-reader/benchmarks/selective-read-results/">Full results</a> include all eight cases, with and without deletion vectors.</sub>

## Get started

Install the reader and Tokio (Rust 1.94 or later):

```bash
cargo add delta-arrow-reader
cargo add tokio --features macros,rt-multi-thread
```

For Python, [build from source to inspect table snapshots](https://mag1cfrog.github.io/delta-arrow-reader/installation/#python).
Python row streaming and published wheels are planned.

Read up to 100 rows from an existing Delta table:

```rust,no_run
use delta_arrow_reader::DeltaTableBuilder;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaTableBuilder::new("/path/to/delta-table")
        .load_table()
        .await?;
    let scan = table.scan().with_limit(100).build().await?;
    let mut batches = scan.into_stream();

    while let Some(batch) = batches.next_batch().await? {
        println!("Read {} rows", batch.num_rows());
    }
    Ok(())
}
```

Save this as `src/main.rs`, replace the table path, and run `cargo run`.

[Read a table](https://mag1cfrog.github.io/delta-arrow-reader/streaming-reader/)
or [query with SQL](https://mag1cfrog.github.io/delta-arrow-reader/datafusion/).
See the [Rust API reference](https://docs.rs/delta-arrow-reader) for types and methods.

For development and benchmark setup, see the
[repository documentation](https://github.com/mag1cfrog/delta-arrow-reader/blob/main/docs/README.md).
