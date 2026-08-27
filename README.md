# Delta Arrow Reader

<h3 align="center">
  <strong>Delta Lake in. Arrow batches out. No Spark required.</strong>
</h3>

Delta Arrow Reader is a read-only Rust library that streams Delta Lake data as
Arrow batches, with optional SQL through DataFusion.

<p align="center">
  <a href="https://docs.rs/delta-arrow-reader"><img alt="Rust API" src="https://docs.rs/delta-arrow-reader/badge.svg"></a>
  <a href="https://crates.io/crates/delta-arrow-reader"><img alt="crates.io" src="https://img.shields.io/crates/v/delta-arrow-reader.svg"></a>
</p>

For guided examples and design details, see the
[Delta Arrow Reader documentation](https://mag1cfrog.github.io/delta-arrow-reader/).

## When to use it

Delta Arrow Reader fits Rust services, command-line tools, and data pipelines
that read Delta Lake tables. It is especially useful when:

- You need to read a large table without holding all of it in memory.
- You want to process each batch as soon as it arrives.
- Your application already works with Arrow data.
- You want to run SQL through DataFusion.

## Why not...

Most alternatives solve a much bigger problem than reading a Delta table. Their
Delta path pays for that extra weight.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/mag1cfrog/delta-arrow-reader/main/docs/content/assets/reader-benchmark-wall-dark.svg">
  <source media="(prefers-color-scheme: light)" srcset="https://raw.githubusercontent.com/mag1cfrog/delta-arrow-reader/main/docs/content/assets/reader-benchmark-wall-light.svg">
  <img alt="Wall-time comparison across five Delta readers and four workloads" src="https://raw.githubusercontent.com/mag1cfrog/delta-arrow-reader/main/docs/content/assets/reader-benchmark-wall-light.svg" width="1000">
</picture>

### Spark or Trino

Spark is Delta Lake's home ground, and Trino is a proven distributed engine.
They fit naturally when a cluster is already part of the system. A small,
single-node read service would still carry their JVM, full query runtime, and
operational machinery. Running either one just to stream Arrow batches is
bringing a distributed system to do a library's job.

### The "read everything" engines

DuckDB, Polars, and Daft promise one engine for many formats. Delta Lake becomes
another compatibility box to check, and the jack-of-all-trades tradeoff showed
clearly in our benchmarks. DuckDB took 5.8-14.6 times as long as Delta Arrow
Reader, and Polars took 1.7-20 times as long. Of our four workloads, Daft could
run only the text projection; it took 2.1 times as long and rejected the
deletion-vector tables. All three also used more memory in every comparable
run. See the
[benchmark setup and complete results](https://mag1cfrog.github.io/delta-arrow-reader/benchmarks/).

### delta-rs

delta-rs is the closest alternative and covers the full Delta lifecycle,
including writes. Delta Arrow Reader concentrates on asynchronous reads,
bounded memory, Arrow streaming, and efficient deletion vectors. Across the two
projection workloads, it ranged from roughly even with delta-rs to finishing
24% sooner. **On deletion-vector tables, delta-rs took three times as long to
return one live row and seven times as long to scan the full table.**

Databricks now
[recommends deletion vectors for most tables and is rolling out automatic enablement for new tables](https://docs.databricks.com/aws/en/admin/workspace-settings/deletion-vectors).

## Install

Add the reader, Tokio, and the futures utilities used by the example:

```console
cargo add delta-arrow-reader futures-util
cargo add tokio --features macros,rt-multi-thread
```

For DataFusion, follow the
[DataFusion installation instructions](https://mag1cfrog.github.io/delta-arrow-reader/installation/#datafusion-adapter)
to add the matching dependencies.

## Read a table

Load a table and consume its batches from asynchronous code:

```rust,no_run
use delta_arrow_reader::DeltaTableBuilder;
use futures_util::TryStreamExt;

# async fn read_table() -> Result<(), Box<dyn std::error::Error>> {
let table = DeltaTableBuilder::new("/tmp/example-delta-table")
    .load_table()
    .await?;
let mut batches = table.scan().build().await?.into_stream();

while let Some(batch) = batches.try_next().await? {
    println!("rows={}", batch.num_rows());
}
# Ok(())
# }
```

Once this works, the
[streaming reader quickstart](https://mag1cfrog.github.io/delta-arrow-reader/streaming-reader/)
shows how to select columns, filter rows, limit results, and inspect metrics.

## Query with DataFusion

Enable the `datafusion` feature when you want to register a Delta table with a
DataFusion `SessionContext`. Registration loads the Delta metadata; Parquet data
is read when DataFusion executes the query.

The [DataFusion quickstart](https://mag1cfrog.github.io/delta-arrow-reader/datafusion/)
walks through registration and a first SQL query.

## Scope

The reader can load the latest or a selected table snapshot. It supports column
selection, row filters, result limits, deletion vectors, bounded read
scheduling, and optional DataFusion integration.

It does not write Delta tables, manage transactions, create a Tokio runtime, or
provide Delta Funnel orchestration, reporting, or Python APIs.

## Documentation

- [Streaming reader quickstart](https://mag1cfrog.github.io/delta-arrow-reader/streaming-reader/)
- [DataFusion quickstart](https://mag1cfrog.github.io/delta-arrow-reader/datafusion/)
- [Architecture](https://mag1cfrog.github.io/delta-arrow-reader/architecture/)
- [Execution options](https://mag1cfrog.github.io/delta-arrow-reader/reference/execution-options/)
- [Scan metrics](https://mag1cfrog.github.io/delta-arrow-reader/reference/metrics/)
- [Reader benchmarks](https://mag1cfrog.github.io/delta-arrow-reader/benchmarks/)
- [Rust API reference](https://docs.rs/delta-arrow-reader)

## Development

For local checks and documentation setup, see the
[development guide](https://mag1cfrog.github.io/delta-arrow-reader/contributing/).
