# Delta Arrow Reader

<h3 align="center">
  <strong>Delta Lake in. Arrow batches out. No Spark required.</strong>
</h3>

<p align="center">
  <a href="https://docs.rs/delta-arrow-reader"><img alt="Rust API" src="https://docs.rs/delta-arrow-reader/badge.svg"></a>
  <a href="https://crates.io/crates/delta-arrow-reader"><img alt="crates.io" src="https://img.shields.io/crates/v/delta-arrow-reader.svg"></a>
</p>

Delta Arrow Reader is a read-only Rust library built for queries that need a
small slice of a large Delta Lake table. It skips unnecessary data and streams
Apache Arrow batches into your service, CLI, or pipeline.

- **Pruning beyond files.** Skip irrelevant files and row groups. Where Parquet
  page indexes allow it, use matching rows to read only the output pages you need.
- **Filter first, decode less.** Evaluate supported predicates before decoding
  output columns, so wide projections don't force unrelated data through the
  decoder.
- **Arrow as it arrives.** Process batches without buffering the whole result,
  with bounded read scheduling.

Use the Arrow stream directly, or query through DataFusion. Both use the same
reader, with support for Delta snapshots, schema changes, and deletion vectors.

See how it compares with delta-rs, DuckDB, Polars, and single-machine Spark in
the [selective-read benchmarks](docs/content/benchmarks/selective-read-results.md),
including the layouts where performance is close.

## Get started

The [documentation](https://mag1cfrog.github.io/delta-arrow-reader/) covers setup
and guides for streaming and SQL. The
[Rust API reference](https://docs.rs/delta-arrow-reader) has the types and methods.
