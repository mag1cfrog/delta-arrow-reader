# Delta Arrow Reader

<h3 align="center">
  <strong>Delta Lake in. Arrow batches out. No Spark required.</strong>
</h3>

<p align="center">
  <a href="https://docs.rs/delta-arrow-reader"><img alt="Rust API" src="https://docs.rs/delta-arrow-reader/badge.svg"></a>
  <a href="https://crates.io/crates/delta-arrow-reader"><img alt="crates.io" src="https://img.shields.io/crates/v/delta-arrow-reader.svg"></a>
</p>

Delta Arrow Reader is a fast, memory-efficient Delta Lake reader for Rust.

- **Get results faster.** Spend less time downloading and processing data your
  query doesn't need.
- **Use less memory.** Process results as they arrive, without keeping the whole
  result in RAM.
- **Keep deployment simple.** Add the reader to your Rust app, with optional SQL
  through DataFusion.

**Faster than delta-rs, DuckDB, Polars, and single-machine Spark** in all eight
of our [selective-read benchmark cases](docs/content/benchmarks/selective-read-results.md),
measured by median query time.

## Get started

The [documentation](https://mag1cfrog.github.io/delta-arrow-reader/) covers setup
and guides for streaming and SQL. The
[Rust API reference](https://docs.rs/delta-arrow-reader) has the types and methods.
