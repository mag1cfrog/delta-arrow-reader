# Installation

Delta Arrow Reader requires Rust 1.94 or newer. The dependencies you need
depend on whether you plan to use the streaming API or DataFusion.

## Streaming reader

For streaming Arrow batches, add the reader, Tokio, and the futures utilities
used by the quickstart:

```bash
cargo add delta-arrow-reader futures-util
cargo add tokio --features macros,rt-multi-thread
```

The [streaming reader quickstart](https://mag1cfrog.github.io/delta-arrow-reader/streaming-reader/)
shows how to load a table and consume its Arrow batches.

## DataFusion adapter

For SQL queries, enable the reader's `datafusion` feature and add DataFusion:

```bash
cargo add delta-arrow-reader --features datafusion
cargo add datafusion --no-default-features --features sql
cargo add tokio --features macros,rt-multi-thread
```

The [DataFusion quickstart](https://mag1cfrog.github.io/delta-arrow-reader/datafusion/)
shows how to register a table and query it with SQL.

## Optional Cargo feature

The streaming API and both Parquet backends are always available. Enable the
optional `datafusion` feature to register Delta tables with DataFusion and
expose execution metrics.

The direct Parquet backend is selected by default. Rust callers can choose the
Delta Kernel backend through `DeltaScanExecutionOptions` without changing
Cargo features.

Both APIs run on your application's Tokio runtime. Delta Arrow Reader does not
create a separate runtime.
