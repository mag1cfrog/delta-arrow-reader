# Installation

Delta Arrow Reader requires Rust 1.94 or newer. The dependencies you need
depend on whether you plan to use the streaming API or DataFusion.

## Streaming reader

For streaming Arrow batches, add the reader, Tokio, and the futures utilities
used by the quickstart:

```toml
[dependencies]
delta-arrow-reader = "0.3.0"
futures-util = "0.3"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

The [streaming reader quickstart](https://mag1cfrog.github.io/delta-arrow-reader/streaming-reader/)
shows how to load a table and consume its Arrow batches.

## DataFusion adapter

For SQL queries, enable the reader's `datafusion` feature and add the matching
DataFusion release:

```toml
[dependencies]
datafusion = { version = "54.1.0", default-features = false, features = ["sql"] }
delta-arrow-reader = { version = "0.3.0", features = ["datafusion"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

The [DataFusion quickstart](https://mag1cfrog.github.io/delta-arrow-reader/datafusion/)
shows how to register a table and query it with SQL.

## Optional features

The streaming API and both Parquet backends are always available.

| Feature | Default | Purpose |
| --- | --- | --- |
| `datafusion` | No | Register Delta tables with DataFusion and expose execution metrics. |
| `experimental-parquet-metadata-preparation` | No | Prepare and retain Parquet metadata while loading a table. |

The direct Parquet backend is selected by default. Rust callers can choose the
Delta Kernel backend through `DeltaScanExecutionOptions` without changing
Cargo features.

Parquet metadata preparation works with the streaming API by itself. Enable
both optional features to reuse prepared metadata through DataFusion. See
[Prepare Parquet metadata for repeated queries](https://mag1cfrog.github.io/delta-arrow-reader/prepared-parquet-metadata/)
before enabling it for a table.

Both APIs run on your application's Tokio runtime. Delta Arrow Reader does not
create a separate runtime.
