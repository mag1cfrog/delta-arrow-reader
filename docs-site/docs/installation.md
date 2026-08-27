# Installation

Delta Arrow Reader requires Rust 1.94 or newer. The dependencies you need
depend on whether you want an Arrow stream or a DataFusion table.

## Streaming reader

For streaming Arrow batches, add the reader, Tokio, and the futures utilities used
by the quickstart:

```toml
[dependencies]
delta-arrow-reader = "0.3.0"
futures-util = "0.3"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

You can now continue to the [streaming reader quickstart](streaming-reader.md).

## DataFusion adapter

For SQL queries, enable the reader's `datafusion` feature and add the matching
DataFusion release:

```toml
[dependencies]
datafusion = { version = "54.1.0", default-features = false, features = ["sql"] }
delta-arrow-reader = { version = "0.3.0", features = ["datafusion"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

You can now continue to the [DataFusion quickstart](datafusion.md).

## Optional feature

The streaming API and both data-file reader backends are always available. The
only optional feature adds the DataFusion integration.

| Feature | Default | Purpose |
| --- | --- | --- |
| `datafusion` | No | Register Delta tables with DataFusion and expose execution metrics. |

The direct Parquet backend is selected by default. Rust callers can choose the
Delta Kernel backend through `DeltaReaderExecutionOptions` without changing
Cargo features.

Both APIs run on your application's Tokio runtime. Delta Arrow Reader does not
create a separate runtime.
