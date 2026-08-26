# Installation

Delta Arrow Reader requires Rust 1.94 or newer. The dependencies you need
depend on whether you want an Arrow stream or a DataFusion table.

## Direct reader

For a direct Arrow stream, add the reader, Tokio, and the futures utilities used
by the quickstart:

```toml
[dependencies]
delta-arrow-reader = "0.3.0"
futures-util = "0.3"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

You can now continue to the [direct reader quickstart](direct-reader.md).

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

## Optional features

Most applications can keep the default features. Change them only when you
need DataFusion or want to select the other reader backend.

| Feature | Default | Purpose |
| --- | --- | --- |
| `native-async` | Yes | Read Parquet files asynchronously and report data-file I/O metrics. |
| `official-kernel` | No | Use the official Delta Kernel data-file reader backend. |
| `datafusion` | No | Register Delta tables with DataFusion and expose execution metrics. |

A scan needs at least one reader backend. Unless you turn off the default
features, the reader uses `native-async`.

Both APIs run on your application's Tokio runtime. Delta Arrow Reader does not
create a separate runtime.
