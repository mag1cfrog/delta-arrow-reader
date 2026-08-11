# delta-arrow-reader

`delta-arrow-reader` is a read-only Delta Lake reader for Apache Arrow record
batches. It provides a direct pull-driven stream API and an optional DataFusion
table provider. The caller owns the Tokio runtime, and scans do not collect the
whole result in memory.

## Installation

The 0.1.1 package declaration is:

```toml
[dependencies]
delta-arrow-reader = "0.1.1"
futures-util = "0.3"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

Enable the DataFusion adapter and add the matching DataFusion dependency when
you need SQL integration:

```toml
[dependencies]
datafusion = { version = "54.1.0", default-features = false, features = ["sql"] }
delta-arrow-reader = { version = "0.1.1", features = ["datafusion"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

## Read a table directly

Use `load_async` from asynchronous code. The returned stream exposes live scan
metrics and yields batches as the caller requests them.

```rust,no_run
use delta_arrow_reader::{DeltaComparison, DeltaPredicate, DeltaScalar, DeltaTableBuilder};
use futures_util::TryStreamExt;

# async fn read_table() -> Result<(), Box<dyn std::error::Error>> {
let table = DeltaTableBuilder::new("/tmp/example-delta-table")
    .load_async()
    .await?;
let scan = table
    .scan()
    .with_projection(vec!["id".into(), "name".into()])
    .with_predicate(DeltaPredicate::Compare {
        column: "id".into(),
        op: DeltaComparison::GtEq,
        value: DeltaScalar::Int64(10),
    })
    .with_limit(100)
    .build()
    .await?;
let mut batches = scan.execute().await?;
let metrics = batches.metrics();

while let Some(batch) = batches.try_next().await? {
    println!("rows={}", batch.num_rows());
}

println!("files={}", metrics.snapshot().files_completed);
# Ok(())
# }
```

`DeltaTableBuilder::load` is the blocking table-load alternative. Scan building
and execution remain asynchronous.

## Register a DataFusion table

The `datafusion` feature exposes `DeltaTableProvider`, `register_delta_table`,
and DataFusion execution metrics. Registration loads no data files; reads begin
when DataFusion executes a query.

```rust,no_run
# #[cfg(feature = "datafusion")]
# async fn query() -> Result<(), Box<dyn std::error::Error>> {
use datafusion::prelude::SessionContext;
use delta_arrow_reader::{
    DeltaDataFusionScanOptions, DeltaTableBuilder, register_delta_table,
};

let context = SessionContext::new();
let table = DeltaTableBuilder::new("/tmp/example-delta-table")
    .load_async()
    .await?;
register_delta_table(
    &context,
    "orders",
    table,
    DeltaDataFusionScanOptions::default(),
)?;

let batches = context.sql("SELECT * FROM orders").await?.collect().await?;
println!("batches={}", batches.len());
# Ok(())
# }
```

## Features

| Feature | Default | Purpose |
| --- | --- | --- |
| `native-async` | Yes | Native asynchronous Parquet data-file reader and I/O metrics. |
| `official-kernel` | No | Official Delta Kernel data-file reader backend. |
| `datafusion` | No | DataFusion provider, registration, filtering, execution, and metrics. |

At least one reader backend must be enabled to execute a scan. Select the
backend for a scan with `DeltaReaderExecutionOptions::with_reader_backend`.
When default features are enabled, NativeAsync is selected by default.

## Runtime, errors, and metrics

- The caller supplies the Tokio runtime and drives returned streams.
- Execution limits, buffering, Parquet metadata prefetch, and optional
  full-file reads are configured through `DeltaReaderExecutionOptions`.
- `DeltaReaderError::phase` and `DeltaReaderError::as_str` return stable,
  redacted categories. Dependency failures remain available through the
  standard error source chain.
- `DeltaReadMetrics` is a cloneable live handle. `snapshot` returns an
  immutable point-in-time view. NativeAsync Parquet I/O counters are `None`
  when the OfficialKernel backend is selected.

## Scope

The crate supports the extracted read path: snapshot selection, protocol and
schema loading, projections, predicates, deletion vectors, partition planning,
bounded scheduling, NativeAsync and OfficialKernel data-file reads, and the
optional DataFusion adapter.

It does not write Delta tables, manage transactions, create a Tokio runtime,
or provide Delta Funnel orchestration, reporting, or Python APIs.

See [architecture](https://github.com/mag1cfrog/delta-arrow-reader/blob/main/docs/architecture.md),
[provenance](https://github.com/mag1cfrog/delta-arrow-reader/blob/main/docs/provenance.md),
and the [security policy](https://github.com/mag1cfrog/delta-arrow-reader/blob/main/SECURITY.md)
for repository details.

## Development checks

The repository CI runs every feature combination. The focused local checks are:

```console
cargo test --locked --all-features
RUSTDOCFLAGS="-D warnings" cargo doc --locked --all-features --no-deps
cargo package --locked
```
