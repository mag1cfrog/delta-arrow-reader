# delta-arrow-reader

`delta-arrow-reader` is a read-only Delta Lake reader for Apache Arrow record
batches. It provides a direct pull-driven stream API and an optional DataFusion
table provider. The caller owns the Tokio runtime, and scans do not collect the
whole result in memory.

See the [documentation](https://mag1cfrog.github.io/delta-arrow-reader/) for
quickstarts and design details. The
[reader benchmarks](https://mag1cfrog.github.io/delta-arrow-reader/benchmarks/)
compare the crate with delta-rs and DuckDB on projection and deletion-vector
workloads.

## Installation

The 0.3.0 package declaration is:

```toml
[dependencies]
delta-arrow-reader = "0.3.0"
futures-util = "0.3"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

Enable the DataFusion adapter and add the matching DataFusion dependency when
you need SQL integration:

```toml
[dependencies]
datafusion = { version = "54.1.0", default-features = false, features = ["sql"] }
delta-arrow-reader = { version = "0.3.0", features = ["datafusion"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

## Read a table directly

Use `load_table` from asynchronous code. The returned stream exposes live scan
metrics and yields batches as the caller requests them.

```rust,no_run
use delta_arrow_reader::{DeltaComparison, DeltaPredicate, DeltaScalar, DeltaTableBuilder};
use futures_util::TryStreamExt;

# async fn read_table() -> Result<(), Box<dyn std::error::Error>> {
let table = DeltaTableBuilder::new("/tmp/example-delta-table")
    .load_table()
    .await?;
let scan = table
    .scan()
    .with_projection(["id", "name"])
    .with_predicate(DeltaPredicate::Compare {
        column: "id".into(),
        op: DeltaComparison::GtEq,
        value: DeltaScalar::Int64(10),
    })
    .with_limit(100)
    .build()
    .await?;
let mut batches = scan.execute()?;
let metrics = batches.metrics();

while let Some(batch) = batches.try_next().await? {
    println!("rows={}", batch.num_rows());
}

println!("tasks={}", metrics.snapshot().file_tasks_completed);
# Ok(())
# }
```

## Register a DataFusion table

The `datafusion` feature exposes the `datafusion` module with its table provider,
registration helper, scan options, and execution metrics. Registration loads no
data files; reads begin when DataFusion executes a query.

```rust,no_run
# #[cfg(feature = "datafusion")]
# async fn query() -> Result<(), Box<dyn std::error::Error>> {
use datafusion::prelude::SessionContext;
use delta_arrow_reader::{
    DeltaTableBuilder,
    datafusion::{ScanOptions, register_table},
};

let context = SessionContext::new();
let table = DeltaTableBuilder::new("/tmp/example-delta-table")
    .load_table()
    .await?;
register_table(
    &context,
    "orders",
    table,
    ScanOptions::default(),
)?;

let batches = context.sql("SELECT * FROM orders").await?.collect().await?;
println!("batches={}", batches.len());
# Ok(())
# }
```

The DataFusion provider uses Arrow view arrays for string and binary data-file
columns and dictionary arrays for string and binary partition columns. Direct
reader scans retain the Delta table's ordinary Arrow schema.

For transformation-heavy queries that benefit from ordinary Arrow arrays,
disable view types without changing partition dictionary encoding:

```rust
# #[cfg(feature = "datafusion")]
# fn configure() {
# use delta_arrow_reader::datafusion::ScanOptions;
let scan_options = ScanOptions {
    use_arrow_view_types: false,
    ..Default::default()
};
# let _ = scan_options;
# }
```

Whole-file planning normally avoids extra ranged reads once it fills the scan
partition target. For skewed direct Parquet scans, allow repartitioning at any
partition count with `datafusion::IntraFileRepartitioning::Always`.
DataFusion's `repartition_file_scans` and `repartition_file_min_size` settings
still control whether repartitioning runs.

## Optional feature

| Feature | Default | Purpose |
| --- | --- | --- |
| `datafusion` | No | DataFusion provider, registration, filtering, execution, and metrics. |

The direct API and both data-file reader backends are always available.
`ParquetReaderBackend::Direct` is the default. Advanced callers can select
`ParquetReaderBackend::DeltaKernel` with
`DeltaReaderExecutionOptions::with_reader_backend`.

## Runtime, errors, and metrics

- The caller supplies the Tokio runtime and drives returned streams.
- Execution limits, buffering, Parquet metadata prefetch, and optional
  full-file reads are configured through `DeltaReaderExecutionOptions`.
- DataFusion scan metrics report whether the provider requested view arrays.
- `DeltaReaderError::phase` and `DeltaReaderError::code` return stable,
  redacted categories. Dependency failures remain available through the
  standard error source chain.
- `DeltaScanMetrics` is a cloneable live handle. `snapshot` returns an
  immutable point-in-time view. Direct Parquet I/O counters are `None` when
  the Delta Kernel backend is selected.

## Scope

The crate supports the extracted read path: snapshot selection, protocol and
schema loading, projections, predicates, deletion vectors, partition planning,
bounded scheduling, direct Parquet and Delta Kernel data-file reads, and the
optional DataFusion adapter.

It does not write Delta tables, manage transactions, create a Tokio runtime,
or provide Delta Funnel orchestration, reporting, or Python APIs.

See [architecture](https://mag1cfrog.github.io/delta-arrow-reader/architecture/),
[provenance](https://mag1cfrog.github.io/delta-arrow-reader/provenance/),
and the [security policy](https://github.com/mag1cfrog/delta-arrow-reader/blob/main/SECURITY.md)
for repository details.

## Development checks

The repository CI runs every feature combination. The focused local checks are:

```console
cargo test --locked --all-features
RUSTDOCFLAGS="-D warnings" cargo doc --locked --all-features --no-deps
cargo package --locked
```
