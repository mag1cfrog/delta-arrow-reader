# Diagnose a failed read in Rust

Check the error returned when opening a table, planning a scan, or reading a
batch. Logs can show the work leading up to that failure; errors remain
available when logging is disabled.

For other interfaces, see [Python logging](python-api/logging.md) or
[CLI diagnostics](cli.md#errors-and-process-status).

## Show reader progress

If your application already configures `tracing`, enable INFO for the
`delta_arrow_reader` target in its existing configuration.

For a standalone application, add these entries to `[dependencies]` in
`Cargo.toml`:

```toml
tracing = "0.1"
tracing-subscriber = "0.3"
```

Set up logging once, at startup, before running reader work:

```rust
fn main() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_writer(std::io::stderr)
        .init();

    // Run the application after configuring logging.
}
```

This writes INFO, WARN, and ERROR events to stderr. WARN and ERROR report
problems; INFO also shows operation progress. Change `INFO` to `DEBUG` for
internal details, or `TRACE` for the most verbose diagnostics. The configuration
above also applies to other crates in your application that use `tracing`.

## Inspect a returned error

Use the error's attributes to identify the failed stage and available cause:

```no_run
use delta_arrow_reader::DeltaTableBuilder;

async fn inspect_error() {
    if let Err(error) = DeltaTableBuilder::new("/path/to/table").load_table().await {
        eprintln!("{}: {}", error.code(), error.reason());
        eprintln!("Phase: {}", error.phase().as_str());
        if let Some(kind) = error.source_kind() {
            eprintln!("Cause: {kind}");
        }
    }
}
```

`phase` identifies the stage that failed. `code` and `reason` describe the
reader failure, while `source_kind` can distinguish causes such as a missing
object or denied access. Some errors have no underlying source; others provide
only a broad or unknown category.

These values omit paths, credentials, and raw dependency messages. See the
[error API](https://docs.rs/delta-arrow-reader/latest/delta_arrow_reader/enum.DeltaReaderError.html)
for individual variants.

## Check the whole read

A query can fail after returning some batches. Keep handling errors until the
stream reaches its end. With DataFusion, check the query's result as well; a
reader progress message does not establish that every SQL operator succeeded.

## Integrate with tracing

If you consume structured events or configure scoped subscribers, use the
[tracing event reference](reference/tracing.md). It defines event fields,
operation lifecycles, and propagation into DataFusion workers.
