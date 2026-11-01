# DAR Spark SQL crate

This crate plans Spark SQL against tables registered in a DataFusion session.
The host supplies table providers, credentials and execution resources. Query
results use DataFusion execution plans and Arrow batches.

The implementation consolidates the accepted frozen Spark experiment. It is not
enabled in the DAR CLI or Python package. Known compatibility gaps remain in the
[restart plan](../../plans/spark-sql-restart.md).

Build and test from the repository root:

```sh
cargo check --locked --manifest-path crates/delta-arrow-reader-spark-sql/Cargo.toml
cargo test --locked --manifest-path crates/delta-arrow-reader-spark-sql/Cargo.toml --workspace
```

Use Rust 1.96 or later. This crate has its own Cargo workspace and lockfile so its
DataFusion, Arrow and lexical patches do not change existing DAR release builds.
Normal compilation uses the source under `vendor/`; it does not apply patches or
reconstruct files from experimental build caches.

`--workspace` also runs the retained Sail crates' unit tests with the same
dependency overrides. The crate's CI uses these commands on Linux, macOS and
Windows.

The public API provides session configuration, query resolution and physical
planning. Its crate-level example shows how to execute a query using the host
session. The host must install `configure_spark_sql` before using `plan_query`.

See [source provenance](UPSTREAM.md) for the retained implementation and dependency
forks. The accepted-runtime test checks preservation of recorded outcomes; some
of those outcomes have known Spark differences and are not compatibility passes.

Floating DISTINCT set results can differ from Spark in their `0.0`/`-0.0` text
after projection and LIMIT optimization. The [set-result references](tests/fixtures/README.md#floating-intersect-and-except-results)
record this limitation separately from equality, row counts and NULL behavior.
