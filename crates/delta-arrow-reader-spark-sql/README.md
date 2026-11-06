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

The test profile omits debug information and requests symbol stripping to reduce
artifact sizes. To keep symbols and line tables for a focused test backtrace:

```sh
CARGO_PROFILE_TEST_DEBUG=line-tables-only \
CARGO_PROFILE_TEST_STRIP=none \
RUST_BACKTRACE=1 \
cargo test --locked --manifest-path crates/delta-arrow-reader-spark-sql/Cargo.toml --workspace --test collection_size
```

Replace `collection_size` with the integration test target you are debugging.

The public API provides session configuration, query resolution and physical
planning. Its crate-level example shows how to execute a query using the host
session. The host must install `configure_spark_sql` before using `plan_query`.

See [source provenance](UPSTREAM.md) for the retained implementation and dependency
forks. The accepted-runtime test checks preservation of recorded outcomes; some
of those outcomes have known Spark differences and are not compatibility passes.

Floating DISTINCT set results can differ from Spark in their `0.0`/`-0.0` text
after projection and LIMIT optimization. The [set-result references](tests/fixtures/README.md#floating-intersect-and-except-results)
record this limitation separately from equality, row counts and NULL behavior.

REGEXP_EXTRACT and REGEXP_REPLACE use the native regex matcher, with recorded
limits for Java lookbehind, final line terminators, Unicode word categories,
optional UTF-16 advancement and regions beginning inside a surrogate pair.
Exact empty-pattern and raw nonboundary UTF-16 replacement remain supported.
The [regex capability references](tests/fixtures/regexp-replace-README.md#native-regex-capability-limits)
define the measured inputs and complete outcomes. Other pattern, value, field
or error differences require investigation.

The native STRING/BINARY capability boundary uses Arrow's valid UTF-8
STRING representations and permits arbitrary BINARY bytes. BINARY-to-STRING
conversion, mixed CONCAT and CONCAT_WS require valid UTF-8 in each evaluated,
visible, non-NULL value. Spark's malformed-byte internal STRING representation
is outside this subset; checked native errors and TRY_CAST NULL results remain
strict compatibility differences. Bytes outside visible slices, NULL payloads
and skipped branches do not narrow supported inputs. Empty strings, embedded
NUL, valid Unicode, NULL handling, pure BINARY operations and the accepted CONCAT
evaluation policy retain their existing contracts. The
[measured profiles](tests/fixtures/README.md#native-stringbinary-capability-boundary)
record complete outcomes, including all 384 strict Spark differences.
