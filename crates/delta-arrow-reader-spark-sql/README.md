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

The frontend admits one read-only statement:

- `SELECT`, `WITH`, `VALUES` or `TABLE`, including supported nested queries and
  set operations over registered providers.
- `DESCRIBE [TABLE] t`, `DESC t`, and `DESCRIBE [QUERY] <query>`.
- `SHOW COLUMNS IN t` or `SHOW COLUMNS FROM t`, `SHOW TABLES [IN schema]
  [LIKE 'pattern']`, and `SHOW SCHEMAS` or `SHOW DATABASES`, optionally with `LIKE`.
- `EXPLAIN [EXTENDED|FORMATTED|COST|CODEGEN] <query>`.

`DESCRIBE` returns one row per provider or query-result column, in field order,
with Spark type names and stored field comments. It omits Spark's partition
information rows, including on partitioned Delta tables. `SHOW` uses the host's
registered catalog and schema names; only providers reporting `TableType::Temporary`
are temporary, and ordinary views retain their registered namespace. Patterns
match complete names, ignore surrounding whitespace, and support case-insensitive
`*` wildcards and `|` alternatives. Invalid alternatives are ignored. The native
matcher uses Unicode case folding; Spark's default ASCII case folding differs
for some non-ASCII patterns. Java-only regex constructs such as lookbehind are
not supported by the native matcher. These limits are recorded in
[known differences](https://github.com/mag1cfrog/delta-arrow-reader/issues/1248).

`EXPLAIN` returns one STRING column named `plan` and one row containing
DataFusion's logical and physical plans. It plans the query without collecting
its rows. The text differs from Spark's and is not compared with Spark's text.
The [metadata references](tests/fixtures/read-only-metadata/README.md) record the
values, types, provider requests and storage checks.

Writes, DDL, maintenance commands and session changes remain rejected, along with
`SHOW CREATE TABLE`, `SHOW PARTITIONS`, `SHOW TBLPROPERTIES`, `SHOW FUNCTIONS`,
`SHOW VIEWS`, `DESCRIBE FUNCTION`, `DESCRIBE DATABASE`, `DESCRIBE HISTORY`,
`DESCRIBE DETAIL`, extended or column-specific descriptions, `EXPLAIN ANALYZE`,
and `EXPLAIN` of a non-query statement. Direct format paths such as
``parquet.`/path/to/data` `` must be replaced with a host-registered table provider.
Ordinary qualified catalog names and quoted names such as ``public.`events.json` ``
remain available.

Static table names and literal `IDENTIFIER` names are checked before resolution,
including inside nested queries. Computed `IDENTIFIER` names are checked after
their expressions are evaluated, before catalog lookup. An earlier host table
function can already have performed I/O by then. See
[known differences](https://github.com/mag1cfrog/delta-arrow-reader/issues/1248)
for this computed-name boundary.

SQL cannot install or invoke external Python/JVM function implementations. The
host can register native scalar and table functions, which keep their existing
execution contract for ordinary queries. Metadata query bodies use registered
providers and frontend built-ins; host scalar and table functions are rejected
before their callbacks. Registered Rust providers retain responsibility for
their metadata and scan-planning contracts. The [admission references](tests/fixtures/read-only-admission/README.md)
cover ordinary query results and the tested rejection paths through the public
API, with actual Delta scans, file reads and writes observed.

SQL configuration references such as `${name}` are expanded before parsing by
default. Supply explicit values in `SparkSqlOptions::variable_substitution_values`
or set `variable_substitution_enabled` to false to preserve the original text.
The unprefixed, `spark`, `sparkconf`, `hivevar` and `hiveconf` namespaces share
that map; missing names become empty text. Expansion is recursive and rejects
cycles. It affects SQL text, including raw literals and identifiers. Provider
values and native `SessionContext.sql` keep their own behavior. The expanded
command must still be one supported read-only statement.

This boundary reads no environment variables, JVM properties or implicit host
configuration. Other namespaces, including `env` and `system`, remain literal.
Values are redacted when their name or value matches the SQL options or secret
redaction pattern, before recursive expansion. The defaults match the pinned
Spark configuration. Overrides named `spark.sql.redaction.options.regex` and
`spark.redaction.regex` in the explicit map use native regex syntax with ASCII
character classes; expressions unsupported by the native UTF-8 matcher fail
planning. This is a SQL-configuration subset, not full Java regex or ambient
Spark-session configuration support.

See [source provenance](UPSTREAM.md) for the retained implementation and dependency
forks. The accepted-runtime test checks preservation of recorded outcomes; some
of those outcomes have known Spark differences and are not compatibility passes.

The [real Delta integration references](tests/fixtures/real-delta-integration/README.md)
compare public Spark SQL planning with equivalent DAR native SQL over real Delta
snapshots in both ANSI modes. They also record provider requests and physical
reads. Shared core limits remain for data-column IN and nullable OR file pruning,
and for unreferenced bytes included in the default metadata prefetch.

The [Delta streaming tests](tests/delta_streaming.rs) and
[snapshot checks](tests/delta_provider.rs) run native SQL and both Spark ANSI
modes over the same real Delta files. They check metadata-only planning and
incremental results for plain scans, filters, projections and LIMIT. Other checks
cover independent executions, retained snapshots, stream drop, task abort and
late read failures. Read gates acknowledge cancellation and resource release;
reexecuting the same plan checks that its shared read permits remain usable.
Separate controls exercise session partition targets, file-read concurrency and
the host memory pool's operator limits. Session target expectations include
DAR's automatic resource caps; explicit targets separately prove two-file
overlap. Memory checks measure DataFusion operator reservations in the supplied
pool.

The [execution-feature references](tests/fixtures/execution-features/README.md)
cover bounded `range` values and BIGINT result types. `SORT BY` orders each output
partition. `DISTRIBUTE BY` places equal keys, including NULL, in the same output
partition. `CLUSTER BY` also sorts each partition by those keys ascending, with
NULL first. Both use the session target partition count; their hash function and
key-to-partition assignment can differ from Spark. `DISTRIBUTE BY` can be combined
with `SORT BY`, and both distribution clauses work in subqueries and CTEs under
an outer `ORDER BY` or `LIMIT`. Partition IDs refer to the execution layout;
monotonic IDs are unique and increase within a partition. Unordered `first`,
`last` and `collect_list` checks use membership and counts.

For directly registered `DeltaTableProvider` snapshots, `input_file_name()`
returns each row's full file URI. It returns an empty STRING for nonfile sources
and opaque host provider contracts. A join with multiple file sources rejects
`input_file_name()`; qualified metadata fields can identify each source instead.
The fields `_metadata.file_path` and `_metadata.file_name` expose the same URI
and the decoded object-key basename. Stored columns and preceding SELECT aliases
named `_metadata` take priority. Generated fields stay out of ordinary and
qualified `SELECT *`, `DESCRIBE` and `SHOW COLUMNS` output.

File URIs retain the Core reader's canonical scheme and percent-encoded path,
including the bucket or container. Authentication components, query strings and
fragments are removed; an Azure container username remains part of the store
identity. File size and modification time are not offered. Planning uses Core's
opt-in file metadata provider without changing the session's registrations.
Unrequested metadata is projected away, and ordinary data scans retain Core's
existing read path. Normal column projection pushdown avoids building unused
metadata constants; hosts that disable pruning may build them without changing
results or file reads. Native `SessionContext::sql` keeps its original schema
and behavior.

The [file-origin references](tests/fixtures/file-metadata/README.md) compare
`input_file_name()` with Spark 4.2.0 reading local Parquet files, including simple
subqueries and CTEs. The capture environment has no Delta Lake installation, so
these references do not establish Spark Delta metadata parity. Real Delta
fixtures separately check row-to-file membership, deletion vectors and column
mapping in both ANSI modes.
Spark can report empty `input_file_name()` values after broadcasting the file
side of a file/nonfile join. This reader retains that source's per-row URI.
The [reference controls](tests/fixtures/file-metadata/README.md) record this
execution-policy difference separately from strict value comparisons.

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
