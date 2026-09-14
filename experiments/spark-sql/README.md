# Spark SQL extraction experiment

The [owning issue](https://github.com/mag1cfrog/delta-arrow-reader/issues/113) defines the scope, coverage and reduction rules. This directory contains the test corpus, an independent Apache Spark oracle and a vendored Rust runner over real Delta tables.

`inputs.json` pins Apache Spark 4.2.0 and Sail 0.7.1, session settings, table schemas and data. Both engines receive the same explicit schemas, including nested nullability. The runner verifies those schemas and captures input rows for comparison. This avoids differences caused by each engine inferring its own schema from SQL VALUES.

`queries.jsonl` contains 116 stable query IDs. The existing `seed_*` fields retain earlier expectations. Ordered queries compare sequences; unordered queries compare row multisets with duplicates preserved. Partition-dependent cases compare input IDs and check nonnegative partition IDs, partition-local ordering and unique/nonnegative monotonic IDs. SORT BY and monotonic-ID queries expose partition IDs for those checks. The `known_boundary` marker records earlier findings and does not suppress oracle differences.

## Current result and remaining source

The latest cut is `ad98dd4` on `feat/spark-sql-extraction`. The retained subset has **8 Sail-owned crates and 91,945 gross Rust lines**, including 83,937 production-source lines, 7,928 test lines and 80 build-script lines. All eight crate directories contain retained Sail source. Compared with the reviewed service/streaming checkpoint, the subsequent cuts remove another 10,685 lines. Compared with the original import, 34,068 lines are gone, a 27.0% reduction. Counts include comments and blank lines.

The resolved dependency graph has 524 packages across all targets and 452 Linux normal/build packages, including the runner and reader. No reduction checkpoint adds or upgrades a package. The Rust frontend has no Python, Spark Connect service or Sail storage-reader dependency; Delta scans still use the host provider. This is the smallest subset demonstrated by these cuts, not a minimum or an adoption decision.

| Retained crate | Gross Rust lines | Why it remains |
| --- | ---: | --- |
| `sail-common` | 1,978 | In-process query/expression/type specs and required Arrow metadata. Rejected protocol payload descriptors remain for boundary checks; they have no decoder or executor. |
| `sail-common-datafusion` | 2,131 | Spark value formatting, constant evaluation, output/schema renaming and Variant metadata detection. |
| `sail-function` | 57,237 | Spark scalar and aggregate kernels, coercion, NULL/ANSI behavior, datetime formats and nested values. The small NTILE adapter retains parameter validation. |
| `sail-logical-plan` | 518 | SQL range, required ordering, partition IDs and monotonic-ID descriptors. Some still need physical extension planning. |
| `sail-plan` | 19,009 | SQL function dispatch, name/type resolution, relational planning, field naming, native table lookup and range execution. |
| `sail-sql-analyzer` | 4,349 | Typed conversion from Spark SQL ASTs to query specs, including literals and SQL data types. |
| `sail-sql-macro` | 626 | TreeParser derives used by the grammar and TreeSyntax derives used by the complete syntax snapshot. |
| `sail-sql-parser` | 6,097 | Tokenizer, query/command grammar, ASTs and syntax snapshot support. Commands are rejected by analysis before their bodies are translated. |

The 57,237 lines in `sail-function` include its tests. Its largest groups implement aggregates, datetime/math/string functions, arrays, JSON, CSV and XML. Variant, map, binary/hash, sketch, spatial and other SQL functions also have live registrations. Removing whole families would narrow the support boundary; absence from the 116-case sample does not make them unused.

The remaining bulk implements SQL behavior. Further substantial reduction would require replacing those implementations or narrowing the supported SQL. Several same-name native replacements were considered and left in place:

| Candidate | Reason retained |
| --- | --- |
| Soundex | DataFusion 54.1.0 constructs Utf8 output even for LargeUtf8 input. Casting afterward does not preserve the full LargeUtf8 capacity contract. |
| make_valid_utf8 | Nullability and FixedSizeBinary support differ. |
| parse_url / try_parse_url | Percent-decoding, relative URLs, path handling and coercion differ. |
| Regression aggregates | Sail preserves Spark's floating-point operation order; algebraically equivalent formulas can produce different values. |
| ABS and other numeric kernels | Per-query ANSI settings, interval/duration types, overflow checks and error behavior still require Spark-specific handling. |
| Parser derives and command grammar | TreeParser drives parsing and TreeSyntax protects the full syntax graph. Command grammar supports the existing explicit rejection checks. |

The final check passes 19 runner, 11 planner and 9 Python tests, plus 302 function, 6 common DataFusion and 6 analyzer unit tests. The standalone parser syntax snapshot also passes; neither its expected graph nor the four frozen corpus/baseline files changed during reduction. To run the retained library suites in addition to the runner commands below:

```bash
cargo test --locked --manifest-path experiments/spark-sql/Cargo.toml \
  -p sail-function -p sail-common-datafusion -p sail-sql-analyzer --lib -j 2
```

All 116 import-baseline observations still match: 83 queries succeed, 18 fail planning and 15 fail execution. Spark comparison remains 45 strict matches / 60 differences / 11 pending reference cases; full-Sail comparison remains 83 / 22 / 11. Matching an error stage does not establish matching error conditions. The 19 seed checks and 18 adapter checks still pass.

Missing physical extensions, deletion-vector/snapshot/stream-lifecycle coverage and the adoption decision remain open in the owning issue. Additional checks before deletion found two existing gaps: projecting EXISTS as a SELECT output fails physical planning, and selecting a qualified join key such as `l.a` after `JOIN ... USING (a)` fails resolution. WHERE EXISTS, the merged USING key and qualified ON-join fields work. These cuts do not repair or hide those gaps.

## Run the references

Use separate environments for the full Spark package and Sail's Spark Connect client:

```bash
uv venv --python 3.13 .venv-spark
uv pip install --python .venv-spark/bin/python pyspark==4.2.0 py4j==0.10.9.9
uv venv --python 3.13 .venv-sail
uv pip install --python .venv-sail/bin/python pysail==0.7.1 pyspark-client==4.2.0 'pandas<3'
```

Create these environments outside the checkout. Supply JAVA_HOME when Java is not on PATH. The captured oracle used CPython 3.13.12, Temurin JRE 21.0.12.1+1 and py4j 0.10.9.9. [Spark's supported Java versions](https://spark.apache.org/docs/4.2.0/index.html) are 17, 21 and 25. Java/Python are reference-test tools, not dependencies of the planned Rust frontend.

From the checkout, using the corresponding environment's Python:

```bash
python experiments/spark-sql/reference.py --engine spark --out target/spark-sql/spark-reference.json
python experiments/spark-sql/reference.py --engine sail --out target/spark-sql/sail-reference.json
```

The runner forces the client timezone to UTC and applies each query's session overrides. Spark runs locally with two workers and two shuffle partitions. It checks the 19 earlier successful seed queries for rows and specified field names. Host-only registration/policy cases do not execute against either reference. Ordinary SQL errors are recorded with phase and structured condition where available; a reference capture is not itself a claim of compatibility.

## Check the fixed oracle

`spark-oracle.json` preserves the independent Spark results, schemas, error conditions, versions, input data and corpus hash. It contains 105 executed queries and 11 pending host-adapter cases. `oracle.py` uses only the Python standard library.

```bash
python3 experiments/spark-sql/reference.py --check
python3 -m unittest discover -s experiments/spark-sql -p 'test_oracle.py'
python3 experiments/spark-sql/oracle.py check target/spark-sql/spark-reference.json
python3 experiments/spark-sql/oracle.py check target/spark-sql/sail-reference.json --report target/spark-sql/sail-check.json
```

Checks fail on changed values, schema dimensions, error stage/condition, input data/schema or corpus hash. Missing, duplicate, reordered or unknown case IDs also fail. Values and schema checks remain separate: types, names, nested nullability and metadata are reported independently. Decimal strings are compared exactly; the current corpus needs no floating-point tolerance. Spark's internal `__autoGeneratedAlias` metadata and generated expression names can differ without changing result values, but remain visible in the report.

The Sail check currently fails because upstream Sail differs from Spark. Keep the differences visible when testing the extracted implementation; do not rewrite the oracle to match Sail. Host-only cases have no Spark expectations and remain pending in this comparison; the Rust runner checks them separately. Deletion vectors, retained snapshots, early stream drop and mid-stream failure checks remain pending and should reuse the existing `tests/reader/` fixtures.

Refreshing expectations is an explicit operation after reviewing a changed corpus or reference version:

```bash
python3 experiments/spark-sql/oracle.py freeze target/spark-sql/spark-reference.json
```

Only a complete capture from the pinned Apache Spark runtime may create the oracle. Unclassified Spark errors cannot be frozen. Review the resulting diff; ordinary checks never update expectations. Raw JVM errors and stack traces stay under `target/`.

## Run the extracted frontend

The experiment has its own Cargo workspace and lockfile. [Source provenance and measurements](UPSTREAM.md) describe the pinned Sail subset and its patch. The root crate does not depend on this workspace, and `cargo package` excludes the experiment.

Use Rust 1.97.1 (the tested version) to build the frontend. Building and running the Rust binary requires neither `protoc` nor a Python installation or development libraries. The external capture tool uses Python with PyArrow 25.0.1; the Sail environment above can supply it:

```bash
export SPARK_TEST_PYTHON=/absolute/path/to/venv-sail/bin/python
uv pip install --python "$SPARK_TEST_PYTHON" pyarrow==25.0.1
export CARGO_TARGET_DIR="$PWD/target/spark-sql-build"
mkdir -p target
cargo build --locked --manifest-path experiments/spark-sql/Cargo.toml -j 2 \
  --message-format=json > target/spark-sql-build.jsonl
"$SPARK_TEST_PYTHON" experiments/spark-sql/extracted.py \
  --binary "$CARGO_TARGET_DIR/debug/delta-reader-sail-extraction-probe" \
  --run-dir target/spark-sql/extracted-run
cargo test --locked --manifest-path experiments/spark-sql/Cargo.toml -j 2
cargo test --locked --manifest-path experiments/spark-sql/Cargo.toml -p sail-plan --lib -j 2
"$SPARK_TEST_PYTHON" -m unittest discover -s experiments/spark-sql -p 'test_*.py'
```

Run from the repository root, set the Python path to your environment and use a new run directory each time. The tool creates four Delta fixtures from the fixed inputs, with two partitioned Parquet files for `t`. It registers every table through the existing DeltaTableProvider. The Rust runner applies each case's settings, resolves Spark SQL to a native DataFusion plan, restores output names and writes Arrow IPC streams batch by batch. The Python capture tool collects these tiny test results for comparison.

IPC streams allow partition dictionaries to change between batches. The transport preserves duplicate column names, nested NULL values, exact decimals and microsecond timestamps. Captured input schemas and rows match the Spark oracle. Physical Arrow types remain recorded separately from Spark logical types.

The runner verifies the 19 earlier query seeds and 18 adapter checks: ten excluded operations rejected before execution, qualified registration, four seed Arrow type expectations, Delta scan identity for a filter and join, and filter/pruning metrics. The filtered query plans one file, excludes one file and emits one row from the reader. Each inspected physical plan has emitted zero reader rows before execution. This counter check is narrower than a full assertion that planning performs no I/O. Native DataFusion SQL works before and after the corpus, and fixture file hashes remain unchanged.

Compare with the import baseline, then with the independent Spark oracle:

```bash
python3 experiments/spark-sql/oracle.py check target/spark-sql/extracted-run/capture.json \
  --against experiments/spark-sql/extracted-baseline.json \
  --report target/spark-sql/extracted-run/baseline-check.json
python3 experiments/spark-sql/oracle.py check target/spark-sql/extracted-run/capture.json \
  --report target/spark-sql/extracted-run/spark-check.json
python3 experiments/spark-sql/oracle.py check target/spark-sql/extracted-run/capture.json \
  --against target/spark-sql/sail-reference.json \
  --report target/spark-sql/extracted-run/sail-check.json
```

`extracted-baseline.json` records the import's behavior, including failures. A second complete run matched all 116 recorded statuses/results and input schemas/data. That is a regression check, not 116 successful or Spark-compatible queries. Rust errors currently have no Spark condition mapping, so the baseline checks their stage but cannot prove that two errors at that stage have the same cause. Raw errors and plans remain in the run directory. The checked-in Spark oracle is unchanged.

The import executes 83 queries successfully and records 33 errors, including the ten deliberately rejected operations. Against Spark, 45 cases match strictly, 60 differ and 11 lack reference expectations. Against full Sail, 83 match strictly, 22 differ and those same 11 lack reference expectations. Both reference comparison commands currently exit 1 to keep differences visible.

Four queries accepted by full Sail fail because the extracted physical planner lacks required extension nodes: `sail_partition_id`, `sail_sort_within_partitions`, `extensions_monotonic_id` and `aggregation_ordered_first`. Two other value/type differences from full Sail agree with Spark: `nulls_conditionals` retains BIGINT, and `nested_columns` returns NULL for a NULL struct's age. These observations do not yet isolate whether the difference comes from the provider, optimizer or Connect result path. Other differences from full Sail concern nullability. No case was removed or reclassified to hide these findings.

To refresh the measured inventory without changing the source:

```bash
python3 experiments/spark-sql/inventory.py --build-messages target/spark-sql-build.jsonl \
  --out target/spark-sql/inventory.json
```

## Python UDF removal checkpoint

The approved Python removal slice, committed as `4674528`, retained ten Sail crates. It removed sail-python-udf, sail-pyarrow, PyO3 and the linked resolver/configuration paths. Source shrank from 126,013 to 120,537 gross Rust lines, a reduction of 5,476 lines. That checkpoint had 610 resolved packages across all targets and 540 Linux normal/build packages. The dependency test checks the full resolved graph for Python bridge packages.

The build succeeded with `PYO3_PYTHON` and `PYO3_CONFIG_FILE` set to nonexistent paths. The binary has no libpython dependency or CPython symbol imports. `extracted.py` runs it with an empty usable PATH, no LD_LIBRARY_PATH and an invalid PYO3_PYTHON, while Python/PyArrow remain outside the Rust process as fixture/capture tools.

All 116 observations match the committed import baseline, including schemas, rows and error stages. The 19 query seeds and 18 adapter checks pass, along with three Rust tests and nine Python tests. The Rust tests exercise seven Python plan/expression entrypoints, reject scalar/table named arguments, and verify the remaining SEQUENCE and CONVERT_TIMEZONE paths. The frozen query corpus and both checked-in behavior baselines are unchanged; existing reference differences remain visible.

The common spec still describes Python functions so the resolver can reject those variants. It contains no Python execution implementation. Named scalar/table arguments now fail explicitly; the removed Python keyword handling had discarded names on native-function paths. Named-argument support would need parameter binding before it could be enabled safely.

## Catalog/session removal checkpoint

The approved catalog/session slice, committed as `4987c12`, retained eight Sail crates and 113,834 gross Rust lines, 6,703 fewer than the Python removal checkpoint. The resolved graph has 599 packages across all targets and 529 Linux normal/build packages. No dependency versions changed. The dependency test also rejects sail-catalog and sail-catalog-memory.

Named tables and derived views use DataFusion's registry. The runner no longer installs Sail catalog or PlanService extensions, and Spark's literal, type and expression-name formatting calls the existing SparkPlanFormatter directly. The removed code includes both catalog crates, catalog command/display types and the unused persistent-view resolver.

The native session boundary has three explicit behaviors outside the frozen corpus:

- Spark builtin scalar, aggregate and table functions take precedence over same-name native functions. Native scalar/table functions are available for names absent from Sail's corresponding registry. A known but unsupported Sail function still fails; it does not retry a native implementation. Custom aggregate/window registration is not added by this slice.
- `current_catalog()` returns the native session's default catalog. `current_database()` and `current_schema()` return its default schema name as stored, including dots or backticks. The default session therefore reports `datafusion` and `public`, replacing the runner's unused `sail`/`default` catalog.
- Spark resolution does not change the native function registry. A plain SessionContext can execute Spark `range(3)` without the runner replacing DataFusion's own range registration.

Two new Rust tests first failed on the missing Sail catalog extension. They now cover plain/default/custom sessions, native derived views, scalar/table function lookup, name collisions, unsupported function rejection, output names/types and unchanged native SQL behavior. All five runner tests, eleven retained Sail planner tests and nine Python tests pass.

All 116 observations still match the import baseline, with the same 83 successes, 18 planning errors and 15 execution errors. The 19 seeds and 18 adapter checks pass. Spark and full-Sail comparison totals remain unchanged, and the fixed inputs, queries and both checked-in baselines are untouched.

## Storage/write removal checkpoint

The approved storage/write slice, committed as `63dee61`, retained eight Sail crates and 107,168 gross Rust lines, a reduction of 6,666 lines from the catalog checkpoint. It removed MERGE/write-constraint nodes, shared catalog/data-source types, unused schema-evolution and time-travel helpers, and storage/write configuration fields. Registered tables still use the existing DeltaTableProvider and native DataFusion execution.

The removed catalog types had one remaining function-help consumer. Removing that unused consumer also removes its build script, function-name listing helpers and 24 YAML files containing 12,447 lines of help metadata. YAML is counted separately from Rust. Executable scalar, aggregate, window and table-function registries remain intact. Build-generated Rust falls from seven files / 1,251 lines to six files / 685 lines.

A direct `ReadType::DataSource` spec now returns `PlanError::NotSupported` before inspecting its format, paths, options or predicates. Previously this route could fail with a missing TableFormatRegistry extension or argument errors. A new test first reproduced the internal error, then verified rejection for Parquet, Delta, Iceberg, unknown and missing formats, with and without predicates. Another test checks CREATE/INSERT/UPDATE/DELETE/MERGE and SQL VERSION/TIMESTAMP modifiers against missing tables, so rejection must precede table lookup. Reader snapshot support remains in the host provider; SQL snapshot modifiers remain outside this frontend's current boundary.

All 116 observations match the unchanged import baseline: 83 successes, 18 planning errors and 15 execution errors. The 19 seeds, 18 adapter checks, seven runner tests, eleven Sail planner tests and nine Python tests pass. Comparisons still report 45 matches / 60 differences / 11 pending reference cases against Spark, and 83 / 22 / 11 against full Sail. Inputs, queries and both checked-in baselines are unchanged.

The resolved graph at that checkpoint remained at 599 packages across all targets and 529 Linux normal/build packages, with identical package versions. Removed direct dependency edges pointed to packages still used elsewhere.

## Service/streaming removal checkpoint

The approved service/streaming slice, committed as `e9f5911`, retained eight Sail crates and 102,630 gross Rust lines, 4,538 fewer than the storage checkpoint. It removes Sail's actor/server runtime, application configuration, telemetry, session/streaming helpers, checkpoint nodes and system-table generation. The range provider uses the existing async-trait crate directly. SQL functions, parser derives and Arrow result streaming remain intact.

Direct specs with `is_streaming: true` now return `PlanError::NotSupported` before resolving any read source. Previously the flag was silently ignored. Watermark specs also return NotSupported, replacing an unimplemented error; remote-checkpoint rejection remains explicit. A new test first reproduced the accepted streaming read, then verified rejection for all four read-source variants, watermarks and remote checkpoints. A batch read from the same registered table still executes.

The resolved graph falls by 42 packages to 557 across all targets and 487 Linux normal/build packages, with no additions or version upgrades. The dependency test rejects tonic, prost, figment and fastrace as well as the previously removed Python/catalog packages. The build succeeds with `PROTOC`, `PYO3_PYTHON` and `PYO3_CONFIG_FILE` pointing to nonexistent paths. Only parser keyword generation remains: one generated Rust file / 269 lines. Unused dependency declarations in the retained upstream workspace manifest do not enter the resolved graph.

All 116 observations match the unchanged import baseline: 83 successes, 18 planning errors and 15 execution errors. The 19 seeds, 18 adapter checks, eight runner tests, eleven Sail planner tests and nine Python tests pass. Spark and full-Sail comparison totals remain unchanged. The parser's shared gold-data test helper is retained; its separate syntax integration test was not run because Cargo rejects selecting that non-member dependency's dev-dependency test from the experiment workspace.

The following cuts continue from this checkpoint. Missing SQL extension planners, Delta/lifecycle checks and the final adoption decision remain open.

## Further reduction checkpoints

Each cut keeps the fixed inputs, SQL and both checked-in baselines unchanged. The full capture still has 83 successes, 18 planning errors and 15 execution errors, with all 116 observations matching the import baseline. The 19 seeds and 18 adapter checks pass; Spark and full-Sail comparison totals remain unchanged. Counts include comments and blank lines. Detailed counts and provenance are in `UPSTREAM.md` and each commit's `inventory.json`.

| Cut | Gross Rust lines | All-target / Linux build packages | Rust runner / planner tests | Python tests |
| --- | ---: | ---: | ---: | ---: |
| DataFrame statistics and display | 101,311 | 549 / 483 | 9 / 11 | 9 |
| Command analysis | 99,020 | 549 / 483 | 10 / 11 | 9 |
| Inline Arrow and transport helpers | 97,492 | 546 / 480 | 11 / 11 | 9 |
| DataFrame transforms and eager planning | 96,442 | 546 / 480 | 12 / 11 | 9 |
| Command spec types | 95,555 | 546 / 480 | 12 / 11 | 9 |
| Orphan dependencies and test helpers | 95,376 | 543 / 477 | 12 / 11 | 9 |
| Variant write helpers and Avro source | 94,554 | 540 / 474 | 13 / 11 | 9 |
| Connect relation and parameter scopes | 94,175 | 540 / 474 | 14 / 11 | 9 |
| DataFrame expression plumbing | 93,643 | 540 / 474 | 15 / 11 | 9 |
| Unused adapter helpers | 93,256 | 539 / 472 | 16 / 11 | 9 |
| Unused codec getters and wrapper modules | 93,094 | 539 / 472 | 16 / 11 | 9 |
| Reuse DataFusion scalar helper | 93,062 | 539 / 472 | 16 / 11 | 9 |
| Native first/last window functions | 92,879 | 539 / 472 | 17 / 11 | 9 |
| Unused protocol enum conversions | 92,699 | 537 / 465 | 17 / 11 | 9 |
| Unused spec JSON serialization | 92,593 | 537 / 465 | 17 / 11 | 9 |
| Unused AST-to-SQL text generation | 92,272 | 537 / 465 | 18 / 11 | 9 |
| Orphan protocol/storage error variants | 92,223 | 537 / 465 | 18 / 11 | 9 |
| Reuse native NTILE evaluator | 92,228 | 537 / 465 | 19 / 11 | 9 |
| Unused JSON union encoder | 92,169 | 537 / 465 | 19 / 11 | 9 |
| Unused Serde feature removal | 92,169 | 537 / 465 | 19 / 11 | 9 |
| Unused DataFusion file-source features | 92,169 | 524 / 452 | 19 / 11 | 9 |
| Remaining protocol configuration getters | 92,069 | 524 / 452 | 19 / 11 | 9 |
| Unused JSON union builder | 91,945 | 524 / 452 | 19 / 11 | 9 |

The DataFrame cut removes NA/statistics resolvers, value replacement and unused ShowString/SchemaPivot nodes. All eleven NA/statistics spec variants now reject before input resolution. The new test first reproduced missing-table lookup, then verified the rejection and successful SQL COUNT, AVG, COVAR_SAMP, CORR and COALESCE execution. Shared value formatting remains because SQL casts and PIVOT use it. No test source was removed from the vendored crates.

The command-analysis cut replaces command conversion with an explicit NotSupported error at AST dispatch. It removes 2,291 Rust lines of write/catalog/metadata command translation without changing query analysis or SQL grammar. The new test covers 23 command forms, including CTAS, views, writes, cache operations, EXPLAIN and session settings. Existing write/snapshot boundary checks now accept rejection at analysis or resolution, while still requiring NotSupported. Parsed commands therefore fail earlier; the fixed corpus still records the same planning-error stages. No vendored test source or resolved package changed.

The transport cut rejects LocalRelation specs before decoding their Arrow payloads, and removes the unused IPC import/cast/serialization helpers, placeholder-array builders, stream-renaming helper, remote/Python error envelopes, StreamUDF/DynObject and LogicalRewriter traits. The new test checks missing, empty and malformed input payloads. Registered TableProviders, derived views, SQL VALUES, literal evaluation and physical-plan output renaming remain. The cut removes 1,528 gross Rust lines, including 338 tests for deleted array helpers, and three resolved packages. The dependency test now rejects serde_arrow as well.

The upstream parser syntax test also passes from a standalone copy of the retained workspace, resolving the earlier Cargo selection limitation. The copy keeps generated lockfiles outside the vendored source. To repeat it, choose a new scratch directory:

```bash
cp -R experiments/spark-sql/vendor/sail target/spark-sql/parser-tests
cp experiments/spark-sql/Cargo.lock target/spark-sql/parser-tests/Cargo.lock
CARGO_PROFILE_TEST_DEBUG=0 CARGO_PROFILE_DEV_DEBUG=0 \
  cargo test --offline --manifest-path target/spark-sql/parser-tests/Cargo.toml \
  -p sail-sql-parser --test syntax -j 2
```

The current test always compares against the checked-in snapshot. It does not regenerate expectations. The copied parser source and gold files matched the retained source byte-for-byte after the test.

The transform cut removes DataFrame column/tail/sample/repartition/hint/metrics/parse resolvers, the unused explicit-repartition node and dynamic PIVOT value inference. Fourteen direct spec cases reject before input resolution. SQL aliases/CTEs/LIMIT, explicit PIVOT, range and derived-table TABLESAMPLE have positive execution checks. SQL TABLESAMPLE keeps the existing Bernoulli filter; its known per-batch random-state limitation remains. SQL DISTRIBUTE BY and CLUSTER BY remain unimplemented. No Sail resolver now calls execute_logical_plan to collect rows during planning; this is not a claim that arbitrary host providers perform no I/O. The cut removes 1,050 Rust lines with unchanged vendored test counts and resolved packages.

The command-spec cut removes the unused command/write/catalog/cache/display descriptors and the Plan/CommandPlan wrapper, a net 887 Rust lines. The analyzer and resolver now exchange QueryPlan directly, and named query fields are always present. Query grammar, explicit command rejection and all 23 command-form checks remain. No vendored test source or resolved package changed.

The dependency cut removes unused direct dependencies, 97 unused workspace declarations, the uncalled system-timezone/display-escaping/field-name helpers, and the generic gold-test framework, a net 179 Rust lines. The parser test now compares its complete generated syntax graph directly with the unchanged JSON snapshot; it passes, and a changed field in a temporary snapshot makes it fail. Snapshot regeneration through an environment variable is removed. The replacement test adds one test-source line; the deleted 154-line shared helper was previously counted as production source. serde_yaml, unsafe-libyaml and rand_chacha 0.10 leave both resolved graphs, with no additions or upgrades. cargo-machete reports no unused direct dependencies.

The Variant cut removes write-time shredding inference, batch rewriting, unshredding/alignment and unused storage helpers, a net 822 Rust lines including 116 tests for the removed writer. Query metadata detection, casts and Variant SQL functions remain. The new SQL test passes before and after deletion and covers extraction, nulls, JSON round trips and string casts; all 23 upstream Variant function tests also pass. Unused DataFusion serde/Avro features and direct dependencies are removed. arrow-avro, datafusion-datasource-avro and strum_macros 0.28 leave both resolved graphs with no added/upgraded packages. This probe no longer enables an Avro file source; Parquet/Delta providers, native SQL and Arrow output remain. The dependency check now rejects the removed Avro packages.

The Connect-scope cut removes reference-plan lookup, DataFrame alias registration, query-parameter substitution and their state scopes, plus the unused positional-marker rewriter, a net 379 Rust lines. Nine direct spec cases now reject with NotSupported before resolving missing inputs or references. Parameter binding remains outside the first Python SQL API. Positive checks preserve scalar subqueries, WHERE EXISTS, multi-column IN/NOT IN, CTEs and constant IDENTIFIER expressions. The shared multi-column IN resolver remains because SQL calls it too. A check before deletion exposed an existing limitation: projecting EXISTS as a SELECT output fails physical planning; filtering with EXISTS works. This is recorded as a compatibility gap, not repaired by the cut. No vendored tests or resolved packages changed.

The DataFrame-expression cut removes Connect plan-ID propagation, regex column selection, struct update/drop implementations and the unused external-schema expression rewriter, a net 532 Rust lines. Seven direct spec cases now reject with NotSupported; internal SQL field IDs, hidden fields and metadata remain. Positive checks execute USING/ON joins, qualified fields, struct wildcard expansion, array indexing and map extraction before and after deletion. A pre-deletion check also identified an existing gap: selecting l.a after JOIN ... USING (a) fails attribute resolution, while the merged a and ON-join forms work. SQL query functions and vendored tests remain; resolved packages are unchanged.

The adapter-helper cut removes unused parse convenience entrypoints, schema/config wrappers, codec getters, format setters and the legacy HEX helper, a net 387 Rust lines. PlanConfig callers use its existing Default implementation. Range execution keeps projection and batching; only unused schema storage/getters leave. SQL HEX continues to use DataFusion Spark's registered implementation, and UNHEX remains; a before/after SQL test checks integer/string HEX, odd-length UNHEX and invalid input. Full upstream suites pass: 301 function tests, 6 common-DataFusion tests, 6 analyzer tests and the unchanged parser syntax snapshot. No vendored test source is removed. Tokio uses only its needed features, and signal-hook-registry leaves the all-target graph; Linux build packages fall by two. No resolved package is added/upgraded, and the normal build has no compiler warnings. Tokio stays a normal Sail-plan dependency so Cargo can run its unit tests from the outer probe workspace.

This cut removes 17 uncalled inherent getters, the unused DebugBinary helper and the unregistered SparkTryToTimestamp wrapper, a net 162 Rust lines. The registered try_to_timestamp path still uses SparkTimestamp; the before/after SQL check covers invalid input and leap-day parsing. All 301 retained function tests pass, with no vendored test deletion or dependency change. Function settings, signatures and execution fields remain. TableInput.plan is retained because it lets a host table function consume a SQL TABLE argument, even though this repository has no such caller.

The scalar-helper checkpoint replaces Sail's copied scalar-expansion implementation with the existing DataFusion helper, keeping the Arc-returning adapter and argument hints. This removes 32 vendored Rust lines without changing dependencies or vendored tests. All 116 baseline cases still match; 16 runner, 11 planner and 9 Python tests pass, along with 301 function, 6 common-DataFusion and 6 analyzer unit tests. Same-name SQL functions were also examined but retained: Soundex constructs Utf8 output even for LargeUtf8 in DataFusion 54.1.0, make_valid_utf8 differs in nullability and FixedSizeBinary support, URL parsing differs in decoding and relative-URL behavior, and regression aggregates intentionally preserve Spark floating-point operation order. Those implementations cannot be replaced solely by matching their names.

The native-window checkpoint removes Sail's first_value/last_value evaluator, which encoded IGNORE NULLS in function state to work around Protobuf plan transport. This frontend passes native DataFusion plans directly, so it now uses the existing DataFusion window functions with null_treatment preserved. The window rewriter still recognizes first/last functions through DataFusion's NthValue type and retains its input-order handling. A regression matrix passed before and after replacement: ten first/last call forms, five ROWS frames, both order directions, and batch sizes 1, 3 and 1024, with all-NULL and single-row partitions, empty frames, values, names, types and nullability checked. This removes 183 vendored Rust lines and one file, with no dependency or vendored-test changes. All 116 baseline cases, 17 runner tests, 11 planner tests and 9 Python tests pass; the Spark/full-Sail comparison totals remain unchanged. NTILE keeps its Spark-specific bucket allocation.

The protocol-enum checkpoint removes unused integer conversions generated for seven spec enums and two Connect-only interval-field enums. SQL analysis constructs the retained enum variants directly; the deleted conversions belonged to upstream Spark Connect protocol decoding. Python rejection payloads and the SQL interval/geography/union types remain. Removing num_enum and num_enum_derive drops the all-target package count from 539 to 537 and the Linux normal/build count from 472 to 465; proc-macro-crate, toml_datetime 1.1, toml_edit 0.25, toml_parser 1.1 and winnow 1.0 also leave the Linux build. No package is added or upgraded. The dependency guard now rejects num_enum packages. This removes 180 vendored Rust lines without changing vendored tests. All 116 baseline cases, 17 runner tests, 11 planner tests and 9 Python tests pass, with unchanged reference comparison totals.

The spec-serialization checkpoint removes JSON serialization/deserialization for in-process query, expression, literal and data-type specs, including decimal string codecs and half's unused serde feature. No runtime caller serializes those specs. Two test matrices now construct the same typed Rust inputs directly instead of passing them through JSON; all 24 rejection cases remain. Spark UDT and GeoArrow metadata serialization, SQL JSON/CSV/XML functions, syntax snapshot serialization and Arrow transport remain. This removes 106 vendored Rust lines; generated Serde implementations are not included in that count. The package sets are unchanged, and the lockfile only removes half's serde dependency edge. All 116 baseline cases, 17 runner tests, 11 planner tests and 9 Python tests pass, as do 301 function, 6 common-DataFusion and 6 analyzer tests. Reference comparison totals and vendored test source are unchanged.

The SQL-unparse checkpoint removes TreeText, its derive macro, AST/container text implementations and the unused QualifiedWildcard wrapper. No runtime path consumes reconstructed SQL text. StringLiteral no longer copies raw tokens or rewinds the parser just to retain printable spelling; decoded values and spans remain. The four former unparse inputs still have a parser acceptance test, while a new SQL execution test passed before and after removal for comments, precedence, raw/escaped strings, Unicode escapes, binary literals, adjacent strings, numeric suffixes and invalid escapes. Assertions about the removed printer's output spacing are gone. TreeParser, TreeSyntax, keyword generation and the complete unchanged syntax snapshot remain; the standalone syntax test and all six analyzer tests pass. This removes 321 vendored Rust lines (315 production and six test lines) and two files, with unchanged dependencies. All 116 baseline cases, 18 runner tests, 11 planner tests and 9 Python tests pass, with unchanged Spark/full-Sail comparisons.

The orphan-error checkpoint removes seven error variants and five constructors left without producers after the protocol and storage cuts, plus their conversion match arms. DataFusion/Arrow errors, active SQL validation errors and unsupported-operation errors remain; Delta provider failures still propagate through DataFusion. A stale Python-UDF configuration comment is also removed without changing large-variable-type behavior. This removes 49 vendored Rust lines with unchanged dependencies and test source. All 116 baseline cases, 18 runner tests, 11 planner tests and 9 Python tests pass, and reference comparison totals remain unchanged. Error-stage comparisons still do not prove equality of underlying error conditions.

The native-NTILE checkpoint delegates bucket allocation to DataFusion 54.1.0, whose evaluator already places remainder rows in the first buckets. Sail's comment described the older DataFusion algorithm. A small wrapper keeps Sail's exact parameter validation, including rejecting UInt64 values above i64::MAX; the SQL window builder still returns non-null Int32. Before/after tests cover 90 SQL combinations of row counts, bucket counts and batch sizes, plus all eight integer input types, the upper bound, zero, negative, NULL, floating-point and missing arguments. This removes 85 production Rust lines and adds 90 vendored test lines, so the gross count increases by five to 92,228; test code is not hidden from the inventory. No dependency changes. All 116 baseline cases, 19 runner tests, 11 planner tests, 9 Python tests and the new NTILE unit test pass, with unchanged reference totals. Other same-name functions remain where interval handling, ANSI configuration, errors or nullability differ.

The JSON-encoder checkpoint removes JsonUnionEncoder and JsonUnionValue, which had no runtime consumer in either the extracted source or pinned upstream crates. The JSON union builder and SQL JSON functions remain. Its test now checks the same values using native Arrow/DataFusion access plus explicit type-ID assertions, preserving the distinction between strings, arrays and objects; it passed before and after encoder removal. This removes 65 production Rust lines and adds six test lines, for a net reduction of 59 gross lines. All 24 JSON unit tests, 19 runner tests, 11 planner tests, 9 Python tests and 116 baseline cases pass. Dependencies and reference comparison totals are unchanged.

The workspace no longer requests Serde support from Arrow schemas, ordered floats or Chrono after removal of the protocol JSON codecs. Resolved Arrow schema, ordered-float, rand and rand_core Serde features disappear; Chrono Serde remains enabled by another dependency. SQL JSON functions, Arrow metadata codecs and parser snapshots retain their existing behavior. No Rust source lines or dependency package versions change. The runner/planner/Python checks pass, as do all 302 function, 6 common DataFusion and 6 analyzer unit tests; the frozen 116-case comparison remains unchanged.

The probe and Sail workspace no longer enable DataFusion parquet or compression features. Registered tables still scan through the existing DeltaTableProvider and its Parquet reader; SQL functions, native DataFusion SQL, Variant handling and Arrow IPC output remain. DataFusion's extra Parquet source and compressed-file transport leave the resolved graph, removing 13 packages from both counts: 524 all-target and 452 Linux normal/build packages remain. The reader's own Parquet/compression dependencies are retained, with no added or upgraded package. The dependency guard rejects the removed DataFusion Parquet source and async-compression packages. All 116 baseline observations, 19 runner tests, 11 planner tests, 9 Python tests, 302 function tests, 6 common DataFusion tests and 6 analyzer tests pass. Rust source counts and reference comparison totals are unchanged.

A type-by-type caller audit removes 30 more unused protocol getters across 21 function files. Several shared method names had hidden them from a simple symbol-count search; the remaining sequence and timezone calls were only forwarding between unused getters. ANSI, timezone, null-short-circuit and safe-mode fields still drive the same SQL implementations directly. TimestampNow's active getters, aggregate moment accessors, Explode.kind and the host-facing TableInput.plan accessor remain. This removes 120 source lines and adds 20 required modification notices, a net reduction of 100 gross/production Rust lines; no tests or dependencies are removed. All 116 baseline observations, 19 runner tests, 11 planner tests, 9 Python tests, 302 function tests, 6 common DataFusion tests and 6 analyzer tests pass. Reference comparison totals remain unchanged.

The follow-up JSON audit removes JsonUnion, JsonUnionField and their builder/scalar-conversion implementations. Their only value-construction consumer was the unit test; runtime SQL still uses the separate union type and input-reading helpers. The test constructs the same sparse union with native Arrow/DataFusion APIs and additionally checks array/object extraction and scalar tags. Before deletion, its native fixture was verified equal to the old builder's complete Arrow ArrayData. This removes 146 production lines and adds 22 test lines, for a net reduction of 124 gross Rust lines; no dependency changes. All 116 baseline observations, 19 runner tests, 11 planner tests, 9 Python tests, 302 function tests, 6 common DataFusion tests and 6 analyzer tests pass. The public JSON union type, registered-table inputs and reference comparison totals remain unchanged.
