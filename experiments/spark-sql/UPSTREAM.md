# Sail source provenance

`vendor/sail/` contains a subset of [Sail v0.7.1](https://github.com/lakehq/sail/tree/v0.7.1), commit `9544c9253e981a82c5f9e493c43ce98a4d9d41b7`, under its Apache-2.0 [LICENSE](vendor/sail/LICENSE). That upstream revision has no root NOTICE file. Existing source notices remain intact. Modified files carry a header pointing here.

The import retains the upstream Cargo.toml, README.md, LICENSE and these 8 crate directories:

```text
sail-common              sail-common-datafusion
sail-function            sail-logical-plan
sail-plan                sail-sql-analyzer
sail-sql-macro           sail-sql-parser
```

`upstream.patch` records the changes within that selection: 431 added lines and 35,608 deleted lines, including 118 deleted files. It also adds `sail-plan/src/function/table/range_exec.rs`, copied from upstream `sail-physical-plan/src/range.rs`. The 145 source lines are unchanged apart from the modification notice. Crates outside the selection are omitted, rather than represented as deletions in the patch.

The adaptation changes table lookup to the native DataFusion registry, rejects table modifiers, restores output names, and removes command/streaming/explain and data-source/cache/physical-plan dependencies. It also handles DataFusion's SQL error variant and SQL-enabled wildcard AST types. Runtime function implementations are retained. Python UDF execution/configuration and its two crates are removed. The two catalog crates, catalog command/display types and PlanService are also removed; SparkPlanFormatter remains as a direct utility. Spark builtin functions take precedence over the native registry. Session catalog/schema functions read the native session configuration. Shared catalog/data-source/write helpers, unused schema-evolution/time-travel logic and function-help generation are removed. Sail's actor/server runtime, application configuration, telemetry, session/streaming/checkpoint modules and system-table/protobuf generation are removed. Streaming read and watermark specs are rejected explicitly. Parser keyword generation, parser test helpers and Arrow result streaming remain.

To reproduce the import, copy the three root files and the 8 directories above from the pinned upstream commit into a temporary Git checkout, then apply `upstream.patch` with `git apply`. An archive of that selection plus the patch was checked against all 388 vendored files byte-for-byte. Normal builds use the committed files directly; they need no upstream checkout.

## Source measurements

`inventory.json` records current per-crate counts and a hash of all vendored files. The import baseline was committed as `fc402c8`. The approved Python removal slice was committed as `4674528`, catalog/session removal as `4987c12`, and storage/write removal as `63dee61`. The current service/streaming removal slice has these measurements:

| Measurement | Import baseline | After Python removal | After catalog removal | After storage removal | After service removal |
| --- | ---: | ---: | ---: | ---: | ---: |
| Sail crates | 12 | 10 | 8 | 8 | 8 |
| Rust files | 489 | 460 | 434 | 419 | 375 |
| Gross Rust lines | 126,013 | 120,537 | 113,834 | 107,168 | 102,630 |
| Nonblank Rust lines | 115,841 | 110,803 | 104,657 | 98,530 | 94,522 |
| Production source lines | 114,700 | 109,224 | 103,767 | 98,009 | 94,281 |
| Test source lines | 10,528 | 10,528 | 9,282 | 8,612 | 8,269 |
| Build-script source lines | 785 | 785 | 785 | 547 | 80 |
| All-target resolved packages | 618 | 610 | 599 | 599 | 557 |
| Linux normal/build packages | 548 | 540 | 529 | 529 | 487 |

Gross/production/test/build-script counts include comments and blank lines. The test count includes files under `tests/` and formatted `#[cfg(test)]` modules/constants. The counter rejects unrecognized test-item shapes; it uses upstream indentation to identify module boundaries. Production count means source outside those test sections and build scripts, not live code reached by this corpus. Unused functions still count.

The tested build generated one Rust file / 269 lines from sail-sql-parser. This cut removes sail-common-datafusion's five generated files / 416 lines and its 467-line build script. It also removes two application/system YAML files / 1,295 lines and one protobuf file / 31 lines, counted separately from Rust. The previous storage cut removed the help-metadata generator's one generated file / 566 lines and 24 YAML files / 12,447 lines. Procedural macro expansion was not measured; macro source remains counted.

Test-source reduction in this cut is 343 lines belonging to removed helpers: 148 actor, 21 application-config, 127 catalog-config and 47 streaming-marker lines. The previous storage cut removed 670 test lines. The frozen corpus and its two checked-in baselines remain unchanged; eight runner tests, eleven Sail planner tests and nine Python tests pass. The README records the separate parser integration-test limitation.

Dependency counts include the runner and reader, rather than only dependencies added by Sail. The all-target graph includes development dependencies. Neither resolved graph contains sail-python-udf, sail-pyarrow, PyO3, sail-catalog, sail-catalog-memory, tonic/prost packages, figment or fastrace. This cut removes 42 resolved packages without adding or upgrading any. `upstream.patch` describes changes within the eight retained crates; the four omitted Python/catalog crates are excluded from its selection. Their removal remains visible in the repository diff against the import commit.

The checkpoint was built with Rust 1.97.1, DataFusion 54.1.0 and Arrow 58.4.0, with `PROTOC`, `PYO3_PYTHON` and `PYO3_CONFIG_FILE` set to nonexistent paths. Sail declares Rust 1.96.0; the reader's lower MSRV is unchanged. Builds reused a local Cargo cache, so no cold-build timing claim is made. The debug-profile runner with debug information disabled is 435,537,800 bytes; this is a host binary measurement, not a wheel-size estimate or release-build measurement. The result is a demonstrated subset, not a minimum.
