# Sail source provenance

`vendor/sail/` contains a subset of [Sail v0.7.1](https://github.com/lakehq/sail/tree/v0.7.1), commit `9544c9253e981a82c5f9e493c43ce98a4d9d41b7`, under its Apache-2.0 [LICENSE](vendor/sail/LICENSE). That upstream revision has no root NOTICE file. Existing source notices remain intact. Modified files carry a header pointing here.

The import retains the upstream Cargo.toml, README.md, LICENSE and these 10 crate directories:

```text
sail-catalog             sail-catalog-memory
sail-common              sail-common-datafusion
sail-function            sail-logical-plan
sail-plan                sail-sql-analyzer
sail-sql-macro           sail-sql-parser
```

`upstream.patch` records the changes within that selection: 288 added lines and 9,929 deleted lines, including 29 deleted files. It also adds `sail-plan/src/function/table/range_exec.rs`, copied from upstream `sail-physical-plan/src/range.rs`. The 145 source lines are unchanged apart from the modification notice. Crates outside the selection are omitted, rather than represented as deletions in the patch.

The adaptation changes table lookup to the native DataFusion registry, rejects table modifiers, restores output names, and removes command/streaming/explain and data-source/cache/physical-plan dependencies. It also handles DataFusion's SQL error variant and SQL-enabled wildcard AST types. Runtime function implementations are retained. Python UDF execution/configuration and its two crates are removed. Catalog coupling remains.

To reproduce the import, copy the three root files and the 10 directories above from the pinned upstream commit into a temporary Git checkout, then apply `upstream.patch` with `git apply`. An archive of that selection plus the patch was checked against all 502 vendored files byte-for-byte. Normal builds use the committed files directly; they need no upstream checkout.

## Source measurements

`inventory.json` records current per-crate counts and a hash of all vendored files. The import baseline was committed as `fc402c8`. The Python removal checkpoint changes these measurements:

| Measurement | Import baseline | After Python removal |
| --- | ---: | ---: |
| Sail crates | 12 | 10 |
| Rust files | 489 | 460 |
| Gross Rust lines | 126,013 | 120,537 |
| Nonblank Rust lines | 115,841 | 110,803 |
| Production source lines | 114,700 | 109,224 |
| Test source lines | 10,528 | 10,528 |
| Build-script source lines | 785 | 785 |
| All-target resolved packages | 618 | 610 |
| Linux normal/build packages | 548 | 540 |

Gross/production/test/build-script counts include comments and blank lines. The test count includes files under `tests/` and formatted `#[cfg(test)]` modules/constants. The counter rejects unrecognized test-item shapes; it uses upstream indentation to identify module boundaries. Production count means source outside those test sections and build scripts, not live code reached by this corpus. Unused functions still count.

The tested build generated seven Rust files totaling 1,251 lines: 416 from sail-common-datafusion, 269 from sail-sql-parser and 566 from sail-plan. These are separate from checked-in source and unchanged by Python removal. Procedural macro expansion was not measured; the macro source remains counted. No expanded code was substituted to reduce the source count.

Dependency counts include the runner and reader, rather than only dependencies added by Sail. The all-target graph includes development dependencies. Neither resolved graph contains sail-python-udf, sail-pyarrow or PyO3. `upstream.patch` describes changes within the ten retained crates; the two omitted Python crates are excluded from its selection. Their removal remains visible in the repository diff against the import commit.

The checkpoint was built with Rust 1.97.1, DataFusion 54.1.0 and Arrow 58.4.0. Sail declares Rust 1.96.0; the reader's lower MSRV is unchanged. Builds reused a local Cargo cache, so no cold-build timing claim is made. The result is a demonstrated subset, not a minimum.
