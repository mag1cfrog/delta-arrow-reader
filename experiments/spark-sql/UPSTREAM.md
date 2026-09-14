# Sail source provenance

`vendor/sail/` contains a subset of [Sail v0.7.1](https://github.com/lakehq/sail/tree/v0.7.1), commit `9544c9253e981a82c5f9e493c43ce98a4d9d41b7`, under its Apache-2.0 [LICENSE](vendor/sail/LICENSE). That upstream revision has no root NOTICE file. Existing source notices remain intact. Modified files carry a header pointing here.

The import retains the upstream Cargo.toml, README.md, LICENSE and these 12 crate directories:

```text
sail-catalog             sail-catalog-memory
sail-common              sail-common-datafusion
sail-function            sail-logical-plan
sail-plan                sail-pyarrow
sail-python-udf          sail-sql-analyzer
sail-sql-macro           sail-sql-parser
```

`upstream.patch` records the changes within that selection: 192 added lines and 7,985 deleted lines, including 24 deleted files. It also adds `sail-plan/src/function/table/range_exec.rs`, copied from upstream `sail-physical-plan/src/range.rs`. The 145 source lines are unchanged apart from the modification notice. Crates outside the selection are omitted, rather than represented as deletions in the patch.

The adaptation changes table lookup to the native DataFusion registry, rejects table modifiers, restores output names, and removes command/streaming/explain and data-source/cache/physical-plan dependencies. It also handles DataFusion's SQL error variant and SQL-enabled wildcard AST types. Runtime function implementations are retained. Python UDF and catalog coupling are still present.

To reproduce the import, copy the three root files and the 12 directories above from the pinned upstream commit into a temporary Git checkout, then apply `upstream.patch` with `git apply`. An archive of that selection plus the patch was checked against all 534 vendored files byte-for-byte. Normal builds use the committed files directly; they need no upstream checkout.

## Import measurements

`inventory.json` records per-crate counts and a hash of all vendored files. The imported source has 489 Rust files and 126,013 gross lines, including 115,841 nonblank lines. Of the gross lines, 10,528 are test source, 785 are build scripts and 114,700 remain in production source. These counts include comments and blank lines. The ten-line increase over the earlier 126,003-line subset consists of modification notices in Rust files.

The test count includes files under `tests/` and formatted `#[cfg(test)]` modules/constants. The counter rejects unrecognized test-item shapes; it uses upstream indentation to identify module boundaries. Production count means source outside those test sections and build scripts, not live code reached by this corpus. Unused functions still count.

The tested build generated seven Rust files totaling 1,251 lines: 416 from sail-common-datafusion, 269 from sail-sql-parser and 566 from sail-plan. These are separate from checked-in source. Procedural macro expansion was not measured; the macro source remains counted. No expanded code was substituted to reduce the source count.

The experiment lockfile resolves 618 packages across all targets, including development dependencies. Its Linux normal/build graph contains 548 packages, including the runner and reader. It still contains sail-python-udf, sail-pyarrow and PyO3 0.29.2. These are whole-experiment counts, not counts of new dependencies added to the reader.

The import was built with Rust 1.97.1, DataFusion 54.1.0 and Arrow 58.4.0. Sail declares Rust 1.96.0; the reader's lower MSRV is unchanged. Builds reused a local Cargo cache, so no cold-build timing claim is made. This checkpoint establishes the baseline before removing Python UDF dependencies.
