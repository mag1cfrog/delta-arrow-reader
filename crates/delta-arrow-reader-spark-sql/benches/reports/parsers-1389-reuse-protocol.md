# STRING parser construction protocol (#1389, PR 2)

This reference covers the single construction change after the merged
[PR 1 baseline](parsers-1389.md). Product baseline is staging
`be3c663a8380d5094cb52003f351d5b9a5d48383`. The change reuses existing parsers
within an array invocation and removes owned cycles from the interval-only
expression factory. It introduces no grammar, timestamp fast path, unsafe code,
dependency fork, SQL statement-parser change or explicit input-format change.

Review and commit the code and protocol before release preflight. Commit the
complete successful receipt and verify that the exact rebuild produces the
same executable hash before timing.

## Construction boundaries and retained behavior

| Path | Construction after this change |
| --- | --- |
| SparkDate array fallback | The first fallback row builds the parser. Exact ASCII dates use Arrow directly. |
| SparkTimestamp LTZ/NTZ arrays | Existing timestamp grammar built lazily once for the unformatted invocation |
| SparkDateFormat STRING arrays | Same timestamp grammar reused across that invocation before the existing display formatting |
| Three interval arrays | Existing lexer and token parser built lazily once for the invocation, with default ParserOptions |
| Scalar paths | The single-value path builds at most one ordinary parser per scalar invocation. NULLs skip parsing. |
| Decoded quoted interval strings | Original scoped reparse for each decoded layer, described below |

The date/timestamp trimming, grammar and Rich error formatter are unchanged.
The interval factory consumes the same lexer output through an owning Chumsky
Stream, preserving source-byte spans and end-of-input position while removing
the borrow of a temporary row-local token vector. The private expression factory
is used only by interval_literal. Its same Expr/Query/DataType/TableRelation
combinators and branch ordering stay in place. Nested `recursive()`
constructors give the graph weak references. Ordinary SQL statement and schema factories keep
their existing construction.

The named residual path is
`from_ast_signed_interval_with_rule -> IntervalExpr::Literal ->
parse_interval_string -> parse_interval_literal -> parse!`. Unescaping a quoted
literal produces a temporary String whose lifetime is shorter than the batch.
It retains the exact scoped path rather than extending a borrow or changing the
grammar. A focused test counts zero scoped calls for ordinary `2 months`, one
for a quoted `2 months`, and two more for a doubly quoted value. That path can
still construct a parser for each decoded layer. The ownership repair also
applies to these temporary graphs. Report it explicitly rather than claiming every possible
interval form shares one parser. It is not separately timed in this protocol.

The [input lists](../../vendor/sail/crates/sail-sql-analyzer/tests/fixtures/string-parser-1389-inputs.rs)
compare reused entry points with original per-call APIs and a
[frozen original interval factory](../../vendor/sail/crates/sail-sql-analyzer/tests/fixtures/interval-parser-1389-original.rs).
Check exact values, recursive ASTs, error variants/messages and byte positions,
including lexer failures, parenthesized/arithmetic, CAST, CASE and subquery
interval operands, nested quoted strings, interval families/limits, date
fallbacks, fractions, offsets, zone names, DST transitions and leap seconds.
The [kernel and connected SQL tests](../../tests/parser_reuse.rs) compare with
[frozen unformatted conversion bodies](../../tests/fixtures/row-parser-1389-original.rs)
over scalar/array, empty/NULL and Utf8/LargeUtf8/Utf8View forms, SQL dictionaries,
UTC/New_York, CAST/TRY_CAST and both ANSI modes. Require identical actual types,
values, NULLs, failures and SQL failure phases. Keep every old test and capture
unchanged, including pruning, folding and effect checks. Record the change in
[UPSTREAM.md](../../UPSTREAM.md).

## Frozen workload, controller and evidence

Reuse the exact PR 1 [manifest](../parsers/queries.json),
[schedule](../parsers/schedule.json), common native ViewTable and retained SF1
fixture. There is no new manifest or reduced timing schedule. DATE, LTZ/NTZ and
date_format retain 6,001,215 rows. All three intervals and the paired input
projection retain the predicate `l_orderkey BETWEEN 1 AND 8192`: exactly 8,184
rows, including 90 NULL inputs. The original count proof, full preflight,
receipts, samples, reports and all 86 historical assets remain unchanged.

The Python [controller](../parsers/run.py) adds only `--reuse-row-parsers` to
select PR 2 evidence names and the separate `reuse-preflight-receipt.json`.
Default behavior retains the PR 1 paths. PR 1's actual timed controller identity
remains pinned to commit `997278f7f73ad53b050f3d4b4ca381dd5eaf5e34`.
The Rust selector, harness and embedded workload are unchanged. Its emitted
source_commit `298015f9def26cc97a271bdbc0316e64083d7874` identifies the workload
origin. The controller also records the current product source, HEAD, product
baseline, binary and protocol hashes.

Keep new evidence under
`/home/hanbo/.local/share/delta-arrow-reader-review/costs-168-2026-11-19/1389/pr2`.
The genuine SF1 snapshot stays at
`/home/hanbo/.local/share/delta-arrow-reader-review/measurements-2026-11-18/1380/delta-snapshot`.
Verify the retained base manifest, both Delta logs and every Parquet size and
hash before reusing the fixture. Keep the 64 GiB fixture/export/scratch allowance.

## Build, correctness and untimed allocation check

Serialize all Cargo/query work on local Linux. From the Spark workspace:

```sh
CARGO_TARGET_DIR=/home/hanbo/.cache/delta-arrow-reader/costs-168-2026-11-19 \
CARGO_BUILD_JOBS=2 CARGO_PROFILE_RELEASE_DEBUG=1 \
cargo +1.96.0 build --locked --release --bench execution \
  --message-format=json-render-diagnostics
```

Keep opt3/debug1 and no RUSTFLAGS, CPU, LTO, codegen or CLI overrides. Record
command/CWD, toolchain, locked source/config hashes, compiler artifact/profile,
source objects and executable SHA-256, with an independent read-only seal.
Run focused checks, the full Linux regression gates and clean-context review.
Bind every old named outcome/capture rather than accepting equal totals alone.

After compilation, reuse PR 1's separately specified untimed allocation probe
with the exact candidate release analyzer, retaining no parse results. Use
separate date/interval processes, affinity 2-5, a 2 GiB address cap and 60-second
process limits. Record VmSize/RSS/HWM at 0,128,256,512,1024,2048,4096 calls,
with original `1996-2-9` and `2 months` inputs. Preserve the old logs. Label the
new source/artifact and keep outputs under the PR 2 evidence directory. This
measures whether retained memory grows across parser calls. RSS records
process memory alongside the virtual size and high-water mark.

From the repository root, run the new untimed preflight once:

```sh
python3 crates/delta-arrow-reader-spark-sql/benches/parsers/run.py --reuse-row-parsers --check
python3 crates/delta-arrow-reader-spark-sql/benches/parsers/run.py --reuse-row-parsers preflight BINARY /home/hanbo/.local/share/delta-arrow-reader-review/measurements-2026-11-18/1380/delta-snapshot /home/hanbo/.local/share/delta-arrow-reader-review/costs-168-2026-11-19/1389/pr2/preflight
```

Export all 22 complete results and perform the same 14 native/off/on comparisons
as PR 1. Preserve the distinction between semantic native-pair exclusions and
operational failures, which stop preflight. Then verify the hashes of all 22
retained PR 1 IPC outputs and fully decode each old/new pair, including both
Spark-only day-time and projection modes. Require identical values, NULLs,
multiplicity, exact row counts, executed SQL types, actual Arrow storage/unit/
zone types and exact source I/O. Any old/new difference stops the change before
timing, regardless of native-pair eligibility. Never infer equality from hashes
or row totals. Retain raw plans, diffs, counters, read ranges and all proofs.

One shared 1,800-second deadline covers exports, native/mode comparisons and all
22 old/new pairs. Each export keeps its 300-second limit, and each comparison
keeps its 180-second limit.
Write new full evidence to `parsers-1389-reuse-preflight.json.gz` and the separate
bound receipt. Bind the old full preflight/receipt, new controller/protocol,
tests/provenance, source objects, fixture and executable. Commit and independently
verify that the exact rebuild matches the preflight binary before timing.

## Fixed after collection and decision

Use the unchanged 126-process schedule: 60 same-binary A/B noise processes, then
66 main processes, each with one warmup and three retained iterations. Keep
affinity 2,3,4,5, two runtime workers/partitions, batch 8192, 2/4 GiB measured/
preflight pools, 12/16 GiB address caps, 8 GiB spill, 180 seconds per measured
process and the 3,600-second timing bound. There are no pilots, profiles,
concurrency cases or unchanged timing reruns.

After verifying the committed receipt and executable hash, run the fixed
schedule once and summarize its samples.

```sh
python3 crates/delta-arrow-reader-spark-sql/benches/parsers/run.py --reuse-row-parsers measure BINARY /home/hanbo/.local/share/delta-arrow-reader-review/measurements-2026-11-18/1380/delta-snapshot /home/hanbo/.local/share/delta-arrow-reader-review/costs-168-2026-11-19/1389/pr2/after-once
python3 crates/delta-arrow-reader-spark-sql/benches/parsers/run.py --reuse-row-parsers summarize /home/hanbo/.local/share/delta-arrow-reader-review/costs-168-2026-11-19/1389/pr2/after-once /home/hanbo/.local/share/delta-arrow-reader-review/costs-168-2026-11-19/1389/pr2/after-summary
```

Validate every sample's rows, partitions, completed counters and exact source
traffic against preflight. Retain median-of-process-medians, minimum/maximum/IQR,
ms and ns per returned row. Each native-reference flag is strictly
`spark - native > native A/B noise`, separately per ANSI mode. Day-time reports
the difference from the same-mode input projection, with A/B noise for that
difference. Keep planning/execution/first-batch and RSS/pool/source
metrics separate from the completion decision.

Report all seven before/after shapes, both modes, native or projection references,
spreads, noise, exclusions and the named quoted-string residual construction.
Compare the original PR 1 measurements with the new campaign and report native
drift. The interval conclusions apply to the fixed subset. If an ordinary
timestamp gap remains, evaluate the separate timestamp stage permitted by
#1389. Record any other residuals and their supported causes in the open issue.
