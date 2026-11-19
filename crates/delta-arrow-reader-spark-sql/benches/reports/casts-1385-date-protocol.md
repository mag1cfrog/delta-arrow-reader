# DATE row path measurement protocol (#1385, PR 2)

This stage makes one reviewed DATE change after attribution PR #1386. Its source
base is staging merge `0b6ff0fd0a89a78dee8b0ae7419ee4b4ebfe745f`. Commit the product
change, differential inputs/tests, this protocol, stage manifests, harness changes
and successful bound preflight receipt before timing. The coordinator owns that
commit, exact-source rebuild, clean review and explicit timing authorization.
No prototype or microbenchmark timing is part of this stage.

## Product scope and semantic proof

Only `SparkDate::string_to_date32` changes execution. Exact ten-byte ASCII
`YYYY-MM-DD` values may use Arrow's existing `Date32Type::parse`; that subset uses
the same Chrono calendar validation and epoch-day conversion as the original
parser. The guard is essential because Arrow admits other forms, such as compact
`YYYYMMDD`, that Spark's existing parser rejects. An unsuccessful native attempt
and every unhandled form fall through to the original `parse_date` call and
unchanged NULL/error formatter. Formatted dates, shared analyzer parsing,
TIMESTAMP, DECIMAL, planning and dependency-fork sources stay unchanged.

The committed [differential corpus](../../vendor/sail/crates/sail-function/tests/fixtures/date-cast-1385-inputs.json)
has 102 inputs in 13 categories. The unit test invokes the new row path directly
and compares it with `sail_sql_analyzer::parser::parse_date`, bypassing the new
branch in the oracle. It checks 204 value/NULL/failure outcomes across strict and
null-on-error modes. Inputs cover complete, partial, one-digit, signed and long
years; spaces and controls; space/T suffixes; invalid calendars and leap years;
empty/malformed text; non-ASCII digits; native-only compact dates and trailing
junk. [Caller tests](../../tests/date_cast_row_path.rs) cover CAST/TRY_CAST in
both ANSI modes, scalar and array calls, NULLs, Utf8, LargeUtf8, Utf8View and
dictionary storage. All existing named outcomes must retain their prior result.
`UPSTREAM.md` documents the repair without rewriting accepted source hashes.

## Immutable before evidence and fresh preflight

Preserve every original #1380 and PR 1 manifest, schedule, receipt, report and
compressed evidence asset. Historical timing reproduction requires its pinned
source because receipts bind the harness hash. PR 1's matched current before
numbers in [the baseline report](casts-1385.md), rather than historical #1380
numbers, are the before reference. Its read-only, independent-inode binary copy
remains sealed outside Git.

The [stage query manifest](../casts/date/queries.json) retains all three exact
cast shapes and the original common view/session setup. The
[stage schedule](../casts/date/schedule.json) times only `cast_strings` and
`cast_date`. `cast_decimal` remains in fresh preflight only; it is not retimed.
The explicit `--casts-1385-date` selector chooses these stage files in the shared
harness and controllers. Inventory and binary guards reject DECIMAL timing and
concurrency. The existing profile entry is reserved for a conditional final
above-line diagnostic, not this measurement schedule.

Reuse the retained SF1 snapshot from #1380, after verifying its recorded source
manifest/log and Parquet hashes. Do not regenerate valid data. It has 6,001,215
rows and six unchanged Parquet objects. Fresh preflight performs nine exports,
six native/Spark decoded comparisons and nine decoded baseline/current
comparisons using retained baseline IPC exports. Require full value, NULL,
multiplicity and executed SQL-type equality with the original normalization
rules. Also require identical exact source path sets and delivered/range byte
totals against both the current native reference and the corresponding PR 1
baseline record. Every recorded range sum must equal received bytes, with final
source counters. Stop on any mismatch and preserve evidence; do not change the
workload or product behavior to obtain eligibility.

Use new stage evidence destinations under:

```text
/home/hanbo/.local/share/delta-arrow-reader-review/casts-1385-2026-11-19/date-stage
```

Reuse only the owned cache and serialize all Cargo/query work with two jobs.
From the Spark workspace, build with the unchanged release settings:

```sh
CARGO_TARGET_DIR=/home/hanbo/.cache/delta-arrow-reader/casts-1385-2026-11-19 \
CARGO_BUILD_JOBS=2 CARGO_PROFILE_RELEASE_DEBUG=1 \
cargo +1.96.0 build --locked --release --bench execution \
  --message-format=json-render-diagnostics
```

Resolve `BINARY` from the compiler-artifact JSON, retain its SHA-256 and exact
source/build receipt, then run from the repository root:

```sh
python3 crates/delta-arrow-reader-spark-sql/benches/execution/run.py --casts-1385-date --check
python3 crates/delta-arrow-reader-spark-sql/benches/execution/summarize.py --casts-1385-date --check
python3 crates/delta-arrow-reader-spark-sql/benches/execution/run.py --casts-1385-date preflight BINARY /home/hanbo/.local/share/delta-arrow-reader-review/measurements-2026-11-18/1380/delta-snapshot /home/hanbo/.local/share/delta-arrow-reader-review/casts-1385-2026-11-19/date-stage/preflight-v1
```

Retain raw plans and both ANSI diffs for all three shapes before the export sort.
Compress the full untimed receipt and build/fixture provenance as
`casts-1385-date-preflight.json.gz`. The bound receipt at
`../casts/date/preflight-receipt.json` records source, harness, driver, summarizer,
protocol, manifest, schedule, binary, fixture and baseline-receipt hashes;
expected rows and source I/O; nine exports, six native/Spark comparisons and nine
baseline comparisons. Include the changed product source, input corpus and tests
in the freeze inventory. Commit the exact candidate before rebuilding and
preserving a distinct read-only binary copy. No frozen file is amended with the
postcommit rebuild receipt.

## Fixed affected-shape timing

The stage schedule is the PR 1 schedule filtered to its combined and DATE shapes,
retaining the original process order and configuration rotation. Settings remain
CPU affinity `2,3,4,5`, two runtime workers/target partitions, batch size 8,192,
one warmup and three retained iterations per fresh process. No cache flushing.

- Three alternating native A/B pairs per affected shape: 12 noise processes.
- Three processes per affected shape/configuration: 18 main processes.
- Total: 30 separate warmups and 90 retained iterations in 30 fresh processes.
- Each query process has 180 seconds, with one hour for the fixed schedule.
  Preflight has 300 seconds per export, 180 seconds per comparison and 30 minutes
  overall. Failures retain partial evidence and stop without unchanged retries.
- Pool limits remain 2 GiB for timing and 4 GiB for preflight, address-space limits
  12 GiB and 16 GiB, and spill ceiling 8 GiB. Fixture/exports/scratch allowance is
  64 GiB outside Git, separate from the owned build cache.
- No DECIMAL timing, concurrency, profiles or second optimization round.

All timing boundaries, source read accounting, memory tracking and aggregation
are unchanged from [PR 1's protocol](casts-1385-protocol.md). Query completion
includes planning through stream drain. Take each process's median of three
samples, then the median of three process medians; retain min/max/IQR. Native
A/B arm median differences are the per-shape noise floor. The completion line
remains strictly >1.25 times native AND >50 ms slower; pool findings are separate
diagnostics and cannot authorize DECIMAL work.

Only after the exact committed rebuild, independent review and explicit
coordinator authorization, invoke once into a new destination:

```sh
python3 crates/delta-arrow-reader-spark-sql/benches/execution/run.py --casts-1385-date measure BINARY /home/hanbo/.local/share/delta-arrow-reader-review/measurements-2026-11-18/1380/delta-snapshot /home/hanbo/.local/share/delta-arrow-reader-review/casts-1385-2026-11-19/date-stage/after-once
python3 crates/delta-arrow-reader-spark-sql/benches/execution/summarize.py --casts-1385-date /home/hanbo/.local/share/delta-arrow-reader-review/casts-1385-2026-11-19/date-stage/after-once crates/delta-arrow-reader-spark-sql/benches/reports
```

Each sample must match current preflight rows/files/bytes, already checked against
the baseline. Preserve stage samples, metadata, plans/diffs, validation and
summary/CSV files with the distinct `casts-1385-date` prefix. The DATE-stage report
adds all six affected after medians beside their matched PR 1 before medians.
For DECIMAL, carry the three unchanged baseline measurements with their actual
source commit/stage labels; never label them fresh final-head timings. Its fresh
current correctness/I/O evidence remains separate from carried timing evidence.
Attribution remains non-additive because the isolated projections differ.

DECIMAL's isolated baseline completion flags are false. PR 3 becomes eligible
only if combined completion_flag is true in either ANSI mode after this DATE
change. No DECIMAL implementation starts in this stage. If no eligible change
remains and a shape still exceeds the line, follow the original bounded final
profile rule (at most six shape/mode cases) and report the gap; a second repair
round requires the maintainer's decision. STRING-to-TIMESTAMP remains unchanged;
the baseline report already records its similar parser-construction pattern.

## Local gates and review

Run serially from the Spark workspace:

```sh
export CARGO_TARGET_DIR=/home/hanbo/.cache/delta-arrow-reader/casts-1385-2026-11-19
export CARGO_BUILD_JOBS=2
cargo +1.96.0 fmt --all -- --check
cargo +1.96.0 clippy --locked -p delta-arrow-reader-spark-sql --all-targets --no-deps -- -D warnings
cargo +1.96.0 test --locked --workspace
```

The two new test names may increase totals; all 3,165 old named outcomes,
including three ignored tests, must retain their results. Publish the complete
new named inventory for the final platform checkpoint. The author checks the
full declaration/caller scope, differential expectations, stage guards and prose
with Bulletproof Review, Documentation Writer and Humanizer. A separate clean
reviewer evaluates the concrete candidate before coordinator commit/timing.
The coordinator owns PR/merge and issue updates. No CLI/Python activation,
license changes, `main` merge or #620/#1099/#626/#1340 work is included.
