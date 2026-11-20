# Ordinary STRING timestamp protocol (#1389, PR 3)

This reference fixes one candidate for ordinary LTZ/NTZ conversion after
merged PR 2, staging `bc5ac56fc6630c76dadbba6582f704616e22eae0`. Product scope is
SparkTimestamp's unformatted STRING path in
[spark_timestamp.rs](../../vendor/sail/crates/sail-function/src/scalar/datetime/spark_timestamp.rs).
Review and commit the code and protocol before release preflight. Commit its
successful receipt, then verify an exact-source rebuild against the sealed
preflight executable before timing. Run the selected timing schedule once.

## Candidate boundary and correctness proof

Only exact 19-byte ASCII `YYYY-MM-DD HH:MM:SS` text with valid calendar/clock
fields uses the direct Chrono calendar helper. The existing normalized parser
uses the same Chrono validation. The resulting naive datetime goes through the
unchanged `localize` route with no explicit input zone. LTZ retains the session
zone and DST gap/overlap policy. NTZ retains the wall clock. Invalid fields,
leap seconds, fractions, explicit zones, trimmed text, other spellings and
non-ASCII input retain the existing parser, error order and safe/strict policy.
The reused array fallback remains lazy. NULLs bypass conversion. Scalars share
the same guard and original fallback. Formatted input, STRING date_format,
DATE, intervals, grammar, optimizer and dependency forks are unchanged.

The [additional corpus](../../vendor/sail/crates/sail-function/tests/fixtures/timestamp-cast-1389-inputs.json)
has 72 inputs: 19 ordinary calendar cases and 53 fallback cases. Direct checks
compare values and exact error variants/text with the original normalized
parser and assert whether fallback ran. Include year zero and year/calendar/
clock boundaries, New_York spring gaps and fall overlaps, fractions, explicit
offsets/zones, leap seconds, controls, separators and Unicode. Check NTZ and
LTZ UTC/New_York/Kathmandu/invalid-zone in safe and strict modes.

The [connected tests](../../tests/parser_reuse.rs) compare the same inputs with
the unchanged [frozen conversion bodies](../../tests/fixtures/row-parser-1389-original.rs).
Cover scalar and array results, Utf8/LargeUtf8/Utf8View, SQL dictionaries, NULLs,
UTC/New_York/Kathmandu, both ANSI modes and CAST/TRY_CAST. Require identical
actual result types, values, NULLs and errors, including SQL failure phase.
Keep the existing differential tests and captures. Record provenance in
[UPSTREAM.md](../../UPSTREAM.md). Run focused checks, independent review and
the full Linux regression gates, comparing named outcomes rather than totals
alone. Do not merge a confirmed regression or introduce a second candidate.

## Frozen workload and selected schedule

Reuse the unchanged PR 1 [manifest](../parsers/queries.json),
[schedule](../parsers/schedule.json), common native ViewTable and genuine SF1
fixture. The two timed shapes are `timestamp_ltz` and `timestamp_ntz`. Both
retain 6,001,215 rows, including their original NULL inputs. Keys remain
Int64/Int32. LTZ results are Timestamp(Microsecond, UTC), and NTZ results are
Timestamp(Microsecond, no zone), with identical executed SQL types in native
and Spark. Each execution retains its six-file, 25,585,934-byte source work.
The common native input-text construction remains inside completion timers.
Only ordinary UTC input is timed. Other forms and zones receive correctness
coverage without a separate performance collection.

The [controller](../parsers/run.py) adds `--ordinary-timestamp-cast`, selecting
separate `timestamp-preflight-receipt.json` and `parsers-1389-timestamp-*`
evidence. Default and `--reuse-row-parsers` modes retain their prior behavior.
Their actual measured identities stay pinned to PR 1 commit
`997278f7f73ad53b050f3d4b4ca381dd5eaf5e34` and PR 2 candidate
`1f6cb4fcd6ebbce3b993f411b9f66571ec6d230d`. The Rust harness and its workload
origin marker `298015f9def26cc97a271bdbc0316e64083d7874` are unchanged.

Timing filters the original schedule by the two query IDs, preserving every
selected process name, configuration, replica, A/B arm and relative position.
There is no copied manifest or schedule. This selects 30 fresh processes:
12 native A/B noise processes before 18 native/Spark main processes. Each has
one warmup and three retained iterations, for 90 retained samples. Preflight
still covers all eight shapes and 22 configurations. Receipt eligibility
therefore continues to describe the full workload. Only the two selected
shapes contribute timing records and summary rows.

Keep affinity 2,3,4,5, two runtime workers/partitions, batch 8192, 2/4 GiB
measured/preflight pools, 12/16 GiB address caps and 8 GiB spill. Keep the
180-second measured process limit and 3,600-second campaign bound. There are
no pilots, profiles, concurrency cases or unchanged timing repetitions.

## Build and complete untimed preflight

Serialize Cargo/query work on local Linux. From the Spark workspace:

```sh
CARGO_TARGET_DIR=/home/hanbo/.cache/delta-arrow-reader/costs-168-2026-11-19 \
CARGO_BUILD_JOBS=2 CARGO_PROFILE_RELEASE_DEBUG=1 \
cargo +1.96.0 build --locked --release --bench execution \
  --message-format=json-render-diagnostics
```

Keep release opt3/debug1, Rust 1.96.0 and no RUSTFLAGS, CPU, LTO, codegen or CLI
overrides. Record command/CWD, toolchain, source/config hashes, artifact/profile,
HEAD/product baseline and executable SHA-256. Keep an independent executable
seal. Preserve all previous receipts, evidence, captures and 86 historical
assets. No sealed binaries or raw IPC enter Git.

Use new evidence paths under
`/home/hanbo/.local/share/delta-arrow-reader-review/costs-168-2026-11-19/1389/pr3`.
The protected input stays at
`/home/hanbo/.local/share/delta-arrow-reader-review/measurements-2026-11-18/1380/delta-snapshot`.
Verify its retained base manifest, both Delta logs and every Parquet size/hash.
Keep the existing 64 GiB fixture/export/scratch allowance.

From the repository root:

```sh
python3 crates/delta-arrow-reader-spark-sql/benches/parsers/run.py --ordinary-timestamp-cast --check
python3 crates/delta-arrow-reader-spark-sql/benches/parsers/run.py --ordinary-timestamp-cast preflight BINARY /home/hanbo/.local/share/delta-arrow-reader-review/measurements-2026-11-18/1380/delta-snapshot /home/hanbo/.local/share/delta-arrow-reader-review/costs-168-2026-11-19/1389/pr3/preflight
```

Export all 22 complete results and retain all 14 native/mode comparisons.
Then verify the retained original PR 1 IPC hashes and fully decode all 22
old/new pairs. The [retained PR 2 validation](parsers-1389-retained-validation.json)
already establishes complete equality with those original outputs. Bind that
validation along with the original full preflight/receipt. Compare every value,
NULL, multiplicity, row count, executed SQL type, actual Arrow storage/unit/zone
type, and exact source objects/bytes/ranges, including the unchanged DATE,
date_format, three interval shapes and projection control. Hashes and row totals
alone do not establish equality. Any old/new difference stops the candidate.

A completed native-pair semantic mismatch excludes that entire shape from
timing and is reported. Process errors, incomplete counters, control failures
or a shared incorrect declared input count stop preflight. Do not reclassify
a failed native pair or patch product behavior to proceed. A single shared
1,800-second deadline covers exports and every comparison. Retain 300-second
export and 180-second comparison limits.

Retain raw plans/diffs, complete counters, read ranges and decoded proofs.
Write `parsers-1389-timestamp-preflight.json.gz` and its separate bound receipt.
Bind the executable, product source, fixture, frozen workload, controller, new
protocol/corpus, frozen oracle, tests/provenance and previous correctness
evidence. After committing the receipt, confirm the exact rebuild's binary hash
before collecting any timing.

## One collection and decision

```sh
python3 crates/delta-arrow-reader-spark-sql/benches/parsers/run.py --ordinary-timestamp-cast measure BINARY /home/hanbo/.local/share/delta-arrow-reader-review/measurements-2026-11-18/1380/delta-snapshot /home/hanbo/.local/share/delta-arrow-reader-review/costs-168-2026-11-19/1389/pr3/after-once
python3 crates/delta-arrow-reader-spark-sql/benches/parsers/run.py --ordinary-timestamp-cast summarize /home/hanbo/.local/share/delta-arrow-reader-review/costs-168-2026-11-19/1389/pr3/after-once /home/hanbo/.local/share/delta-arrow-reader-review/costs-168-2026-11-19/1389/pr3/after-summary
```

Validate each retained sample against complete preflight rows, two partitions,
completed counters and exact source I/O. Report native and both ANSI modes in
ms and ns per returned row, with median of process medians, minimum/maximum/IQR,
native drift, A/B noise and exclusions. A shape's flag is strictly
`spark - native > native A/B noise`, separately per ANSI mode. Retain separate
planning/execution/first-batch, source, RSS and pool metrics, plus raw samples,
metadata, full preflight, plans and all summary rows.

Compare with the [PR 1 measurements](parsers-1389.md) and
[exact full PR 2 candidate measurements](parsers-1389-reuse.md). The retained
PR 2 binary after DATE omission was checked only for correctness. It has no
before timing campaign. State that limitation rather than presenting the
historical candidate as a timed retained binary or a matched new baseline.
Report native drift and spreads because separate campaigns do not establish
paired causal effects. If a confirmed Spark regression remains after accounting
for the observed execution ranges and native drift, stop and report it. Do not
claim the issue target is met while a selected native-reference excess remains
above its same-binary noise floor. Name the preserved general-parser fallback
and any other supported residuals without starting another optimization.
