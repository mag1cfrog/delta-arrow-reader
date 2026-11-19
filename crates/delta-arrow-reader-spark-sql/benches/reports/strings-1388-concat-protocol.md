# CONCAT view row-cost protocol (#1388, PR 3)

This reference covers the single CONCAT change after
[PR 1's attribution](strings-1388.md) and the
[SUBSTRING result](strings-1388-substring.md). The product baseline is staging
`05cde76fe6d46d505916e349ef23436a94416c7d`. Only Sail SparkConcat's string
delegate choice changes. The native ConcatFunc reads eligible View inputs
directly; the existing outer cast/nullif, NULL evaluation order, preparation,
folding, BINARY/List paths and formatting remain unchanged. No new dependency,
fork or string algorithm is introduced.

## Eligible domain and failure preservation

Direct delegation requires Utf8/Utf8View arguments with non-NULL scalars, at
least one Utf8View array, equal array lengths and a checked byte bound no greater
than i32::MAX. Native ConcatFunc then chooses Utf8View, matching Sail's declaration;
the outer cast/nullif and Array/Scalar result form are retained. Large strings,
all-scalar calls, non-string families and every rejected size/type/length case
keep the old materialization route exactly.

Reject other argument types before any per-row size scan. The bound adds all
encoded View lengths, including NULL entries, ordinary Utf8
value-buffer bytes and scalar string bytes times the actual array row count.
Every scalar's own length must also fit i32::MAX, even for zero-row arrays.
Arithmetic is checked. Encoded lengths count repeated and inline values rather
than physical View buffer size. Including NULL View lengths preserves the old
cast's capacity calculation; Arrow's total_bytes_len() has a different contract
because it excludes them. Counting scalar bytes on every row also bounds the
old native concatenation before Sail applies the outer NULL mask. This
conservative sum bounds both old per-input Utf8 materialization and old Utf8
output capacity, so bypassing casts cannot turn an old offset failure into
success. Above-limit cases retain the original failure path.

The [input corpus](../../vendor/sail/crates/sail-function/tests/fixtures/concat-row-cost-1388-inputs.json)
compares the new invocation directly with a
[frozen baseline invocation](../../vendor/sail/crates/sail-function/tests/fixtures/concat-row-cost-1388-original.rs)
that does not use the new guard. Cover ordinary/Large/View strings, every
scalar/column combination, NULL in each argument, empty/multibyte/long values,
all-scalar and zero-row calls, fallback failures and accepted BINARY/List paths.
Guard-only threshold tests use repeated views over small buffers; do not invoke
multi-GiB materialization in tests. Connected SQL checks cover all three decoded
dictionary string types with explicit logical-NULL field declarations, both
ANSI modes, executed storage, effects and reached/suppressed errors. Preserve all
3,172 prior named outcomes (3,169 passed, three ignored) and independent captures.

Independently review and commit the source, corpus, controller and protocol
before release preflight. Run the required correctness checks, build the exact
reviewed source, complete the untimed comparisons and commit their bound receipt.
Rebuild the exact final committed source and verify source/executable/fixture
bindings before the fixed timing run. A clean tree alone is insufficient. No
pilot SQL timings, retries or unchanged reruns are permitted. One reviewed
CONCAT candidate is measured; a remaining gap does not authorize another strategy.

## Workload, before references and immutable files

The new [query manifest](../strings/concat/queries.json) contains only
`string_functions` and `concat_only`, in `native`, `spark-off` and `spark-on`.
It is an exact subset of PR 1's four shapes. Preserve the common ViewTable,
dimension, session/UTC settings and source-to-text expressions. Reuse the genuine
SF1 fixture from [#1380](execution-1380-fixture.json): 6,001,215 lineitems, six
Parquet files, 80 stored columns and the virtual file_bucket column. Verify the
base manifest/log, derived log, every file's size and SHA-256; do not regenerate:

```text
/home/hanbo/.local/share/delta-arrow-reader-review/measurements-2026-11-18/1380/base-sf1
/home/hanbo/.local/share/delta-arrow-reader-review/measurements-2026-11-18/1380/delta-snapshot
```

Combined before measurements and old IPC come from PR 2; isolated CONCAT before
measurements and old IPC come from PR 1, because PR 2 did not retime CONCAT. The
controller selects combined results from
`strings-1388-substring-preflight.json.gz` and isolated CONCAT results from
`strings-1388-preflight.json.gz`, with each original IPC checksum verified. Do
not rerun an old binary. The final report must label these separate campaigns.

The [new controller](../strings/concat/run.py) reuses PR 1's frozen process,
export, comparison, telemetry, measurement and summary functions. Its explicit
bindings select this slice's manifests and receipts. The existing Rust
`--strings-1388` selector and execution/timing/counter functions remain unchanged.
Emitted `source_commit=8260c58ba48e0a65ca7cb7668dd2d8c64b1434f1` identifies the
embedded query manifest's origin. Actual product baseline, current source
objects/HEAD, binary SHA-256 and current protocol/controller hashes are bound
separately. Do not mistake query origin for the product commit.

All 86 historical assets and all 33 protected prior PR 1/PR 2 paths remain
byte-identical, including SUBSTRING code/tests and the existing harness.
UPSTREAM.md may append CONCAT provenance; earlier receipts keep their original
hashes and remain tied to their recorded commits. Root/core, CLI/Python, planning,
NULL selection and dependency forks do not change in this slice. No other shapes,
concurrency cases or profiles run.

## Build and untimed comparison

Serialize Cargo and query work on local Linux. Keep evidence under
`/home/hanbo/.local/share/delta-arrow-reader-review/costs-168-2026-11-19/1388/pr3`.
The retained fixture/export/scratch allowance remains 64 GiB, separate from the
owned build cache. Preserve partial evidence on any failure. From the Spark
workspace, build:

```sh
CARGO_TARGET_DIR=/home/hanbo/.cache/delta-arrow-reader/costs-168-2026-11-19 \
CARGO_BUILD_JOBS=2 CARGO_PROFILE_RELEASE_DEBUG=1 \
cargo +1.96.0 build --locked --release --bench execution \
  --message-format=json-render-diagnostics
```

Keep opt3/debug1 with no RUSTFLAGS, LTO, CPU, codegen or #1381 CLI overrides.
Record command/CWD, verbose toolchain, locked source/config hashes, resolved
compiler artifact, profile and executable SHA-256. Keep an independent read-only
executable seal. From the repository root:

```sh
python3 crates/delta-arrow-reader-spark-sql/benches/strings/concat/run.py --check
python3 crates/delta-arrow-reader-spark-sql/benches/strings/concat/run.py preflight BINARY /home/hanbo/.local/share/delta-arrow-reader-review/measurements-2026-11-18/1380/delta-snapshot /home/hanbo/.local/share/delta-arrow-reader-review/costs-168-2026-11-19/1388/pr3/preflight
```

Export all six complete results and decode four native/Spark comparisons,
allowing only the existing storage/metadata/nullability normalization. Require
6,001,215 equal values, NULLs and row multiplicities, equal executed SQL types,
two output partitions, final counters, identical observed source paths and exact
received/range byte totals. Retain actual Arrow types, all ranges/counters,
pre-export-sort plans and four native/Spark plan diffs. Confirmed native/Spark
value/type/count or complete source-I/O differences exclude the whole shape.
Crashes, OOM, missing IPC, malformed output or incomplete counters stop preflight.
Only the three exact decoded mismatch errors with exit status 1 establish a
comparison-process exclusion; never infer equality from hashes or row totals.

Then decode six old/new comparisons using the per-shape receipts above. Require
equal rows, values, NULLs, multiplicity, SQL types, actual Arrow storage and exact
source I/O for every configuration. Any difference stops the slice before timing;
it does not permit a correctness change. The new full compressed receipt retains
all proofs and ranges; the compact receipt stores expectations, eligibility and
source/binary/input hashes. The initial native/Spark phase and additional old/new
phase each have a 1,800-second bound. Commit the reviewed receipt and verify the
exact final build before measurement.

## Fixed measurement and reporting

The [schedule](../strings/concat/schedule.json) filters PR 1's order to these two
shapes. Each has three fresh process replicas per configuration and three native
A/B pairs. Each process runs one warmup and three retained iterations. If both
shapes are eligible, collect 30 processes, 30 warmups and 90 retained samples.
Use affinity 2,3,4,5, two runtime workers/target partitions, batch 8192, 2 GiB
measured/4 GiB preflight pools, 12/16 GiB address caps and 8 GiB spill. Keep
180-second measured and 300-second preflight process limits and the 3,600-second
measurement bound. No new profiling phase is scheduled.

After the committed freeze and exact build have been independently verified,
run the fixed collection once and summarize its retained records:

```sh
python3 crates/delta-arrow-reader-spark-sql/benches/strings/concat/run.py measure BINARY /home/hanbo/.local/share/delta-arrow-reader-review/measurements-2026-11-18/1380/delta-snapshot /home/hanbo/.local/share/delta-arrow-reader-review/costs-168-2026-11-19/1388/pr3/after-once
python3 crates/delta-arrow-reader-spark-sql/benches/strings/concat/run.py summarize /home/hanbo/.local/share/delta-arrow-reader-review/costs-168-2026-11-19/1388/pr3/after-once /home/hanbo/.local/share/delta-arrow-reader-review/costs-168-2026-11-19/1388/pr3/after-summary
```

Validate every retained sample's rows, partitions, complete counters and exact
source I/O against preflight. Reuse median-of-process-medians and min/max/IQR.
Per-shape noise is the absolute difference of native A/B arm medians under the
same aggregation. The flag in each ANSI mode is strictly
`spark_time - native_time > noise`; equality is false. The old 1.25x/50 ms cutoff
does not apply. The inherited summary's profile_cases field is eligibility only,
not a capture or authorization.

Report before/after native/off/on completion in ms and ns/row, spread, noise,
exact flags and exclusions. Retain phase, first-batch, memory and source metrics.
Show native drift and variation without treating separate campaigns as paired
causal measurements. Carry SUBSTRING/UPPER evidence forward with its original
campaign labels; do not retime them. Combined gaps are not an additive function
breakdown. If any target remains missed, state its size and the extent of proven
attribution, keep #1388 open and stop work on this issue. There is no second
optimization round or extra profile in this protocol.
