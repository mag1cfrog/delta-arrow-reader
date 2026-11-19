# SUBSTRING prefix row-cost protocol (#1388, PR 2)

This reference describes the single SUBSTRING change following
[PR 1's attribution](strings-1388.md). The product baseline is staging
`7cbb9dbf0ca0e625f0fe4a50f12292e0720788b7`. The adaptation delegates Utf8View
inputs with non-NULL scalar Int64 position 1 and nonnegative scalar Int64 length
to the installed DataFusion `unicode::substr()` singleton. It removes the
unconditional positional-length calculation, including the full character count
on the measured short-prefix path. It does not
add a Unicode bounds implementation or a dependency fork.

The source-type guard preserves the existing executed storage type. Position 1
keeps the delegated view's byte offset unchanged; other positions retain the old
kernel because valid views near u32::MAX in buffers larger than 4 GiB can overflow
the native builder's offset addition. Ordinary/Large STRING, dictionary decoding,
column or NULL bounds, non-prefix positions, negative lengths, BINARY and private
closed-lambda/async evaluation retain their existing paths. Omitted length is
still INT_MAX and can use the prefix path after the existing expansion.

Independently review the product diff, differential corpus, protocol and new
controller before committing them. Run the required correctness checks, build
the exact reviewed source and complete the untimed comparisons below. Commit the
successful bound receipt, rebuild the exact committed source, and verify source,
executable and fixture hashes before the fixed timing schedule. A clean tree
alone does not establish that the executable matches the source. Do not run pilot
SQL timings, retry a failed collection or repeat an unchanged schedule. Retain
partial evidence on any failure. One reviewed SUBSTRING strategy is measured;
the result does not authorize another implementation round.

## Scope and source identity

The new [manifest](../strings/substring/queries.json) contains only
`string_functions` and `substring_only`, on `native`, `spark-off` and `spark-on`.
They are exact subsets of PR 1's four shapes. The combined query retains CONCAT
and UPPER so that this change's effect on the original query remains visible.
CONCAT and UPPER measurements remain the labeled PR 1 evidence; do not rerun them
for this slice or treat their earlier measurements as new observations.

The new [controller](../strings/substring/run.py) imports the unchanged PR 1
driver. Explicit module bindings select this slice's two-shape manifest,
schedule, receipt, hashes and source validation. The existing Rust
`--strings-1388` selector and normal execution/timing/counter functions remain
byte-identical. The selector embeds PR 1's query manifest, so emitted
`source_commit=8260c58ba48e0a65ca7cb7668dd2d8c64b1434f1` identifies the query
manifest's origin. It is not the PR 2 product commit. New metadata and the bound
receipt separately record product baseline 7cbb9dbf, actual Git source objects,
current HEAD, executable SHA-256 and the new controller/manifests' hashes. The
build receipt must bind the exact committed product source to that executable.

All PR 1 frozen files and the 86 protected historical assets remain
byte-identical. Root/core, CLI/Python, dependency forks and all planner/coercion
code remain unchanged. The product edit is the Sail SparkSubstring dispatch
guard described in [UPSTREAM.md](../../UPSTREAM.md#string-and-binary-substring).
The original registry `datafusion-spark` 54.1.0 package is retained with lockfile
checksum `a1ccd16a6949503e56c084df1b90c8889db826ec9347d2f0f51a7837d6fa011e`.

## Fixture, build and untimed comparison

Reuse the genuine SF1 fixture and checksums from
[the #1380 receipt](execution-1380-fixture.json): 6,001,215 rows, six Parquet
objects, 80 stored columns and the same virtual file_bucket column. Do not
regenerate it. The original common native ViewTable, dimension, session setup,
UTC and source-to-text expressions remain unchanged. The fixture roots are:

```text
/home/hanbo/.local/share/delta-arrow-reader-review/measurements-2026-11-18/1380/base-sf1
/home/hanbo/.local/share/delta-arrow-reader-review/measurements-2026-11-18/1380/delta-snapshot
```

Verify the base manifest/log, derived log, all six file sizes and SHA-256 hashes.
Keep local IPC, plans, build/test logs and partial evidence under
`/home/hanbo/.local/share/delta-arrow-reader-review/costs-168-2026-11-19/1388/pr2`.
The existing 64 GiB fixture/export/scratch allowance is separate from the build
cache. Serialize all Cargo and query processes on local Linux. From the Spark
workspace, use the same release build as PR 1:

```sh
CARGO_TARGET_DIR=/home/hanbo/.cache/delta-arrow-reader/costs-168-2026-11-19 \
CARGO_BUILD_JOBS=2 CARGO_PROFILE_RELEASE_DEBUG=1 \
cargo +1.96.0 build --locked --release --bench execution \
  --message-format=json-render-diagnostics
```

Keep optimization level 3/debug level 1, with no RUSTFLAGS, LTO, CPU, codegen or
#1381 CLI release overrides. Retain the command/CWD, verbose toolchain, lock and
source hashes, resolved compiler-artifact path and executable hash. Keep a
separate read-only executable copy. Run from the repository root:

```sh
python3 crates/delta-arrow-reader-spark-sql/benches/strings/substring/run.py --check
python3 crates/delta-arrow-reader-spark-sql/benches/strings/substring/run.py preflight BINARY /home/hanbo/.local/share/delta-arrow-reader-review/measurements-2026-11-18/1380/delta-snapshot /home/hanbo/.local/share/delta-arrow-reader-review/costs-168-2026-11-19/1388/pr2/preflight
```

Preflight reuses PR 1's full IPC export, comparator, native/Spark type and value
checks, source-I/O validation and plan diffs for all six results. Confirmed
native/Spark value/type/count or complete source-I/O differences exclude the
whole shape from timing. Crashes, OOM, missing IPC, malformed output, invalid
partitions or incomplete counters stop collection. The exact legacy comparator
errors with exit status 1 are the only process failures treated as decoded
mismatches. Never infer equality from checksums or row totals alone.

The additional six comparisons use the retained PR 1 IPC results, verified by
their recorded hashes, as the old-product oracle for each shape/configuration.
Require exact decoded values/NULLs/multiplicity, SQL types, actual Arrow storage
types, row counts and source I/O. Any old/new difference stops this slice before
timing; it is not permission to change established semantics. Keep full plans,
old/new comparison proofs and source ranges in the new compressed receipt
`strings-1388-substring-preflight.json.gz`. The compact bound receipt stores
eligibility, rows, expected file identities/counts and received/range bytes,
actual source objects and hashes. The existing full-range receipt remains intact.

The committed focused input corpus also compares the wrapper directly with the
old registry kernel across ordinary/Large/View strings, all eight scalar/column
forms, NULLs, zero-row calls, omitted length, empty/multibyte/long out-of-line
values and direct Int64 extremes. BINARY uses the unchanged binary helper as its
oracle. Resolver tests check all three decoded dictionary storage types, both
ANSI modes, executed result storage and volatile/error suppression. Preserve
every existing named test and independently captured result unchanged.

## Fixed timing and interpretation

The new [schedule](../strings/substring/schedule.json) is PR 1's order filtered to
the two affected shapes: three fresh process replicas for each configuration,
plus three native A/B pairs per shape. Each process has one warmup and three
retained samples. If both shapes are eligible this is 30 processes, 30 warmups
and 90 retained samples. No concurrency cases or other shapes run.

Use affinity 2,3,4,5, two runtime workers, two target partitions, batch size 8192,
2 GiB measured/4 GiB preflight pools, 12/16 GiB address-space caps and an 8 GiB
spill limit. Preserve PR 1's per-process limits (180 seconds measured,
300 seconds preflight), 1,800-second preflight phase bound and 3,600-second
timing bound. The additional old/new comparison phase has its own 1,800-second
bound. There is no new profiling phase in this schedule.

After the reviewed freeze is committed and its exact build verified, run once:

```sh
python3 crates/delta-arrow-reader-spark-sql/benches/strings/substring/run.py measure BINARY /home/hanbo/.local/share/delta-arrow-reader-review/measurements-2026-11-18/1380/delta-snapshot /home/hanbo/.local/share/delta-arrow-reader-review/costs-168-2026-11-19/1388/pr2/after-once
python3 crates/delta-arrow-reader-spark-sql/benches/strings/substring/run.py summarize /home/hanbo/.local/share/delta-arrow-reader-review/costs-168-2026-11-19/1388/pr2/after-once /home/hanbo/.local/share/delta-arrow-reader-review/costs-168-2026-11-19/1388/pr2/after-summary
```

Validate every retained sample against the untimed rows, partitions, complete
counters and exact source I/O. Reuse the frozen median-of-process-medians and
spread aggregation, including min/max/IQR. Per-shape noise is the absolute
difference between the native A and B median-of-process-medians. For each ANSI
mode separately, the completion flag is strictly
`spark_time - native_time > noise`; equality does not flag a shape. The older
1.25x/50 ms reporting rule does not apply.

Report PR 1 before and PR 2 after completion in ms and ns/row, spreads, native A/B
noise, exact flags in both modes and all eligibility exclusions. Retain planning,
execution, first-batch, pool/RSS and source counters as supporting metrics.
Before/after observations come from separate process collections; do not assign
the combined query's entire delta to SUBSTRING or claim an additive decomposition.
If the strict target is still missed, report the remaining gap and evidence and
leave this cost's single implementation round complete. Later CONCAT work is a
separate slice.
