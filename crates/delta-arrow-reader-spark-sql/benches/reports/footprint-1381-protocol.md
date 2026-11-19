# Spark release footprint measurement protocol (#1381)

Prepared before any clean-build or native-process timing. The
maintainer report will record footprint, native behavior and dependency costs
for an adoption decision. It makes no shipping or adoption change.

Product source is pinned to staging merge
`093bf3d65408043b87d39bf92fd5b42eda635013`. Against the fetched `origin/main`
`e89cd6b56f0812446f68e80132cc569bdbddb898`, staging has 1,494 unique commits and
is missing one main commit; their merge base is
`39884a559afc10b1d6c255197471d7eeb05760df`. Record these exact refs rather than
assuming staging's root is current main. The missing main commit changes only
README prose; it changes no root Cargo, CLI or release-build settings.

## Isolated sources and integration limits

The report branch `perf/1381-spark-release-footprint` receives only bench scripts,
frozen inputs, reports, raw evidence and recovery patches. Its root workspace and
CLI never gain Spark dependencies or patches.

One owned scratch worktree is
`/home/hanbo/.local/share/delta-arrow-reader-review/measurements-2026-11-18/1381/scratch-worktree`,
on `perf/1381-spark-link-scratch`. Three immutable source states are required:

| Build | Source |
| --- | --- |
| stock | Pinned staging source, unchanged root Cargo workspace |
| forks-only | Scratch commit with all 15 Spark patches selected at the consuming root, required lock/version alignment, and scratch CI only |
| linked | Next scratch commit, adding Spark as a CLI path dependency and one reachable internal call |

The full current root workspace cannot resolve that linked dependency as-is:
the Python member requires PyO3 0.29, while Sail's jiter 0.15.0 records an
optional PyO3 0.28 dependency. Cargo rejects their conflicting `links=python`
identities even without activating Python in the CLI. Preserve the exact initial
resolver failure. The linked scratch state therefore removes the unrelated
Python member from `members` and lists it under `exclude`. No Python manifest,
Sail/jiter source or dependency version is changed. Verify the selected Linux
CLI normal/build closure contains no Python/PyO3 packages. Linked measurements
describe this CLI-only isolated membership configuration; they do not establish
that the existing complete root workspace can adopt Spark successfully.

The immutable forks-only commit is
`b0c554143c686d44c7fe9897350b24a0a3cc1669`; linked is
`1394728a925b6cdae28e2062eb5637464e4a2fd1`. The cumulative
[forks-only patch](footprint-1381-forks-only.patch) SHA-256 is
`2765889bc8ca9bad68f39454447a89f03045a4d0a0a50b63354c70933fec728c`;
the [linked patch](footprint-1381-linked.patch) SHA-256 is
`20e44214f8f472f36b8c4caf928b8a15517f20cb378026c2d7538df2c3a0fff0`.
Each applies independently to the pinned baseline, and isolated-index recovery
matches the exact committed tree. The forks-only patch includes scratch-only
CI; the linked patch also contains the internal probe and membership isolation.
Neither is a PR or merge candidate.

The root `[patch.crates-io]` paths point into the existing vendored dependency
directories. No fork, Sail, frontend or other product implementation is edited.
Keep existing versions where compatible; record every lock/version change that
resolution forces. Verify the forks-only CLI graph uses all selected fork
packages and does not contain the Spark package. Record any patch not selected
by the actual graph instead of treating its declaration as proof of use.

The linked CLI has an internal environment gate, `DAR_1381_SPARK_SQL`. When set,
it creates a configured session, passes the environment's SQL to `plan_query`,
and creates its physical plan. The plan is consumed through `black_box` before
returning. This keeps both planner entry points reachable under release dead
code elimination. The gate adds no public flag, dialect switch or shipping
behavior. Execute the gate once outside timing and retain linked symbols/bloat
proof. Leave the variable absent for all ordinary commands. The scratch change
is the explicitly authorized measurement exception; another product change
would stop this task for coordinator handling.

## Actual release settings and clean builds

The release source trace is `.github/workflows/cli-build.yml` through
`xtask/src/build.rs::build_binary` and `rustup_command`. It runs Cargo's default
release profile, `--locked --package delta-arrow-reader-cli`, with an explicit
Linux target. Neither root Cargo.toml nor `.cargo/config.toml` overrides release
optimization, debug information, LTO or stripping. The helper clears caller
compiler/profile/wrapper variables and sets:

```text
RUSTFLAGS=-Ctarget-cpu=x86-64
CFLAGS=-march=x86-64 -mtune=generic
CXXFLAGS=-march=x86-64 -mtune=generic
LC_ALL=C
```

Use Rust 1.96.0 for all three comparable builds, target
`x86_64-unknown-linux-gnu`, and two Cargo jobs. Do not inherit the debug override
used for the earlier planning/execution diagnostics. Build on this local Fedora
Linux worker with the release flags; label the host difference from the official
pinned Debian 12 archive builder. These are comparison artifacts, not validated
public release artifacts or a GLIBC compatibility claim.

Tool installation, lock preparation, dependency fetching, source hashing and
license collection happen outside timed clean builds. Install pinned
cargo-about 0.9.2 if needed, under the owned cache, with two jobs and a 30-minute
limit. Retain its command and result. Pre-fetch all three locked graphs using
the same registry/git cache policy. Then use offline Cargo for timed builds,
with no sccache/compiler wrapper or prebuilt target artifacts.

Run exactly one clean release build of each source, in order stock, forks-only,
linked. Each target directory must be new and empty:

```text
/home/hanbo/.cache/delta-arrow-reader/measurements-2026-11-18/target/footprint/clean-stock
/home/hanbo/.cache/delta-arrow-reader/measurements-2026-11-18/target/footprint/clean-forks-only
/home/hanbo/.cache/delta-arrow-reader/measurements-2026-11-18/target/footprint/clean-linked
```

Measure elapsed wall time immediately before Cargo starts through its completed
exit, including compilation/linking and Cargo's offline checks. Exclude strip,
archive, notice, metadata and bloat analysis. Set a 5,400-second bound per build.
No incremental targets or compiler caches are shared between the three builds;
source/download and OS filesystem caches are shared and not flushed. Record
commands, clean-source commits/tree hashes, full compiler/environment receipts,
exit status and binary hashes. Check clean source and expected HEAD both before
and after each timed build. The coordinator's external authorization receipt
binds the protocol commit, protocol/driver/input hashes and all source commits;
command receipts retain UTC start/finish timestamps. The single clean build is itself a measurement,
so do not prebuild these binaries and repeat the same clean schedule.

## Binary, archive and linked-crate sizes

Retain each original release binary. Separately copy it and run GNU
`strip --strip-all` once; record tool version and the stripped copy's bytes/hash.
Do not silently change Cargo's release stripping settings.

The real archive code in `xtask/src/artifacts.rs::package` stores the original
release binary, repository LICENSE and NOTICE with generated dependency notices
appended. Its USTAR member order is `dar`, `LICENSE`, `NOTICE`, modes
0755/0644/0644, zero owner IDs/timestamps, gzip best compression, mtime zero and
OS byte 255. Use that identical policy for all three comparison archives, retain
member sizes/hashes, and report any tooling/compressor difference. Do not package
the stripped copy as though that were the existing release policy. The small
measurement-local writer uses Python's zlib best compression, rather than the
release tool's flate2/zlib-rs backend; all three comparisons use that same writer.
Record Python/zlib versions and do not promise byte identity with release tools.
Archive contents remain local measurement artifacts and are not published.

Run installed `cargo-bloat` once against the completed linked target, with the
same Cargo flags, `--release --locked --bin dar --package delta-arrow-reader-cli
--target x86_64-unknown-linux-gnu --crates -n 20 --message-format json`. Its
incremental artifact lookup is outside the clean-build timer. Verify binary
hashes before/after; a changed artifact is a finding to inspect, not permission
to replace the measured build. Preserve immutable copies before this tool runs.
If the tool rebuilds, retain and label its attribution artifact separately from
the original measured binary, record its changed flags/hash, and do not repeat
the clean build. Report its 20 largest linked code contributors
and the tool's .text-versus-file-size attribution limits. Preserve a symbol
check for retained Spark parser/logical/physical entry points.

## Linux package and license graph

Save target-filtered Cargo metadata and the CLI normal/build dependency closure
for each source using the committed lockfiles and default CLI features. Exclude
dev-only and unrelated Python/xtask workspace packages. Preserve exact package
IDs, versions, source/path and selected features, and verify the selected graph
against Cargo's CLI-target tree. Package counts are distinct resolved package
IDs, not names or every package in Cargo.lock. Report linked additions relative
to stock and forks-only and every duplicate-version family in the linked graph.
Show forks substituted at the same version separately from new package names.
The prepared [graph evidence](footprint-1381-graphs.tar.gz) selects 303 packages
for stock, 303 for forks-only and 456 for linked, including the CLI/root crates.
Stock and forks-only have the same name/version set, with 15 source substitutions;
linked adds 153 name/version pairs and removes none. Cargo metadata includes
inactive optional edges, including jiter-to-PyO3, so a naive metadata traversal
would give wrong counts. Use the actual CLI-target tree and its selected features
as the package set; map those identities back to metadata for license/source data.
The [preparation receipt](footprint-1381-preparation.json) binds these inputs and
preserves the initial full-workspace resolver error.

Compare every SPDX license identifier and exception appearing in the selected
graphs, including packages that specify a license file instead of an expression.
Use pinned cargo-about and the existing release notice collector where it
supports these graphs. Preserve complete errors or unavailable license evidence;
do not assume an unknown license or a path dependency means no notice is needed.
Inspect added packages' LICENSE, copyright and NOTICE material and document what
the archive NOTICE needs. No product allowlist, root NOTICE or source license is
changed. If extra measurement-only notice configuration/text is needed, retain
it with explicit provenance. Include std/toolchain notices consistently in all
three archive comparisons.

Eight selected Sail crates omit both license and license-file package metadata.
The pinned shared `vendor/sail/LICENSE` covers the selected subtree; no standalone
Sail NOTICE/copyright file is present there. Its Apache-2.0 license SHA-256 is
`c71d239df91726fc519c6eb72d318ec65820627232b2f796219e87dcf35d0ab4`.
The measurement-only [about configuration](../footprint/about.toml) clarifies
those eight crates and the Spark crate using unchanged shared license files.
It also accepts `ar_archive_writer`'s sole Apache-2.0 WITH LLVM-exception expression
for that package only. Preserve ordinary-config scan results before using this
supplement. The ordinary release renderer also rejects non-DAR local dependencies;
its product policy is not changed. A small measurement-local notice collector
uses the selected graph, cargo-about texts, available original license/NOTICE/copyright
files, existing checksum-pinned extra notices and Rust standard-library notices
consistently for all three archives. It accepts the known local fork/Sail sources
only within this measurement. Record every collected file/hash and any failure.
The first untimed collection passed stock/forks-only, then stopped because the
published datasketches 0.3.0 crate omits original legal files. Preserve that
failure. Its `.cargo_vcs_info.json` pins Apache DataSketches Rust commit
`9a80c604bec14b14000fc20d47f9f4b3c2baa5a1`, with crate path `datasketches`.
The measurement-only [extra notices](../footprint/extra-notices.toml) add its
upstream root LICENSE and NOTICE at that exact commit, with SHA-256 checks.
Retain the registry checksum, VCS provenance and full source texts. This
supplement is also a packaging integration finding, not a product policy edit.

The published genawaiter 0.99.1, genawaiter-macro 0.99.1 and
genawaiter-proc-macro 0.99.1 packages and their exact pinned source trees have no
original legal/attribution files. Their pinned READMEs also provide none. The
[three explicit gap records](../footprint/attribution-gaps.json) bind name/version,
registry checksum, VCS commit and exact tree/README evidence. The collector may
mark only those exact identities UNAVAILABLE, while preserving their declared
and scanner-chosen license text; every unrecorded missing original still fails.
It infers no copyright holder. Audit and report
`original_attribution_complete=false` for linked, with all three names visible.

Stock/forks-only have complete original-file coverage under this collection
policy. All archive sizes remain measurement-local comparisons. Linked is a
known-text measurement archive; a complete release archive size is unavailable
and unverified because the original attribution is incomplete and the ordinary
collector cannot handle the integration as-is. The known-text size is not a
strict compressed lower bound and is not a validated release artifact.

The other omitted originals are recovered with exact pinned upstream legal
supplements for datasketches, next-gen-proc_macros and regexml. The old
next-gen-proc_macros repository URL returns 404, with no verified redirect. Its
published next-gen parent instead names next-gen-rs at the exact same VCS commit,
lists the proc-macro workspace path, and its manifest plus all three source files
match that source byte for byte. Preserve that proof instead of claiming a
repository rename. Retain regexml's full COPYRIGHT and both original licenses,
including its described third-party MPL/Unicode/W3C material; a scanner's
manifest alternative selection does not establish that all those obligations
are satisfied. These are adoption/packaging findings, with no product policy fix.

The only manifest license token newly appearing versus stock is MPL-2.0, an
alternative in regexml's `Apache-2.0 OR MPL-2.0`; distinguish expressions from
cargo-about's actual selected redistribution license. Report the LLVM exception
package acceptance and missing metadata as packaging integration constraints,
without claiming that an alternative license was selected.

Python-wheel build/size changes remain an estimate
from these CLI differences, explicitly labeled as an estimate, with no wheel
build or binding activation.

## Bounded Rust-version matrix and native tests

The frozen candidates are 1.93.0, 1.94.0, 1.95.0 and 1.96.0. Try ordinary locked
release CLI builds in ascending order, stopping at the first successful full
compile/link for each source. A Cargo rust-version rejection is a recorded lower
boundary, not a compiler error or proof of actual language requirements. Do not
use `--ignore-rust-version`, edit dependencies for an old compiler, search more
versions, or infer success from metadata/check alone. Reuse the measured common
1.96.0 build as its positive proof instead of rebuilding it. Separate owned
MSRV target directories keep these untimed checks out of the clean-build
receipts; each invocation has a 5,400-second bound. Report the lowest tested
successful CLI build and exact tested lower failure reason, including any
manifest-enforced floor. Do not assume forks-only inherits Spark's 1.96 minimum.

On the immutable forks-only source, run and retain all three Linux commands:

```sh
cargo +1.96.0 test --locked --no-fail-fast -p delta-arrow-reader
cargo +1.96.0 test --locked --no-fail-fast -p delta-arrow-reader --all-features
cargo +1.96.0 test --locked --no-fail-fast -p delta-arrow-reader-cli
```

Use two Cargo jobs in a separate owned test target and bounded command deadlines.
Use `--no-fail-fast` to retain later test binaries after an earlier failure.
Record pass/fail/ignored counts, each failed test name and complete errors. Test
failures are adoption evidence; fix none and create no performance issue. A
native-only failure is not automatically a Spark compatibility defect.

## Native CLI correctness and fixed process timing

The [query manifest](../footprint/queries.json) freezes eight cases: `--version`,
a trivial BIGINT literal query and six exact #1380 native statements:
`projection_five`, `statistics_filter`, `group_low`, `count_distinct`,
`row_number`, `self_join`. These cover full scans, statistics filtering,
aggregation/cardinality, distinct aggregation, window sorting and joining without
requiring Spark's wider optional function set. A tables manifest registers the
same retained #1380 snapshot version 0 as `bench`. No fixture is regenerated or
modified; verify its log and all six Parquet hashes against the #1380 receipt.

The clean builds supply the actual stock/forks-only binaries. Before native
query timing, execute each query once per binary outside timing, exporting typed
JSON files. Add only an untimed outer ORDER BY on the frozen result keys to give
stable comparison order. Compare decoded schema SQL types and every row/value,
including NULLs and multiplicity, with a bounded streaming JSON comparator;
normalize Arrow STRING storage and schema metadata/nullability only. Never infer
semantic equality from bytes or counts alone. Negative self-checks cover changed
cell/type, NULL-to-value, missing/duplicate rows and uneven input chunks.
Retain full exports outside Git and their checksums, executed schema/row counts
and exact comparison receipts. Verify version output too. A mismatch stops
native timing for coordinator handling; do not optimize or silently drop a case.
The preflight binds these exact binary/source/query/schedule/fixture identities.

Timed CLI queries retain the original statements without preflight sorting and
emit complete Arrow IPC to `/dev/null`. End-to-end process elapsed time starts
before spawning `dar` and ends after successful exit, including startup,
metadata/snapshot loading, planning, execution, IPC encoding and output drain.
No phase-only execution claim is made. Capture bounded JSON diagnostics on stderr through a pipe, allowing EOF-based
waiting rather than regular-file timeout polling. Persist diagnostics only after
the end timestamp, and check every completed query's row count against preflight.
Record command/timeout requests before timers and preserve exit/timeout receipts
and partial stderr on failure. First/last UTC timestamps and authorization hashes
prove the protocol preceded the timing. There is no result
file/publication or comparison work inside this timer. The trivial query still
uses the explicit local snapshot registration required by the existing CLI.

[schedule.json](../footprint/schedule.json) freezes all 656 fresh processes:

- One discarded warmup per case/binary: 16 processes.
- Same-against-same noise: 10 A/B pairs for each case on each binary, 320 processes.
- Main: 20 stock/forks-only pairs per case, 320 processes, alternating first path
  across rounds/cases. No linked-native timing is requested.

Use CPUs 2,3,4,5, a 12 GiB address-space limit, 2 GiB query pool, no spill,
7,000,000 row/2 GiB output ceilings, 180-second subprocess deadlines and a
2-hour overall schedule bound. Preserve raw process records and failures.
Preflight has 300 seconds per execution and one hour overall. No competing Cargo
or profiling work runs during native timing. There is one fixed schedule, with
no unchanged rerun to chase medians or thresholds.

Report each binary's main median of its 20 fresh-process elapsed times, spread
(min/max/IQR), and each native A/B noise arm's median. For a case, the absolute
noise floor is the larger absolute A/B median difference across stock and
forks-only. Flag every case whose forks-only median exceeds stock by both >5%
and more nanoseconds than this measured noise floor. Use unrounded values and
strict inequalities. Preserve every case and all raw samples even when it is
below threshold or noisy. Slow/large native functions are findings only, with
no optimization or new performance issue.

## Exact driver commands and source authorization

Run the scripts from the report worktree root with the exact retained
evidence directories below. All destinations
are new and are preserved if a command fails. The scripts switch only the owned
scratch checkout among the three immutable sources and restore its owned branch;
they never alter the primary checkout.

The completed untimed graph preparation command was:

```sh
python3 crates/delta-arrow-reader-spark-sql/benches/footprint/build.py prepare /home/hanbo/.local/share/delta-arrow-reader-review/measurements-2026-11-18/1381/graphs
```

Tool and notice preparation may complete before freeze; neither builds the DAR
CLI nor takes a timing sample. After the protocol commit, the coordinator writes
`/home/hanbo/.local/share/delta-arrow-reader-review/measurements-2026-11-18/1381/authorization.json`
with `authorized=true`, protocol commit/hash, all source commits and the frozen
file hashes. The author verifies that receipt and exact source before any timed
clean build. Prepare tools/notices outside timed builds, then run the clean
schedule once:

```sh
python3 crates/delta-arrow-reader-spark-sql/benches/footprint/licenses.py install /home/hanbo/.local/share/delta-arrow-reader-review/measurements-2026-11-18/1381/tool-install
python3 crates/delta-arrow-reader-spark-sql/benches/footprint/licenses.py collect /home/hanbo/.local/share/delta-arrow-reader-review/measurements-2026-11-18/1381/graphs /home/hanbo/.local/share/delta-arrow-reader-review/measurements-2026-11-18/1381/notices-prepared
python3 crates/delta-arrow-reader-spark-sql/benches/footprint/build.py clean /home/hanbo/.local/share/delta-arrow-reader-review/measurements-2026-11-18/1381/clean-builds
```

Bind the actual clean-build binaries in the separate correctness preflight,
then run the native process schedule once only if preflight is equal:

```sh
python3 crates/delta-arrow-reader-spark-sql/benches/footprint/native.py preflight /home/hanbo/.local/share/delta-arrow-reader-review/measurements-2026-11-18/1381/clean-builds/builds.json /home/hanbo/.local/share/delta-arrow-reader-review/measurements-2026-11-18/1381/native-preflight
python3 crates/delta-arrow-reader-spark-sql/benches/footprint/native.py measure /home/hanbo/.local/share/delta-arrow-reader-review/measurements-2026-11-18/1381/native-preflight/preflight.json /home/hanbo/.local/share/delta-arrow-reader-review/measurements-2026-11-18/1381/native-once
python3 crates/delta-arrow-reader-spark-sql/benches/footprint/summarize.py /home/hanbo/.local/share/delta-arrow-reader-review/measurements-2026-11-18/1381/native-once crates/delta-arrow-reader-spark-sql/benches/reports
```

The remaining analysis and untimed compile/test checks use these commands,
serialized in the same Cargo lane. No Cargo/notice/bloat work overlaps native
process timing:

```sh
python3 crates/delta-arrow-reader-spark-sql/benches/footprint/licenses.py archive /home/hanbo/.local/share/delta-arrow-reader-review/measurements-2026-11-18/1381/notices-prepared /home/hanbo/.local/share/delta-arrow-reader-review/measurements-2026-11-18/1381/clean-builds/builds.json /home/hanbo/.local/share/delta-arrow-reader-review/measurements-2026-11-18/1381/archives
python3 crates/delta-arrow-reader-spark-sql/benches/footprint/build.py bloat /home/hanbo/.local/share/delta-arrow-reader-review/measurements-2026-11-18/1381/clean-builds/builds.json /home/hanbo/.local/share/delta-arrow-reader-review/measurements-2026-11-18/1381/linked-attribution
python3 crates/delta-arrow-reader-spark-sql/benches/footprint/build.py msrv /home/hanbo/.local/share/delta-arrow-reader-review/measurements-2026-11-18/1381/clean-builds/builds.json /home/hanbo/.local/share/delta-arrow-reader-review/measurements-2026-11-18/1381/msrv
python3 crates/delta-arrow-reader-spark-sql/benches/footprint/build.py test /home/hanbo/.local/share/delta-arrow-reader-review/measurements-2026-11-18/1381/native-tests
```

Each of the four scripts has a `--check` mode using only synthetic/local data.
These checks cover selected-graph parsing with inactive optional packages,
release environment filtering, streaming semantic comparison and diagnostics,
archive headers/members, missing/duplicate process records, and strict 5%/noise
boundaries. They do not execute a product query or take a timing sample.

## One scratch CI dispatch and completion

The ordinary `ci.yml` dispatch only enables Spark jobs, so forks-only contains a
scratch-only replacement dispatch workflow with macOS and Windows jobs. Each
checks the exact forks-only source/graph, builds the CLI and runs the same three
core/CLI package-feature selections with Rust 1.96.0 and two Cargo jobs. Each job
has a 120-minute bound. Its suite commands use ordinary Cargo fail-fast behavior;
all three suite steps still run after a test failure when the CLI build passed.
Record any later test binaries skipped within a failed platform suite. The
complete named failure inventory comes from Linux's `--no-fail-fast` commands.
Retain job URLs, run ID/attempt, event-head SHA and outcomes. These
are portability build/test results, not hosted-runner timing samples.

The coordinator first commits forks-only (including scratch CI), then linked,
then this protocol and the two recovery patches with their immutable identities.
The same scratch remote ref may be pushed at the exact forks-only commit for
one `ci.yml` dispatch even when the local scratch checkout is already linked.
Verify the event head equals forks-only, never linked. No second scratch branch,
PR or merge is created. The coordinator authorizes/owns remote operations.

The final report under this directory includes build/size/package tables,
all license/NOTICE findings, tested Rust boundaries, every Linux test failure,
native medians/noise/flags, macOS/Windows outcomes, archive policy and limitations,
and the labeled Python estimate. Compressed raw data and both recovery patches
accompany it. Validate bench scripts, formatting and appropriate scoped checks;
retain independent clean-context review before the coordinator's single report
PR into staging. No scratch product change merges anywhere. The coordinator
owns commits, PR, merge, hierarchy updates and cleanup.
