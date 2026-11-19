# Spark release footprint measurements (#1381)

On this local Linux run, forks-only added 479,104 bytes to the strip-all CLI
copy. Linking the reachable Spark planning path added another 48,279,656 bytes.
The three clean release builds took 732.510, 736.361 and 996.613 seconds
respectively, with one observation per build. These are comparison measurements,
not adoption changes.

All seven stock/forks-only query results and executed types matched in the
separate untimed preflight. None of the six real Delta queries crossed the
frozen native slowdown rule. `--version` crossed it marginally: a 5.2405%
point difference, only 0.0027995 ms beyond the observed noise floor. This does
not establish a robust causal fork regression.

The source and packaging constraints matter alongside size and speed. The full
root workspace cannot resolve Spark as-is because its Python member conflicts
with jiter's optional PyO3 version. The linked source excludes that unrelated
member only for this measurement. It also has three unresolved original
attribution gaps. Its archive size below is a known-text measurement archive;
a complete release archive size is unavailable and unverified.

## Sources, release flags and boundaries

The [protocol](footprint-1381-protocol.md) was committed before timing as
`e723ba87e48148aa6638f6e9f77c81633b2ad78b`. Product source is pinned to staging
`093bf3d65408043b87d39bf92fd5b42eda635013`. Stock uses that exact commit;
forks-only is `b0c554143c686d44c7fe9897350b24a0a3cc1669`; linked is
`1394728a925b6cdae28e2062eb5637464e4a2fd1`. The cumulative
[forks-only patch](footprint-1381-forks-only.patch) and
[linked patch](footprint-1381-linked.patch) each independently recover their
exact source tree from the baseline. No scratch branch merges anywhere, and
the report branch adds no Spark dependency or patches to the root workspace.

Against fetched main `e89cd6b56f0812446f68e80132cc569bdbddb898`, staging has
1,494 unique commits and is missing one main commit. That main-only commit
changes README prose, with no root Cargo, CLI or release-build setting change.
The merge base is `39884a559afc10b1d6c255197471d7eeb05760df`.

The fifteen patches replace Arrow array/arithmetic/cast, eleven DataFusion
components and lexical-parse-float at the consuming root. Existing versions
already align: Arrow 58.4.0, DataFusion 54.1.0 and lexical-parse-float 1.0.6.
No fork, frontend or Sail implementation was changed. Linked adds only the CLI
path dependency and a private environment-gated call through logical and physical
Spark planning. Its runtime probe passed, and retained frontend symbols are
recorded. This is measurement material, not a user-facing dialect switch.

The release source trace is `.github/workflows/cli-build.yml` through
`xtask/src/build.rs`. All three comparable builds use Rust 1.96.0, default Cargo
release optimization, target `x86_64-unknown-linux-gnu`, two jobs,
`RUSTFLAGS=-Ctarget-cpu=x86-64` and generic x86-64 C/C++ flags. No LTO, debug or
stripping override was added. Cargo's release debug-0 invocation includes its
default strip-debuginfo behavior; ordinary symbols remain. The original binary
with symbols is distinct from a separately copied GNU `strip --strip-all` binary.

Each clean target directory was new and empty, with no incremental/compiler
cache reuse or sccache. All graphs were fetched first, then builds ran offline
in stock/forks-only/linked order. Registry/git sources and the OS filesystem
cache were shared and not flushed. Source HEAD/cleanliness was checked before
and after each build. UTC command receipts and the coordinator authorization
bind source, protocol and frozen input hashes. No clean build was repeated.

The host is Fedora Linux on a Ryzen 7 8845HS, eight cores/16 threads. These builds
match release profile/CPU flags, rather than the official pinned Debian 12 host.
They do not establish the public archive's GLIBC compatibility. Native process
measurements use four fixed cores (2,3,4,5); no other local Cargo or profiling
work overlaps them. Host/compiler/configuration receipts are retained with raw
measurements.

## Build and size results

Each build time is one clean Cargo wall-time observation through successful exit,
including compilation/linking, and excluding tool setup, source checkout,
license scans, stripping, archives and code-size analysis. MiB means 1,048,576
bytes. File sizes are exact; there is no build-time confidence interval.

| Build | Clean release seconds | Original with symbols bytes | Strip-all bytes | Strip-all MiB | Local archive bytes | Archive qualification |
| --- | --- | --- | --- | --- | --- | --- |
| stock | 732.510 | 167,032,840 | 132,749,008 | 126.599 | 51,336,373 | original-file coverage complete |
| forks-only | 736.361 | 167,558,296 | 133,228,112 | 127.056 | 51,537,813 | original-file coverage complete |
| linked | 996.613 | 233,996,616 | 181,507,768 | 173.099 | 68,840,125 | known-text; incomplete original attribution |

All archives are measurement-local. They contain the original release binary,
LICENSE and NOTICE, in that order, with USTAR headers, modes 0755/0644/0644,
zero owners/timestamps, and gzip best compression. The local writer uses Python
3.14.8/zlib-ng 1.3.1, while the official tool uses flate2/zlib-rs. All three
comparisons use the same local policy. The stripped copy is not silently
substituted into the existing release archive policy. Linked's known-text size
is not a strict compressed lower bound or a verified complete release size.

Forks-only's single observed build time is 3.851 seconds above stock; linked is
260.253 seconds above forks-only. Source-cache warmth, ordering and local worker
conditions limit interpretation of these single observations. Binary hashes,
archive member/hash receipts and exact commands remain available for checking.

## Selected Linux dependency graph

Package counts include the CLI and root crate and use unique selected Cargo
package IDs in the Linux normal/build graph. They exclude dev-only/unrelated
workspace packages. Cargo metadata alone retains inactive optional dependencies;
for example it includes jiter-to-PyO3 edges that the actual CLI target does not
select. Counts and features therefore come from the CLI-target tree, mapped
back to metadata. No Python/PyO3 package is selected in these CLI graphs.

| Build | Selected packages | Logical name/version additions vs stock | Source substitutions |
| --- | --- | --- | --- |
| stock | 303 | 0 | 0 |
| forks-only | 303 | 0 | 15 |
| linked | 456 | 153 | 15 |

Stock and forks-only have identical package-name/version sets. Linked adds
126 distinct new names represented by 127 name/version pairs, plus 26 additional
versions of existing names, removing no logical name/version pair.
[All 153 additions](footprint-1381-added-packages.csv) retain versions, sources,
license expressions and selected features. [Every duplicate-version family](footprint-1381-duplicate-versions.csv)
is included below; the raw [graphs](footprint-1381-graphs.tar.gz) retain the
selected tree and full metadata. A duplicate version is a graph fact, not an
optimization proposal.

| Package | Stock versions | Linked versions | New duplicate family |
| --- | --- | --- | --- |
| base64 | 0.22.1,0.23.1 | 0.22.1,0.23.1 | no |
| block-buffer | 0.10.4 | 0.10.4,0.12.1 | yes |
| chumsky | (absent) | 0.12.0,1.0.0-alpha.8 | yes |
| cpufeatures | 0.2.17,0.3.0 | 0.2.17,0.3.0 | no |
| crypto-common | 0.1.7 | 0.1.7,0.2.2 | yes |
| digest | 0.10.7 | 0.10.7,0.11.3 | yes |
| foldhash | 0.1.5,0.2.0 | 0.1.5,0.2.0 | no |
| getrandom | 0.2.17,0.3.4,0.4.3 | 0.2.17,0.3.4,0.4.3 | no |
| hashbrown | 0.14.5,0.15.5,0.17.1 | 0.14.5,0.15.5,0.17.1 | no |
| icu_collections | 2.2.0 | 1.5.0,2.2.0 | yes |
| icu_normalizer | 2.2.0 | 1.5.0,2.2.0 | yes |
| icu_normalizer_data | 2.2.0 | 1.5.1,2.2.0 | yes |
| icu_properties | 2.2.0 | 1.5.1,2.2.0 | yes |
| icu_properties_data | 2.2.0 | 1.5.1,2.2.0 | yes |
| icu_provider | 2.2.0 | 1.5.0,2.2.0 | yes |
| litemap | 0.8.2 | 0.7.5,0.8.2 | yes |
| md-5 | 0.10.6 | 0.10.6,0.11.0 | yes |
| ordered-float | 2.10.1 | 2.10.1,5.5.1 | yes |
| phf | 0.12.1 | 0.12.1,0.13.1 | yes |
| phf_shared | 0.12.1 | 0.12.1,0.13.1 | yes |
| rand | 0.10.2,0.9.5 | 0.10.2,0.8.8,0.9.5 | no |
| rand_core | 0.10.1,0.9.5 | 0.10.1,0.6.4,0.9.5 | no |
| regex-automata | 0.4.18 | 0.2.0,0.4.18 | yes |
| reqwest | 0.12.28,0.13.4 | 0.12.28,0.13.4 | no |
| sha2 | 0.10.9 | 0.10.9,0.11.1 | yes |
| syn | 2.0.119,3.0.3 | 1.0.109,2.0.119,3.0.3 | no |
| tinystr | 0.8.3 | 0.7.6,0.8.3 | yes |
| writeable | 0.6.3 | 0.5.5,0.6.3 | yes |
| yoke | 0.8.3 | 0.7.5,0.8.3 | yes |
| yoke-derive | 0.8.2 | 0.7.5,0.8.2 | yes |
| zerotrie | 0.2.4 | 0.1.3,0.2.4 | yes |
| zerovec | 0.11.6 | 0.10.4,0.11.6 | yes |
| zerovec-derive | 0.11.3 | 0.10.4,0.11.3 | yes |

## License and NOTICE findings

The only new token in selected package manifest license expressions is MPL-2.0,
in regexml's `Apache-2.0 OR MPL-2.0`. cargo-about selected Apache-2.0 under the
measurement configuration. This is a scanner/manifest choice, not a conclusion
that all original third-party obligations are satisfied: regexml's original
COPYRIGHT describes MPL-derived Saxonica material and Unicode/W3C notices.
The complete original COPYRIGHT and both licenses are preserved.

The ordinary linked license scan failed on eight Sail packages lacking declared
license metadata and on ar_archive_writer's sole `Apache-2.0 WITH LLVM-exception`
expression. The bench-only configuration clarifies the unchanged shared Sail
LICENSE and accepts that exception for this package. Its chosen source text
includes the LLVM exception even though cargo-about labels its license ID as
Apache-2.0. The ordinary release notice renderer also rejects non-DAR local
path dependencies. No product allowlist, manifest, NOTICE or dependency source
was repaired here.

Original legal files omitted from datasketches, next-gen-proc_macros and regexml
are supplied through checksum-pinned, source-verified measurement-only extras.
The next-gen macro metadata's old repository URL returns 404; no redirect or
rename is claimed. Its published parent names the canonical repository at the
same commit, declares the macro workspace path, and its manifest plus three
source files match byte for byte. [Preparation evidence](footprint-1381-preparation.json)
records that proof and the original scan failures.

Three genawaiter packages have no original legal/attribution files in their
published crates or exact pinned source trees. Their declared and scanner-chosen
license texts remain present, but no copyright holder was invented. Only the
three exact checksummed/VCS-pinned identities in the frozen gap inventory may
be marked UNAVAILABLE; an unknown missing original still fails collection.

| Package | Version | Declared license | Original attribution |
| --- | --- | --- | --- |
| genawaiter | 0.99.1 | MIT | UNAVAILABLE |
| genawaiter-macro | 0.99.1 | MIT/Apache-2.0 | UNAVAILABLE |
| genawaiter-proc-macro | 0.99.1 | MIT/Apache-2.0 | UNAVAILABLE |

Stock/forks-only have complete original-file coverage under this collection
policy; linked records `original_attribution_complete=false`. Existing packaging
cannot be described as ready for this integration: local-source policy, missing
Sail metadata, LLVM exception acceptance, verified extra notices and the three
unknown original attributions all remain visible adoption/packaging findings.
[Notice evidence](footprint-1381-notices.tar.gz) includes scans, full known texts,
file hashes and failures. The three linked gaps leave a complete release archive
size unavailable/unverified. These findings are not fixed or filed as new issues.

## Code-size attribution

The first cargo-bloat 0.12.1 invocation set
`CARGO_PROFILE_RELEASE_STRIP=false` and rebuilt a different diagnostic artifact,
SHA-256 `ee483877f5cac8bffc5c132d8d450be60d1c7a871a0e88460dba93e896c7d483`.
Its .text was 134,233,959 bytes, compared with 134,189,959 in the measured
original; bytes, VMA, symbols and read-only data differed. That first output
is preserved and is not the source of this table.

The coordinator and independent reviewer approved one additional untimed
analysis before execution, adding only
`--config 'profile.release.strip="debuginfo"'` to restore Cargo 1.96's original
release profile. It reproduced the entire measured linked binary byte for byte:
SHA-256 `aff6f0b6261d87a584934a1ea11bdb27eaab07fc20b3aeface0cbfc2494c1cd7`,
233,996,616 bytes, including the 134,189,959-byte .text section. The coordinator
independently checked both hashes and the command/log receipts. The following
twenty entries therefore describe the actual measured binary.

This second diagnostic invocation is an explicit deviation from the frozen
one-bloat rule, authorized to correct the tool's profile change. Both immutable
artifacts, the pre-execution request, commands, outputs and before/after receipts
are retained. Product source, the three clean-build samples and all 656 native
process samples were unchanged and were not repeated.

| Rank | Crate attributed in measured binary | Attributed .text bytes | Measured .text share |
| --- | --- | --- | --- |
| 1 | sqlparser | 19,489,319 | 14.52% |
| 2 | std | 17,886,593 | 13.33% |
| 3 | sail_plan | 10,828,364 | 8.07% |
| 4 | datafusion_expr | 5,778,190 | 4.31% |
| 5 | sail_function | 4,668,646 | 3.48% |
| 6 | datafusion_optimizer | 4,232,887 | 3.15% |
| 7 | datafusion_functions_aggregate | 4,015,747 | 2.99% |
| 8 | datafusion_physical_plan | 3,564,022 | 2.66% |
| 9 | arrow_cast | 3,367,989 | 2.51% |
| 10 | datafusion_physical_expr | 3,277,747 | 2.44% |
| 11 | datafusion_functions | 3,179,490 | 2.37% |
| 12 | parquet | 2,605,378 | 1.94% |
| 13 | datafusion_functions_nested | 2,475,225 | 1.84% |
| 14 | datafusion | 2,433,087 | 1.81% |
| 15 | datafusion_common | 2,236,408 | 1.67% |
| 16 | arrow_ord | 1,839,549 | 1.37% |
| 17 | datafusion_sql | 1,802,445 | 1.34% |
| 18 | sail_sql_analyzer | 1,758,430 | 1.31% |
| 19 | object_store | 1,757,948 | 1.31% |
| 20 | xee_interpreter | 1,667,663 | 1.24% |

The tool groups symbols by crate-name heuristics, including monomorphized code
and an aggregated standard library. These are attributed .text contributions,
not complete package-file sizes, read-only data, or license-package counts.
Exact artifact identity removes the first attempt's profile mismatch; it does
not remove the attribution method's heuristic limits.

## Native CLI results and noise

The same retained SF1 version-0 snapshot supplies 6,001,215 rows in six files,
with 80 stored columns plus the genuine virtual INT partition column. All log
and Parquet hashes were checked against #1380. Seven query results per binary
passed full decoded value/multiplicity and executed SQL-type comparison in a
separate untimed preflight. Only that preflight adds outer ordering for stable
comparison; timed SQL remains the six exact #1380 native reference statements
plus the trivial literal. Version output also matched. Full exports remain
outside Git with hashes and receipts.

The fixed once-only schedule completed 16 warmups, 320 noise and 320 main fresh
processes. Main has 20 stock/forks-only pairs per case. Noise has ten same-binary
A/B pairs for each case on each binary. Every completed query row count matched
preflight. Process elapsed covers spawn, startup, snapshot registration, planning,
execution, complete Arrow IPC encoding to /dev/null and successful exit. The
trivial query still performs the snapshot registration required by the CLI.
Captured diagnostics are persisted after the end timestamp, and EOF-based pipe
waiting avoids regular-file timeout polling. No Cargo/profile work overlapped.

A flag requires both >5% over stock's main median and a larger absolute difference
than the maximum native A/B median difference across the two binaries. Values
use unrounded nanoseconds and strict inequalities. The noise floor is a bounded
local observation, not a confidence interval. [Native CSV](footprint-1381-native.csv)
retains every median, min/max/IQR and both noise arms; raw process data retains
all 656 identities, commands, timestamps and diagnostics.

| Case | Stock median ms | Forks median ms | Difference ms | Forks/stock | Noise floor ms | Above both |
| --- | --- | --- | --- | --- | --- | --- |
| version | 7.063019 | 7.433156 | 0.370138 | 1.052405 | 0.367338 | yes |
| trivial | 10.676740 | 11.207797 | 0.531057 | 1.049740 | 0.801319 | no |
| projection_five | 160.629592 | 160.574715 | -0.054877 | 0.999658 | 2.645815 | no |
| statistics_filter | 18.766175 | 18.605867 | -0.160308 | 0.991458 | 0.764451 | no |
| group_low | 193.004022 | 192.814855 | -0.189167 | 0.999020 | 0.976113 | no |
| count_distinct | 251.994195 | 253.831673 | 1.837478 | 1.007292 | 3.616857 | no |
| row_number | 1461.371361 | 1462.621943 | 1.250582 | 1.000856 | 2.548999 | no |
| self_join | 822.451507 | 820.559723 | -1.891784 | 0.997700 | 5.796876 | no |

`--version` is the only flag: 7.0630185 vs 7.4331560 ms, difference 0.3701375 ms,
noise floor 0.3673380 ms, margin 0.0027995 ms. Its process spreads overlap, and
it barely clears both boundaries. Keep the flag as specified; do not interpret
it as robust causal evidence against a particular fork. The trivial query is
below 5% and below its noise floor. The six real Delta query ratios range from
0.99146 to 1.00729. Nothing was retimed to change the classification, optimized,
or turned into a new performance issue.

## Tested Rust versions, Linux suites and portability

The fixed ascending matrix used ordinary full release CLI builds, without
`--ignore-rust-version`, stopping at each source's first successful build.

| Build | Tested lower failures | Lowest tested full CLI compile/link success |
| --- | --- | --- |
| stock | 1.93.0 rejected root/CLI manifest floor 1.94 | 1.94.0, built and linked |
| forks-only | 1.93.0 rejected root/CLI manifest floor 1.94 | 1.94.0, built and linked |
| linked | 1.93.0/1.94.0/1.95.0 rejected Spark manifest floor 1.96 | 1.96.0, reused successful common clean build |

Thus the forks-only closure compiled at the stock baseline's Rust 1.94 floor;
it did not inherit Spark's 1.96 requirement merely from using the forks. These
are exact tested boundaries under Cargo's declared MSRV enforcement. Lower
rejections establish manifest policy, not compiler language failures below that
policy. Compiler identities, commands, errors and successful binary hashes are
retained. No unbounded version search or clean-build timing repetition occurred.

All three Linux forks-only commands passed with `--no-fail-fast`. Named
outcomes reconcile exactly with all group summaries; no test failure names
were produced and no source fix was made.

| Linux command | Passed | Failed | Ignored | Groups |
| --- | --- | --- | --- | --- |
| core direct | 642 | 0 | 9 | 16 |
| core all features | 803 | 0 | 11 | 16 |
| CLI | 388 | 0 | 1 | 2 |

Counts are per invocation, not unique tests across commands. Full commands,
stdout/stderr and every named outcome are retained, including ignored cases.
The CLI's ignored test is
`unity_catalog::pid_one_cancellation_preserves_uc_diagnostic`. Its recorded
reason is "requires unshare and permission to create user/PID namespaces";
the test source and ignore decision are unchanged.

The one scratch CI dispatch is run
[44440596732](https://github.com/mag1cfrog/delta-arrow-reader/actions/runs/44440596732),
with event head exactly forks-only `b0c554143c686d44c7fe9897350b24a0a3cc1669`.
Both jobs succeeded on attempt 1. These are platform build/test checks; no
hosted-runner timing enters the measurements. The scratch-only workflow never
links Spark. The remote scratch ref advanced to linked only after the run
finished; the CI event remains pinned to forks-only.

| Platform | Command | Passed | Failed | Ignored | Groups |
| --- | --- | --- | --- | --- | --- |
| macOS | core direct | 623 | 0 | 9 | 16 |
| macOS | core all features | 784 | 0 | 11 | 16 |
| macOS | CLI | 353 | 0 | 0 | 2 |
| Windows | core direct | 622 | 0 | 9 | 16 |
| Windows | core all features | 783 | 0 | 11 | 16 |
| Windows | CLI | 242 | 0 | 0 | 2 |

Counts are per command, not unique tests across commands. Full job logs, exact
commands and parsed outcomes are retained. Platform-dependent tests account for
some count differences. All commands completed successfully, so no later binary
was hidden by Cargo's ordinary platform fail-fast behavior.

## Validation and retained evidence

The four frozen script self-checks passed: `native.py --check`,
`build.py --check`, `licenses.py --check` and `summarize.py --check`.
`cargo +1.96.0 fmt --all -- --check` also passed in the Spark workspace.
These checks accompany the full Linux core/CLI and macOS/Windows results above;
no product test, recorded outcome, dependency source or normal workflow changed.

The [metadata](footprint-1381-metadata.json) binds source commits, compiler/host,
authorization, original/stripped binaries, archive members, MSRV, test and
attribution receipts. The [compressed raw evidence](footprint-1381-raw.tar.gz)
retains the premeasurement authorization/freeze manifest, all clean build and
Rust-version command logs, all three Linux suite logs and named outcomes,
complete macOS/Windows job logs and receipts, both code-size diagnostic outputs,
and the authorized correction request. Its internal manifest records every
member's size and SHA-256. Independent method and numerical audits are retained
with these receipts.

[Process samples](footprint-1381-samples.jsonl.gz), the
[preflight receipt](footprint-1381-preflight.json.gz) and
[native summary](footprint-1381-native-summary.json) retain all cases and statuses.
The raw bundle also includes every process request/exit/diagnostic receipt.
Full typed preflight exports, immutable binaries, generated tables and release
archives stay outside Git at the recorded evidence paths. Their hashes and
full-value/type comparison receipts are retained.

## Python estimate and limits

Using only the CLI differences as a rough proxy, linking this path adds about
46.5 MiB to a stripped native payload and 264.1 seconds to one clean build versus
stock at two jobs. This is an estimate for possible wheel impact, not a wheel
measurement or prediction of total wheel size/build time. Wheel ABI, compression,
linking and profile behavior differ, and the full workspace's PyO3 conflict is
unresolved. No wheel/binding build or activation was performed.

These measurements cover one Linux worker, one scale/layout, one compiler for
comparisons, and the frozen cases/counts. Builds have one observation each;
source and filesystem caches are shared. Native timings include CLI transport
and warm filesystem behavior, not only engine kernels. The known-text archive qualification and heuristic symbol attribution remain
limits of these results. No shipping/adoption decision, frontend/fork
optimization, main merge or unrelated issue work is included.
