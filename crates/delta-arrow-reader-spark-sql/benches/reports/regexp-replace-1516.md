# Plain literal REGEXP_REPLACE (#1516)

The first session ran in an Ubuntu 26.10 aarch64 VM on Apple Silicon.
The replay ran on a MacBook Pro Mac14,6 with an Apple M2 Max and
macOS 26.7.3 (25G342).

Spark now compiles a scalar pattern once per live batch when position
is 1 and the scalar replacement needs no parsing. The row loop searches
for full matches and appends the prepared replacement. It still
accepts a zero-width match immediately after a nonempty match. Other
argument shapes, empty patterns and the exact nonword boundary keep
the original path.

## Paired release timings

Times are median [min, max] ns per input row above projection, with
nine samples per arm. The paired change is the median of three
candidate-minus-staging process differences. Both builds ran in the
same local Ubuntu 26.10 aarch64 VM on Apple Silicon.

| Path | Staging | Candidate | Paired change |
| --- | ---: | ---: | ---: |
| Spark ANSI off | 103.871 [102.033, 144.559] | 37.532 [37.069, 40.443] | -65.636 |
| Spark ANSI on | 107.024 [103.616, 110.592] | 38.103 [36.906, 38.544] | -68.756 |
| Native | 50.609 [48.870, 81.614] | 59.599 [49.409, 68.427] | +9.839 |

Spark improved in all six pairs, and both median reductions exceeded
the 13.717 ns per row native A/A floor. The native paired changes were
+18.544, -31.025 and +9.839 ns per row. The native control passed the
frozen rule without a layout exception. The candidate Spark medians
were below the native median on the restricted input.

## Apple Silicon, native macOS

The replay under the 2026-11-25 UTC addendum used the same two source
commits and rebuilt both release executables on this Mac. It ran
on mains power in High Power mode, with verified user-interactive QoS
on the main thread and single Tokio worker. The protocol records the
omitted Linux-only controls. [macOS protocol](../macos_addendum/regexp-replace-protocol.md)

Times are median [min, max] ns per input row above projection. The
paired change uses three process medians. Both A/A arms use this
session's staging binary for each cell.

| Path | Staging | Candidate | Paired change | Staging A/A floor | Judgment |
| --- | ---: | ---: | ---: | ---: | --- |
| Spark ANSI off | 131.822 [129.688, 132.897] | 35.613 [35.447, 35.775] | -96.298 | 1.417 | Faster |
| Spark ANSI on | 131.798 [131.289, 134.781] | 36.099 [35.588, 36.746] | -96.255 | 0.455 | Faster |
| Native | 57.402 [57.134, 58.228] | 57.911 [57.682, 58.037] | +0.625 | 0.313 | Slower |

Both Spark reductions exceeded their floors. The native paired changes
were -0.195, +0.625 and +0.754 ns per row. The source change is in
Spark's replacement adapter. A comparison of ten linked native
replacement routines covered 6,177 instructions. The remaining
operands, including stack and object field offsets, matched after
normalizing linked addresses. Four relocated loads read matching
bytes at their load widths, and 39 relocated stub calls resolved to
matching imports. The comparison leaves some linked targets and the
complete execution state unresolved, so the replay retains the native
timing judgment. [Instruction comparison](../macos_addendum/regexp-native-layout.json)

Both executables preserved all full-input rows and executed Arrow
types within each configuration. On the restricted input, native
and both Spark modes agreed on all 1,000,000 rows and executed
types across the builds. The protocol and pins were committed before
the session at `5619ec6e` and `0d737bdd`. All 36 processes completed
from 02:20:58 to 02:22:09 UTC on November 25, 2026.
[All 108 macOS samples](../macos_addendum/regexp-replace-samples.jsonl.gz)
[macOS build receipts and schedule](../macos_addendum/regexp-replace-session.json)
[macOS full-input checks](../macos_addendum/regexp-replace-full-results.json)

## Correctness and evidence

The full #1391 input matched staging on all 1,000,000 rows and executed
types in native and each Spark ANSI mode. On the restricted input, the
native reference and both Spark modes matched all rows and executed
types across the two builds. The native statement replaces only the
first match by default, so the restricted text has at most one match
per row.

The differential test checks the original capture loop directly on
sliced Utf8, LargeUtf8 and Utf8View arrays. It covers Unicode, adjacent
empty matches, NULL and empty sources, a malformed pattern on live
and all-NULL batches, group references, escapes and a different
position. The code review checked that the literal admission guard
leaves the original special cases in place. Linux formatting and
root Clippy passed. The full workspace had 3,311 passing tests and
three ignored tests.

The [protocol](../functions_1516/regexp-replace-protocol.md) was
committed at `3d31ed33` and the [build pins](../functions_1516/regexp-replace.json)
at `045f3d73` before timing. All 24 processes completed between
21:02:52 and 21:03:36 UTC on November 24, 2026.
[Full-input checks](../functions_1516/regexp-replace-full-results.json)
[All 72 samples](../functions_1516/regexp-replace-samples.jsonl.gz)

| Build | Source | Executable SHA-256 |
| --- | --- | --- |
| Staging | `b8b90b15ab12e19d0597f9fb715a18d601cfe9e8` | `ea07a55c510bf90805d43ae5284774842f9d643446487c0589225ba042c62067` |
| Candidate | `0ae21bfed8c125c6c4c6a7157b3fd6b5b3d2c227` | `719d2d095635dc1c8b5e5dd2fce36e59f74bf685159574d01b64f274afa71f0c` |

## Remaining differences in #1516

The three changes identified by the [29-case attribution](classified-functions-1516-attribution.md)
have each had one trial. The residuals below use each case's own
comparison and native noise floor.

- The approximate-percentile windows still run 209.447 to 229.931 ns
  per row above native. The scalar aggregate forms remain 5.279 to
  5.348 ns per row above native. Spark uses its discrete quantile
  summary and replays window frames. The native reference uses TDigest.
  [Scalar percentile report](scalar-percentile-1516.md)
- ARRAY_INSERT remains 6.283 / 5.685 ns per row above native in ANSI
  off / on. The Spark path retains the original length and CASE
  conditions. The paired run measures the whole expression.
  [Array insert report](array-insert-1516.md)
- The first comparison also left gaps above the native noise floor for
  aggregate REGR_AVGY (0.038 / 0.059 ns per row), aggregate REGR_SXY
  (0.055 with ANSI off), aggregate TRY_AVG (0.064 in each mode),
  window REGR_AVGX (5.699 / 4.654), and window COLLECT_SET (9.272
  with ANSI on). The source review records Spark's input metadata
  for regression, its ordered sum for TRY_AVG, and its NULL filter
  and list declaration for COLLECT_SET. The timings do not isolate
  each contribution. The six source-only cases remain recorded
  without a native comparison. [Initial report](classified-functions-1516-attribution.md)
