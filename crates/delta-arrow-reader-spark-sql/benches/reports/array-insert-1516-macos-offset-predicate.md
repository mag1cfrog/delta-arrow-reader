# Offset predicate for literal second-position ARRAY_INSERT on macOS (#1516)

MacBook Pro Mac14,6, Apple M2 Max, 32 GiB, macOS 26.7.3 (25G342).

Spark improves by 44.995 / 45.001 ns per input row in the paired
ANSI off / on medians. Both improvements exceed their A/A floors.
The native control stays within its floor. Spark is now below native
by about 42 ns per row in both modes on the restricted input.

The [profile](array-insert-1516-macos-profile.md) found about 43.9 ns
per row in ARRAY_LENGTH inside the interior predicate. The candidate
compares adjacent BIGINT List offsets to build the Boolean predicate.
Parent validity carries through. The CASE still selects NULL, empty,
interior and append branches and keeps the prior value evaluation.

The [protocol](../macos_rounds/1516-array-insert-offset-predicate.md)
pins the stage release from `4fd00e77` and candidate `8ad75c1f`.
Stage `abe7ce47` has the same product inputs as the sealed stage
binary. Both native Rust 1.96.0 binaries use the same overlay.
The session used mains power, High Power mode, `caffeinate` and
verified user-interactive QoS on all benchmark threads. Linux CPU
pinning, the address-space limit and peak RSS were omitted.

**Apple Silicon, native macOS.** Query cost above projection in ns
per input row for the restricted 937,500-row input. Paired change is
the median of the within-block candidate-minus-staging differences.

| Mode | Staging | Candidate | Paired change | A/A floor | Judgment |
| --- | ---: | ---: | ---: | ---: | --- |
| Native | 71.991 | 72.373 | +0.324 | 1.427 | not resolvable on Apple Silicon |
| ANSI off | 75.351 | 30.397 | -44.995 | 0.347 | faster |
| ANSI on | 75.399 | 30.387 | -45.001 | 1.886 | faster |

**Apple Silicon, native macOS.** Paired Spark-minus-native gap
in ns per input row, using the same-build native median in each
block and the native A/A floor.

| Mode | Staging gap | Candidate gap | Native floor |
| --- | ---: | ---: | ---: |
| ANSI off | +3.175 | -41.946 | 1.427 |
| ANSI on | +3.370 | -41.999 | 1.427 |

Complete decoded rows and executed types match across releases on
the original million-row input and the restricted input in native
and both ANSI modes. The native plans are identical. In each Spark
plan, replacing the original interior length expression with the
offset predicate produces the candidate plan exactly.

The existing literal-versus-dynamic differential passed with sliced
lists, NULL parents, empty arrays, appends and interior insertions.
The existing derived-value fixture still fails with ANSI on and
returns NULL with ANSI off. Formatting, strict root-package Clippy
and the full Spark workspace suite passed at `8ad75c1f`. The suite
completed 249 targets with 3,333 tests passed and three ignored.

Nine reversed blocks completed 108 processes and retained 972
query/projection sample pairs. All processes completed.

- [Exact preflight and release identities](array-insert-1516-macos-offset-predicate-preflight.json)
- [Before and after physical plans](array-insert-1516-macos-offset-predicate-plans.json)
- [Paired comparisons and native gaps](array-insert-1516-macos-offset-predicate-comparisons.json)
- [Compressed raw samples](array-insert-1516-macos-offset-predicate-samples.jsonl.gz)
