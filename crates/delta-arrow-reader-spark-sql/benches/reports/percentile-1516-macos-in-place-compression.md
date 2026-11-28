# In-place percentile rank compression on macOS (#1516)

MacBook Pro Mac14,6, Apple M2 Max, 32 GiB, macOS 26.7.3 (25G342).

Both aggregate aliases improve beyond their floors in both ANSI modes.
The paired improvements range from 0.658 to 0.697 ns per input row.
The affected Spark windows remain within their floors. The flagged
native window has matching instructions and state layouts.

The [aggregate profile](percentile-1516-macos-aggregate-profile.md)
measured 2.325 / 2.326 ns per row in summary compression. This candidate
retains the existing backward rank comparisons and writes survivors
into the consumed suffix in ascending order. It moves the suffix forward
and bounds capacity by the previous vector growth rule. The earlier
[rank reservation](percentile-1516-macos-rank-capacity.md) stays held.

The [protocol](../macos_rounds/1516-percentile-in-place-compression.md)
pins staging `950b020e` and candidate `4fd00e77`, both Rust 1.96.0
releases built on this Mac. Staging `161f49da` has the same product
input as the sealed #1595 binary. Both releases use the same overlay.
The session used mains power, High Power mode, `caffeinate` and verified
user-interactive QoS on the main and all benchmark worker threads.
Linux CPU pinning, the address-space limit and peak RSS were omitted.

**Apple Silicon, native macOS.** Query cost above projection in ns per
input row. Change is the median of paired candidate minus staging
process medians. The floor comes from the two staging A/A arms.

| Case | Mode | Staging | Candidate | Paired change | A/A floor | Judgment |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| `approx_percentile` aggregate | Native | 3.621 | 3.610 | -0.010 | 0.032 | not resolvable on Apple Silicon |
| `approx_percentile` aggregate | ANSI off | 8.349 | 7.701 | -0.658 | 0.279 | faster |
| `approx_percentile` aggregate | ANSI on | 8.362 | 7.681 | -0.697 | 0.386 | faster |
| `percentile_approx` aggregate | Native | 3.607 | 3.615 | -0.004 | 0.051 | not resolvable on Apple Silicon |
| `percentile_approx` aggregate | ANSI off | 8.360 | 7.698 | -0.662 | 0.358 | faster |
| `percentile_approx` aggregate | ANSI on | 8.392 | 7.690 | -0.684 | 0.514 | faster |
| `percentile_approx` window | Native | 493.572 | 498.472 | +2.648 | 5.501 | not resolvable on Apple Silicon |
| `percentile_approx` window | ANSI off | 371.920 | 372.534 | +0.826 | 1.825 | not resolvable on Apple Silicon |
| `percentile_approx` window | ANSI on | 371.886 | 373.444 | +1.941 | 2.250 | not resolvable on Apple Silicon |
| `approx_percentile` window | Native | 493.956 | 500.614 | +6.481 | 4.632 | build-layout shift |
| `approx_percentile` window | ANSI off | 372.481 | 373.488 | +1.294 | 2.760 | not resolvable on Apple Silicon |
| `approx_percentile` window | ANSI on | 371.724 | 373.292 | +1.706 | 2.052 | not resolvable on Apple Silicon |

**Apple Silicon, native macOS.** Spark minus native in ns per input row.
Each gap is the median of aligned within-block differences. The native
A/A floor applies to both gap columns.

| Case | Mode | Staging gap | Candidate gap | Native A/A floor |
| --- | --- | ---: | ---: | ---: |
| `approx_percentile` aggregate | ANSI off | +4.734 | +4.096 | 0.032 |
| `approx_percentile` aggregate | ANSI on | +4.756 | +4.087 | 0.032 |
| `percentile_approx` aggregate | ANSI off | +4.753 | +4.079 | 0.051 |
| `percentile_approx` aggregate | ANSI on | +4.760 | +4.077 | 0.051 |
| `percentile_approx` window | ANSI off | -122.005 | -124.495 | 5.501 |
| `percentile_approx` window | ANSI on | -123.107 | -125.738 | 5.501 |
| `approx_percentile` window | ANSI off | -121.475 | -126.838 | 4.632 |
| `approx_percentile` window | ANSI on | -121.612 | -129.012 | 4.632 |

The untouched native APPROX_PERCENTILE window flagged +6.481 ns per row
against a 4.632 ns floor. The two releases have the same native plan
and exact output. The comparison matched 31 linked routines and 13,049
instructions, including the timed driver, source, sort, projection,
sliding frame execution, native accumulator and TDigest. It preserved
stack and field offsets and resolved differing linked operands using
their symbol, bytes or loader relocations. All 19 matched vtable records
for six state types agree at their method offsets, sizes and alignments.
The compression edit is in Sail. The native window runs DataFusion
TDigest. Rule 5 classifies this flag as a build-layout shift.

The complete decoded rows, executed types and physical plan digests
match across releases for all four statements on both inputs, in
native and both ANSI modes. The controlled timing uses the restricted
million-row input. The focused differential compares the original rank
walk with the in-place walk on 225 ranges and accuracy combinations,
including signed zeros, infinities and NaN bits.

Formatting, strict root-package Clippy and the full Spark workspace
suite passed at `4fd00e77`. The suite completed 249 targets with
3,333 tests passed and three ignored. Nine reversed blocks completed
432 processes and retained 3,888 query/projection sample pairs.

The aggregate aliases retain 4.077 to 4.096 ns per row above native.
The remaining cost includes the ordered summary updates and rank work.
The capacity and compression trials complete the demonstrated causes
in this round. The aggregate difference stays open.

- [Exact preflight and sealed release identities](percentile-1516-macos-in-place-compression-preflight.json)
- [Paired comparisons and native gaps](percentile-1516-macos-in-place-compression-comparisons.json)
- [Native instruction and state comparison](percentile-1516-macos-in-place-compression-native-layout.json)
- [Compressed raw samples](percentile-1516-macos-in-place-compression-samples.jsonl.gz)
