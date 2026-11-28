# Exact percentile rank buffer capacity on macOS (#1516)

MacBook Pro Mac14,6, Apple M2 Max, 32 GiB, macOS 26.7.3 (25G342).

Hold the candidate. The paired aggregate changes range from -0.336
to -0.362 ns per input row against floors of 0.837 to 0.903 ns.
The improvement is not resolvable on Apple Silicon. The staging
aggregate gap is still about 4.8 ns per row in this session.

The [aggregate attribution](percentile-1516-macos-aggregate-profile.md)
counted 19 buffer flushes that grow an empty rank vector. This trial
reserves the exact combined number of existing and pending ranks
before the original merge loop. Both Spark window aliases also run
the buffer code and were timed as affected controls.

The [protocol](../macos_rounds/1516-percentile-rank-capacity.md)
pins staging release `950b020e` and candidate release `6f8a1754`.
The staging release has the same product input as `2919bcca`.
Both native Rust 1.96.0 builds use the same measurement overlay.
The session used mains power, High Power mode, `caffeinate` and
verified user-interactive QoS on all benchmark threads. Linux CPU
pinning, the address-space limit and peak RSS were omitted.

**Apple Silicon, native macOS.** Query cost above projection in ns per
input row. Change is the median of the per-block differences.

| Case | Mode | Staging | Candidate | Paired change | A/A floor |
| --- | --- | ---: | ---: | ---: | ---: |
| `approx_percentile` aggregate | Native | 3.608 | 3.618 | +0.014 | 0.329 |
| `approx_percentile` aggregate | ANSI off | 8.377 | 8.015 | -0.336 | 0.901 |
| `approx_percentile` aggregate | ANSI on | 8.387 | 8.045 | -0.362 | 0.837 |
| `percentile_approx` aggregate | Native | 3.634 | 3.607 | -0.024 | 0.363 |
| `percentile_approx` aggregate | ANSI off | 8.389 | 8.025 | -0.353 | 0.855 |
| `percentile_approx` aggregate | ANSI on | 8.404 | 8.055 | -0.345 | 0.903 |
| `percentile_approx` window | Native | 495.631 | 493.918 | -2.790 | 46.145 |
| `percentile_approx` window | ANSI off | 372.738 | 371.856 | -0.540 | 19.474 |
| `percentile_approx` window | ANSI on | 373.004 | 372.872 | +1.575 | 21.592 |
| `approx_percentile` window | Native | 496.519 | 493.440 | -4.399 | 26.472 |
| `approx_percentile` window | ANSI off | 371.938 | 374.053 | +1.181 | 31.232 |
| `approx_percentile` window | ANSI on | 372.523 | 372.707 | -1.445 | 35.661 |

All twelve build-to-build changes were within their floors.

**Apple Silicon, native macOS.** Spark minus native in ns per input row.
The gap is the median of aligned Spark-minus-native process medians.
The native A/A floor applies to both gap columns.

| Case | Mode | Staging gap | Candidate gap | Native A/A floor |
| --- | --- | ---: | ---: | ---: |
| `approx_percentile` aggregate | ANSI off | +4.770 | +4.406 | 0.329 |
| `approx_percentile` aggregate | ANSI on | +4.768 | +4.410 | 0.329 |
| `percentile_approx` aggregate | ANSI off | +4.769 | +4.429 | 0.363 |
| `percentile_approx` aggregate | ANSI on | +4.793 | +4.456 | 0.363 |
| `percentile_approx` window | ANSI off | -122.363 | -122.115 | 46.145 |
| `percentile_approx` window | ANSI on | -122.867 | -121.658 | 46.145 |
| `approx_percentile` window | ANSI off | -124.428 | -117.954 | 26.472 |
| `approx_percentile` window | ANSI on | -123.741 | -120.355 | 26.472 |

The full and restricted million-row fixtures have identical complete
outputs, executed types, source guards and physical plan digests
across releases for all four cases in native and both ANSI modes.
The restricted Spark and native outputs also match. Only the restricted
fixture enters this timing session.

Formatting, strict root-package Clippy, the six focused percentile
tests and the full Spark workspace suite passed at candidate
`6f8a1754`. The suite completed 249 targets with 3,332 tests passed
and three ignored. The held branch is
`perf/1516-macos-percentile-rank-capacity` ([#1597](https://github.com/mag1cfrog/delta-arrow-reader/pull/1597)).

Every process completed across the nine reversed blocks. The archive
has 432 process records and 3,888 retained query/projection pairs.

- [Exact preflight and sealed release identities](percentile-1516-macos-rank-capacity-preflight.json)
- [Paired comparisons and native gaps](percentile-1516-macos-rank-capacity-comparisons.json)
- [Compressed raw samples](percentile-1516-macos-rank-capacity-samples.jsonl.gz)
