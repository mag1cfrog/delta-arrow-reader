# Exact small percentile window results on macOS (#1516)

MacBook Pro Mac14,6, Apple M2 Max, 32 GiB, macOS 26.7.3 (25G342).

Both window aliases are faster beyond their A/A floors in both ANSI
modes. All aggregate and native controls are within their floors.
The measured window costs are also below their native references.
The aggregate gap remains open for separate attribution.

The [committed protocol](../macos_rounds/1516-exact-percentile-frame.md)
pins the native Rust 1.96.0 releases at staging `e9f5ce76` and candidate
`950b020e`. Current staging `7bd35f35` adds only benchmark scripts and
evidence over the sealed staging release. Both releases use identical
harness overlays. The session used mains power, High Power mode,
`caffeinate` and verified user-interactive QoS on main and worker threads.
Linux CPU pinning, the address-space limit and peak RSS were omitted.

**Apple Silicon, native macOS.** Query cost above projection in ns per
input row. Change is the median of per-block candidate minus staging
differences. A/A floors use both staging arms for each row of the table.

| Case | Mode | Staging | Candidate | Paired change | A/A floor | Judgment |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| `percentile_approx` window | Native | 494.901 | 494.246 | -0.335 | 19.780 | not resolvable on Apple Silicon |
| `percentile_approx` window | ANSI off | 683.760 | 372.499 | -312.340 | 22.063 | faster |
| `percentile_approx` window | ANSI on | 684.358 | 372.986 | -310.225 | 16.355 | faster |
| `approx_percentile` window | Native | 495.755 | 495.164 | -1.220 | 17.959 | not resolvable on Apple Silicon |
| `approx_percentile` window | ANSI off | 682.322 | 372.377 | -310.402 | 18.607 | faster |
| `approx_percentile` window | ANSI on | 682.209 | 372.296 | -310.540 | 13.334 | faster |
| `approx_percentile` aggregate | Native | 3.608 | 3.607 | +0.007 | 0.154 | not resolvable on Apple Silicon |
| `approx_percentile` aggregate | ANSI off | 8.339 | 8.363 | +0.019 | 0.460 | not resolvable on Apple Silicon |
| `approx_percentile` aggregate | ANSI on | 8.353 | 8.346 | -0.007 | 0.207 | not resolvable on Apple Silicon |
| `percentile_approx` aggregate | Native | 3.615 | 3.621 | +0.020 | 0.044 | not resolvable on Apple Silicon |
| `percentile_approx` aggregate | ANSI off | 8.334 | 8.346 | -0.001 | 0.245 | not resolvable on Apple Silicon |
| `percentile_approx` aggregate | ANSI on | 8.370 | 8.344 | +0.000 | 0.112 | not resolvable on Apple Silicon |

**Apple Silicon, native macOS.** Spark minus native in ns per input row.
Subtract same-build native from Spark within each block, then take
the median. The native A/A floor applies to both gap columns.

| Case | Mode | Staging gap | Candidate gap | Native A/A floor | Candidate judgment |
| --- | --- | ---: | ---: | ---: | --- |
| `percentile_approx` window | ANSI off | +187.041 | -122.513 | 19.780 | Spark faster |
| `percentile_approx` window | ANSI on | +187.210 | -121.563 | 19.780 | Spark faster |
| `approx_percentile` window | ANSI off | +185.497 | -122.787 | 17.959 | Spark faster |
| `approx_percentile` window | ANSI on | +187.073 | -123.934 | 17.959 | Spark faster |
| `approx_percentile` aggregate | ANSI off | +4.741 | +4.760 | 0.154 | Spark slower |
| `approx_percentile` aggregate | ANSI on | +4.755 | +4.744 | 0.154 | Spark slower |
| `percentile_approx` aggregate | ANSI off | +4.754 | +4.733 | 0.044 | Spark slower |
| `percentile_approx` aggregate | ANSI on | +4.741 | +4.735 | 0.044 | Spark slower |

The [attribution](percentile-1516-macos-profile.md) counted 979,520
moving-frame replays on the restricted million-row input. There were
4,591,500 non-NULL summary entries from the replays and 4,610,700
entries including initial frame growth. This change caches a result
only after a fresh evaluation of the same ordered non-NULL Float64
bits. It rebuilds the original summary for a different sequence and
uses executor replay after an untracked frame. Other types and
nonliteral or array percentile arguments use the prior accumulator.

The result audit matched complete decoded Float64 bits, executed types
and row counts across releases for all four statements in native and
both ANSI modes. Both the unrestricted and restricted fixtures have
one million input rows. The restricted Spark and native outputs also
match. The unrestricted fixture is used for cross-release correctness
only because the frontends compute different percentile definitions.
The preflight includes equal physical plan digests for each mode.

The new focused test initially used an array-result helper to exercise
the scalar path. Its corrected scalar fixture and five existing
percentile tests passed. The correction at `01c17d52` is under
`#[cfg(test)]`, which is excluded from the release. Formatting and
strict root-package Clippy passed on the corrected test input.

The run completed 432 processes and retained 3,888 query/projection
sample pairs across nine reversed blocks. No observation was excluded.
The paired medians, floors and gaps below were checked against the raw
records. The table applies only to the committed restricted input.

- [Exact preflight and sealed release identities](percentile-1516-macos-exact-preflight.json)
- [Machine-readable paired comparisons](percentile-1516-macos-exact-comparisons.json)
- [Compressed raw samples](percentile-1516-macos-exact-samples.jsonl.gz)
