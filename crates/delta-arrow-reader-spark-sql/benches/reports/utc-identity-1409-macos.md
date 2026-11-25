# Literal UTC conversions on MacBook Pro Mac14,6 (#1409)

Machine: MacBook Pro Mac14,6, Apple M2 Max, macOS 26.7.3 (25G342).
This native session ran on mains power in High Power mode under the
2026-11-25 UTC addendum. The benchmark verified user-interactive QoS
on its main thread and its single Tokio worker. The protocol records
the omitted Linux-only controls. [Protocol](../macos_addendum/literal-utc-protocol.md)

## Apple Silicon, native macOS

Times are in ns per input row above projection. The staging and
candidate columns show the medians of nine retained samples. The
paired change is the median of three process-median differences.
Each A/A floor comes from two repeat arms using this staging binary.

| Literal UTC cell | ANSI | Staging | Candidate | Paired change | Staging A/A floor | Judgment |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| CONVERT_TIMEZONE | off | 57.254 | 36.231 | -20.064 | 0.884 | Faster |
| CONVERT_TIMEZONE | on | 56.871 | 36.441 | -20.789 | 0.319 | Faster |
| FROM_UTC_TIMESTAMP | off | 19.601 | 20.219 | +0.850 | 0.409 | Slower |
| FROM_UTC_TIMESTAMP | on | 19.735 | 19.673 | +0.114 | 0.172 | Within floor |
| TO_UTC_TIMESTAMP | off | 19.709 | 19.605 | -0.319 | 0.450 | Within floor |
| TO_UTC_TIMESTAMP | on | 19.495 | 19.476 | -0.086 | 0.689 | Within floor |

The candidate stays outside staging. CONVERT_TIMEZONE improved
beyond the floor in both modes, but FROM_UTC_TIMESTAMP with ANSI off
crossed its floor. Its three paired changes were -0.096, +1.008 and
+0.850 ns per row. Both sources use #1513's tagged conversion path
for this cell. The session measures the timing difference and leaves
its cause unresolved. The same gate treats the other three UTC shift
cells as within their floors.

The port applies the archived lazy CONVERT_TIMEZONE identity to the
post-#1513 source. FROM_UTC_TIMESTAMP and TO_UTC_TIMESTAMP use the
checked conversion and strict tagging introduced by #1513. The
separate [#1513 record](https://github.com/mag1cfrog/delta-arrow-reader/issues/1409#issuecomment-7150074142)
lists the previously measured costs of the fallible calendar
projection, strict tagged cast and singleton-zone collection.

## Correctness and evidence

Both release executables passed exact preflight comparisons against
the native reference on the same 1,000,000 input rows for each case.
Both ANSI modes preserved every result and executed Arrow type,
including the untagged CONVERT_TIMEZONE and tagged UTC shift types.
No case was excluded or timed outside the six scheduled cells.

The local Spark workspace ran 3,318 passing tests with three ignored.
The nine time zone integration tests and root Clippy passed after a
test-only change to how the grouping test reports an unexpected
success. The release builds used the pinned source and fixture inputs.

The protocol was committed at `337dd370`, the final pins at
`15e4e190`, and the run completed from 02:18:44 to 02:20:42 UTC
on November 25, 2026. The archived candidate includes the protocol,
source commits and test-only lint correction.
[All 216 samples](../macos_addendum/literal-utc-samples.jsonl.gz)
[Build receipts and schedule](../macos_addendum/literal-utc-session.json)
[Candidate bundle](../macos_addendum/literal-utc-candidate.bundle)
