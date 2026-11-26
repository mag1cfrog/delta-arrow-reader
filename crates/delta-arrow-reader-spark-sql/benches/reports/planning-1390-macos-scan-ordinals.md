# Archived scan ordinals on macOS (#1390)

MacBook Pro Mac14,6, Apple M2 Max, 32 GiB, macOS 26.7.3 (25G342).
The session paired staging `3c6c0a53` and candidate `1d74a82d`.
[Committed protocol and merge rule](../macos_rounds/1390-scan-ordinals.md).

The 400-column physical planning phase improved beyond the floor with
ANSI on. The ANSI-off change was within its floor. Across all 360
phase comparisons, five were faster and 355 were within their floors.
No native control crossed a slower gate.

**Apple Silicon, native macOS.** Physical planning in ms per statement.
Delta is the median of the nine within-block differences. The individual
arm medians can differ from that paired estimate.

| Configuration | Staging | Candidate | Paired delta | A/A floor | Decision |
| --- | ---: | ---: | ---: | ---: | --- |
| Spark ANSI off | 34.674 | 31.733 | -2.563 | 4.933 | Not resolvable on Apple Silicon |
| Spark ANSI on | 34.282 | 30.884 | -3.415 | 3.255 | Faster |
| Native configured | 8.631 | 8.610 | -0.0004 | 2.662 | Within floor |
| Native plain | 8.113 | 8.104 | 0.150 | 1.849 | Within floor |

The paired Spark excess over native configured was 25.664 ms before
and 23.060 ms after with ANSI off. With ANSI on it was 25.500 ms before
and 22.267 ms after. The remaining physical planning gap stays open
under #1390.

The archived DataFusion change uses scan ordinals for a complete,
ordered projection of at least 64 aliases over uniquely named columns.
Every alias must refer to the qualified scan field at that position.
The check accounts for the full field and qualifier lists before it
reuses the required ordinals. The previous lookup handles partial,
reordered, or otherwise ineligible projections. Both paths use the
existing scan and projection reconstruction. The fork provenance is
recorded in [UPSTREAM.md](../../UPSTREAM.md).

Both builds matched 678 canonical plan traces and each passed 120
typed result checks. The new differential test compares the optimizer
with the original ordered lookup, including a reversed source
projection and a shortened qualifier list. Both the new test and the
previous projection lookup tests passed on this Mac.

- [Preflight and build receipts](planning-1390-macos-scan-ordinals-preflight.json)
- [All 360 phase comparisons](planning-1390-macos-scan-ordinals-comparisons.json)
- [Compressed samples for 4,320 processes and 38,880 retained plans](planning-1390-macos-scan-ordinals-samples.jsonl.gz)
