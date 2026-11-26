# Archived physical scan renames on macOS (#1390)

MacBook Pro Mac14,6, Apple M2 Max, 32 GiB, macOS 26.7.3 (25G342).
Staging was `cd8351ee`, with the same release build inputs as
`1d74a82d`. The archived candidate was built at `e2f9d40a`.
[Committed protocol and merge rule](../macos_rounds/1390-physical-renames.md).

Hold `perf/1390-macos-physical-renames`. The candidate's change in the
400-column physical phase was within the A/A floor in both ANSI modes.
Two of 360 phase comparisons were faster. The other 358 were within
their floors, including all native controls.

**Apple Silicon, native macOS.** Physical planning in ms per statement.
Delta is the median of the nine within-block differences. The individual
arm medians can differ from the paired estimate.

| Configuration | Staging | Candidate | Paired delta | A/A floor | Decision |
| --- | ---: | ---: | ---: | ---: | --- |
| Spark ANSI off | 31.149 | 28.812 | -1.701 | 5.395 | Not resolvable on Apple Silicon |
| Spark ANSI on | 30.629 | 30.709 | -0.570 | 3.248 | Not resolvable on Apple Silicon |
| Native configured | 8.692 | 8.668 | -0.066 | 1.916 | Within floor |
| Native plain | 8.192 | 8.356 | 0.090 | 0.385 | Within floor |

The staging Spark excess over native configured was 22.026 ms with
ANSI off and 21.887 ms with ANSI on. These are paired estimates from
the same session. The physical planning gap stays open under #1390.

The candidate lowers complete, ordered aliases over a scan without
filters by binding the physical projection to the scan's ordinals.
It constructs the same `ProjectionExec` and runs the full physical
optimizer. Both builds matched 678 canonical plan traces and each
passed 120 typed result checks. The archive's differential test also
passed on this Mac against default lowering with a reordered source
projection. The [held pull request](https://github.com/mag1cfrog/delta-arrow-reader/pull/1569)
retains the product patch and its Sail provenance.

- [Preflight and build receipts](planning-1390-macos-physical-renames-preflight.json)
- [All 360 phase comparisons](planning-1390-macos-physical-renames-comparisons.json)
- [Compressed samples for 4,320 processes and 38,880 retained plans](planning-1390-macos-physical-renames-samples.jsonl.gz)
