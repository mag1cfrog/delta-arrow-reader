# Sliding BIGINT MAX_BY and MIN_BY result (#1408)

Retaining candidates as the window moves cut the measured MAX_BY cost by
20-22% and MIN_BY by 8-9%. Both still exceed the native noise target.

Times are median [min-max] ns per input row above projection. Each arm
retained nine samples across three processes. The Spark delta uses the
candidate native median. The A/A floor uses native process medians.

| Function | Path | Staging | Candidate | Spark delta | Native A/A floor |
| --- | --- | ---: | ---: | ---: | ---: |
| MAX_BY | native | 148.482 [146.218-150.570] | 148.695 [147.374-151.715] | n/a | 0.860 |
| MAX_BY | ANSI off | 487.594 [471.898-493.565] | 382.008 [380.300-387.487] | +233.313 | 0.860 |
| MAX_BY | ANSI on | 479.747 [476.279-495.080] | 382.115 [378.349-391.418] | +233.420 | 0.860 |
| MIN_BY | native | 148.267 [146.330-150.916] | 148.321 [146.630-151.990] | n/a | 5.914 |
| MIN_BY | ANSI off | 412.233 [404.606-420.981] | 378.749 [374.965-381.852] | +230.428 | 5.914 |
| MIN_BY | ANSI on | 416.855 [408.059-420.499] | 378.333 [375.984-383.547] | +230.011 | 5.914 |

The sliding accumulator replaces a fresh scan of the frame with incremental
updates and retractions. It retains the earliest row among tied keys, including
rows whose value is NULL, and discards candidates when they expire. The queue
can hold all active rows if every key ties. Other types and ordinary aggregates
use the original accumulator.

The remaining measured gaps are 230-233 ns per row. The Spark window still
slices both inputs and maintains candidates for arbitrary keys during each
frame move. Native FIRST_VALUE and LAST_VALUE read one position in the frame.
They give the same answers on this fixture because its unique key orders the
frame. The original phase profile identifies this difference in work.
These totals do not assign phase timings to the new accumulator.

## Validation and reproduction

The exact preflight matched each million-row result by ID and executed
types on both builds, both ANSI modes and native. The 24 committed staging
observations cover filtered rows, key ties, NULL keys and values, BIGINT
extremes, empty input and frames, ROWS, RANGE, growing and whole-partition
frames, ordinary aggregation and each non-BIGINT fallback. A separate
accumulator test compared results and state across frame moves. Both tests,
workspace formatting and root Clippy passed.

The [protocol](../window_aggregates_1408/max-min-by-protocol.md) was committed
at `311f1b2aeb5ce36d5e6f542f869c28cb227bbbbc` before clocks. The single Linux
release session ran 2026-11-21 01:26:53 to 01:28:51 UTC. All 36 comparison
and 12 native A/A processes completed, retaining 144 query/projection pairs.
Local evidence is under `costs-168-2026-11-20/1408/max-min-by`.

| Build | Source | Executable SHA256 |
| --- | --- | --- |
| staging | `ee5ea2b31d675524ffd7d1d6d3371dc57c4bd2a3` | `f8e1d4e59f16c5787eae95b66ba23fe58fa07303c52c99c73f17f1f2568823ae` |
| candidate | `09a08d17d4706e80920eb54dc5809288e93d0db8` | `20e7b7b5d82edf568cc4a09a75be59ab078accd5076b18de2d973d6a7f5f52ea` |
