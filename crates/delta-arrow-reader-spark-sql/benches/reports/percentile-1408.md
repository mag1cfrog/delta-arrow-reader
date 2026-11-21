# Unweighted DOUBLE percentile result (#1408)

The measured Spark percentile cost fell by 32-33%. Both ANSI modes met
the native noise target. The typed path skips a same-type Arrow cast and
frequency validation for batches whose rows each have weight one. It
retains the count map, update order and interpolation.

Times are median [min-max] ns per input row above projection, from nine
samples per arm. The Spark delta uses the candidate native median. The
same-session native process-median A/A floor was 1.956 ns.

| Path | Staging | Candidate | Spark delta |
| --- | ---: | ---: | ---: |
| native QUANTILE_CONT | 487.461 [486.660-492.933] | 490.219 [485.555-493.558] | n/a |
| Spark ANSI off | 657.975 [641.673-667.479] | 438.295 [430.002-453.244] | -51.924 |
| Spark ANSI on | 644.928 [635.996-653.368] | 435.418 [428.362-437.024] | -54.801 |

The preflight matched all one million result rows and the executed Int64
and Float64 types for both builds and all three paths. The comparison used
exact Float64 bits. The accumulator tests compare implicit and explicit
unit frequencies with signed zeros, infinities, distinct NaN payloads,
NULLs and empty frames. The query test compares exact results and
array-of-double outputs for ordinary aggregation, empty input, and ROWS,
RANGE, growing, whole-partition and empty windows in both ANSI modes.
The 640 local workspace library and percentile integration tests passed,
as did formatting and root Clippy.

The [protocol](../window_aggregates_1408/percentile-protocol.md) was committed
at `d1f06df58a2997212863bb774adee4c4736be6b2` before clocks. The single
Linux release session ran 2026-11-21 01:44:46 to 01:46:10 UTC. All 18
comparison and six native A/A processes completed, retaining 72
query/projection pairs. Local evidence is under
`costs-168-2026-11-20/1408/percentile`.

| Build | Source | Executable SHA256 |
| --- | --- | --- |
| staging | `09a08d17d4706e80920eb54dc5809288e93d0db8` | `20e7b7b5d82edf568cc4a09a75be59ab078accd5076b18de2d973d6a7f5f52ea` |
| candidate | `066c8d1504bf8d8900cd4f09131ad22b821b25ed` | `52e2f8feb1b77f66d4dc5065744d78a5ecd28be1c2a04e342a99c6c8b89caccb` |
