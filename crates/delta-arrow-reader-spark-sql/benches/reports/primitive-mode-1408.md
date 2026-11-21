# Sliding primitive MODE result (#1408)

The candidate lowered the median by 30-34 ns per input row, or 8-9%.
The retained ranges are separated in both ANSI modes. There is no native
equivalent for this comparison.

Times are median [min-max] ns per input row above projection. Each arm
retained nine query/projection pairs across three processes.

| Path | Staging | Candidate | Median change |
| --- | ---: | ---: | ---: |
| Spark ANSI off | 388.811 [385.104-399.550] | 358.639 [356.623-367.386] | -30.172 |
| Spark ANSI on | 391.345 [389.057-395.238] | 357.258 [353.219-358.062] | -34.087 |

The primitive accumulator decrements frequencies for departing values.
Zero counts leave the map, and the existing evaluation still applies
the tie rule to the current counts. The float and string accumulators
retain their existing behavior.

Preflight matched each of the million BIGINT values by ID and the
executed types across builds and ANSI modes. The 24 committed staging
observations include 20 result sets and four option errors. They cover
ties, NULLs, input extremes, the typed fallbacks, filtered and partitioned
frames, ROWS, RANGE, growing, whole-partition and empty frames, and plain
aggregation with and without input. The unit test compares the live
frequency map and its result with a replay. Local library and integration
tests passed (636 tests), along with formatting and root Clippy.

The [protocol](../window_aggregates_1408/primitive-mode-protocol.md) was
committed at `10f4cd6b251859006db49a3e4eb03fd684f021e1` before clocks.
The Linux release session ran 2026-11-21 02:21:46 to 02:22:24 UTC.
All 12 processes completed, retaining 36 query/projection pairs.
Local evidence is under `costs-168-2026-11-20/1408/mode`.

| Build | Source | Executable SHA256 |
| --- | --- | --- |
| staging | `e2dcd44a1085538d9515f786c779703e61bc2b4b` | `2996198a632a5bd495fce420e2405586c01f05dfcda41419341fd87d14165068` |
| candidate | `9c6c9551c01987b774440b47816f84b394444925` | `532be2aa96b3bde6b6bb0cea091e0823380459191bf2a560d38a5669d5c86614` |
