# HISTOGRAM_NUMERIC bin field result (#1408)

Reusing the planned output fields lowered the median by 303-322 ns per
input row, or 25-26%. The retained ranges are separated in both ANSI
modes. There is no native equivalent for this comparison.

Times are median [min-max] ns per input row above projection, using nine
samples per arm from the same session.

| Path | Staging | Candidate | Median change |
| --- | ---: | ---: | ---: |
| Spark ANSI off | 1216.915 [1212.851-1238.645] | 914.295 [903.071-929.741] | -302.619 |
| Spark ANSI on | 1228.687 [1223.070-1234.041] | 906.449 [900.718-915.727] | -322.239 |

The accumulator shares the Arrow list item and bin fields with its
prepared return type. If a physical rewrite changes the center type,
it builds the original layout from the actual input type. Bin updates,
trimming, random tie choices, casts and replay still use the original
implementation.

Preflight matched one million result rows by ID, all Float64 bits and
the executed list and struct types across both builds and ANSI modes.
The committed corpus compares 18 staging result sets and six errors.
It checks signed zero, NULLs, input and center types, empty inputs, bin
counts and sliding ROWS, RANGE, growing, whole and empty
frames. A unit test covers the fallback with a retained Float64 return
type and a rewritten BIGINT input. All 637 local library and integration
tests passed, along with formatting and root Clippy.

The [protocol](../window_aggregates_1408/histogram-numeric-protocol.md)
was committed at `d3448272413622432cc8822a7177d1987310da32` before clocks.
The Linux release session ran 2026-11-21 02:35:10 to 02:36:29 UTC.
All 12 processes completed, retaining 36 query/projection pairs.
Local evidence is under `costs-168-2026-11-20/1408/histogram`.

| Build | Source | Executable SHA256 |
| --- | --- | --- |
| staging | `9c6c9551c01987b774440b47816f84b394444925` | `532be2aa96b3bde6b6bb0cea091e0823380459191bf2a560d38a5669d5c86614` |
| candidate | `ab35133d30ef6cd3170a724335770d75fd52cc74` | `72e9f124c417e048ae0762bc72d794818285d1cd8b60312a95991bc9c1c2aca0` |
