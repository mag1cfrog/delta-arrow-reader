# Unpadded DATE fallback on the SF1 snapshot (#1389)

Complete unpadded DATE strings take about 70% less query time with the
guarded Arrow parser. Both ANSI modes still have a measurable native gap.

| Path | Staging ms (ns/row) | Candidate ms (ns/row) | Candidate native gap ns/row |
| --- | ---: | ---: | ---: |
| Native | 267.005 (44.492) | 267.695 (44.607) | |
| ANSI off | 974.357 (162.360) | 293.188 (48.855) | 4.248 |
| ANSI on | 978.568 (163.062) | 292.285 (48.704) | 4.098 |

The candidate's native A/B repeat spread is 0.186 ns per row. Each cell
uses 6,001,215 rows per execution and the median of three process medians.
The [protocol](../date_1389/comparison.md) and
[90 samples](../date_1389/query-completion.csv.gz) cover the 30-process
same-session comparison. All seven preflight comparisons matched complete
decoded results, Arrow and SQL types, and source I/O. No shapes were excluded.

## Remaining cost

The guarded route checks the complete short date before calling Arrow.
Arrow's parser accepts additional input forms, so Spark needs that
validation at this entry point. A [paired probe of the exact shape check](../date_1389/shape-check-samples.csv)
found a median cost of 2.366 ns per row (MAD 0.044). All seven pairs
showed a cost. That explains part of the 4.098 to 4.248 ns per row
remaining against native. The probe omits array assembly and query
execution, so it cannot account for the full gap.

The [attribution replay](date-fallback-1389-attribution.md) also establishes
that the historical reuse slowdown did not recur when isolated. Those
separate samples do not enter this candidate's timing comparison.

## Build identities

| Build | Git source | Executable SHA-256 |
| --- | --- | --- |
| Staging-equivalent | `a2ca19d3548caf3f749998bb1c1cc75dd8d2e7f0` | `d64696e29d6b4ea02ae65b54e056af5f1a240b89565ecebcc6ca185c42578b32` |
| Candidate | `7e027c72ac4e6595d3c62996ee92fc78fb4c894c` | `6e879737e02d1022717e2725e25512df1bd6fd87b38d5676d4646fbd50e7c73b` |

The reused before executable matches the product, execution benchmark
and frozen workload sources of staging
`124246cbc61f6b7893df713d18f397bf37e74b6c`. The candidate
switches complete ASCII dates with a one-digit month or day to the existing
Arrow DATE32 parser. The same Sail parser and CAST/TRY_CAST error handling
apply when the guard or Arrow's calendar validation declines an input.
