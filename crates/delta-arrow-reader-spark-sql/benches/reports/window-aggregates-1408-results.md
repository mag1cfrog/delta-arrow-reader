# Spark sliding aggregate results (#1408)

Six of the eight paired statements meet the native noise target in both
ANSI modes. MAX_BY and MIN_BY improve, with 230-233 ns per row still
separating them from their native references. Their sliding accumulators
still update candidates for arbitrary keys. FIRST_VALUE and LAST_VALUE
read a frame position and agree on this fixture because the sort key
matches the unique comparison key. #1408 remains open for that gap.

Each before-and-after pair comes from its own matched Linux release
session. Values are median ns per input row above projection. Exact
result and type checks passed for every timed statement. The linked
reports give ranges, source pins and the committed protocols.

## Statements with a native reference

| Function | ANSI off | ANSI on | Native | Native A/A floor | Result |
| --- | ---: | ---: | ---: | ---: | --- |
| LISTAGG (plain) | 21.087 to 15.101 | 21.121 to 15.218 | 14.686 to 15.134 | 0.127 | Met [String aggregation](string-aggregation-1408.md) |
| STRING_AGG (plain) | 21.148 to 15.315 | 20.670 to 15.244 | 14.751 to 15.194 | 0.124 | Met [String aggregation](string-aggregation-1408.md) |
| ARRAY_JOIN | 1066.728 to 403.098 | 1070.007 to 409.445 | 420.675 to 414.579 | 4.421 | Met [String aggregation](string-aggregation-1408.md) |
| LISTAGG | 1065.848 to 410.168 | 1060.385 to 400.318 | 414.032 to 411.042 | 6.181 | Met [String aggregation](string-aggregation-1408.md) |
| STRING_AGG | 1071.092 to 405.431 | 1062.795 to 408.629 | 414.547 to 410.178 | 1.001 | Met [String aggregation](string-aggregation-1408.md) |
| MAX_BY | 487.594 to 382.008 | 479.747 to 382.115 | 148.482 to 148.695 | 0.860 | +233.313 to +233.420 remaining [BIGINT extrema](sliding-extrema-1408.md) |
| MIN_BY | 412.233 to 378.749 | 416.855 to 378.333 | 148.267 to 148.321 | 5.914 | +230.011 to +230.428 remaining [BIGINT extrema](sliding-extrema-1408.md) |
| PERCENTILE | 657.975 to 438.295 | 644.928 to 435.418 | 487.461 to 490.219 | 1.956 | Met [DOUBLE percentile](percentile-1408.md) |

## Spark-only statements

Single numbers are the original attribution medians for functions whose
replay remains in place. Fixed sketch settings were identified by source
inspection. The profile timed the whole factory, so it assigns no
separate measured cost to those settings.

| Function | ANSI off | ANSI on | Result |
| --- | ---: | ---: | --- |
| COUNT_MIN_SKETCH | 881.921 to 824.807 | 884.399 to 822.836 | Range lower in ANSI on [Sketch retraction](count-min-sketch-1408.md) |
| MODE | 388.811 to 358.639 | 391.345 to 357.258 | Both ranges lower [Primitive retraction](primitive-mode-1408.md) |
| HISTOGRAM_NUMERIC | 1216.915 to 914.295 | 1228.687 to 906.449 | Both ranges lower [Histogram fields](histogram-numeric-1408.md) |
| HLL_SKETCH_AGG | 428.413 | 426.222 | Replay retained. The summary discards insertion history. |
| HLL_UNION_AGG | 947.020 | 950.366 | Replay retained. The compact union discards each input sketch's contribution. |
| THETA_SKETCH_AGG | 699.762 | 700.797 | Replay retained. The compact sample discards history beyond its threshold. |
| THETA_UNION_AGG | 986.995 | 987.044 | Replay retained. The compact union drops hashes and input contributions. |
| KURTOSIS | 253.975 | 254.194 | Replay retained. Inverting the rounded fourth moment can change result bits. |
| SKEWNESS | 252.016 | 251.770 | Replay retained. Inverting rounded moments can change result bits. |
| PRODUCT | 238.046 | 237.140 | Replay retained. Division cannot undo rounding, zero, overflow or NaN. |

COUNT_MIN_SKETCH's ANSI-off ranges overlap by 3.719 ns. The lower median
there is inconclusive under its predeclared comparison. ANSI on has a
separated range.

The [attribution report](window-aggregates-1408-attribution.md) records
creation, replay, update, retraction, evaluation and output costs for
the original paths. It also explains the limits on retaining exact bits
when replay is removed.
