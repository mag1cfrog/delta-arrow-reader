# DATE fallback parser attribution (#1389)

Restoring only the old DATE reuse code made the same SF1 query faster on
current staging. The earlier 97 ms ANSI-off and 91 ms ANSI-on slowdowns
did not recur. Reuse still leaves about 103 ns per row above native.
The earlier campaigns varied at least in collection time and the other
product changes in the candidate. The original regression's cause remains
undetermined.

| Paired SF1 DATE fallback | Current ms | Restored reuse ms | Difference ms |
| --- | ---: | ---: | ---: |
| ANSI off | 974.635 | 888.169 | -86.467 |
| ANSI on | 976.428 | 885.526 | -90.902 |
| Native | 266.821 | 270.264 | 3.443 |

All three Spark process pairs improved in each ANSI mode. The only source
changes in the scratch build are the historical `spark_date.rs` and
`date_parser` factory. The 90 samples from 30 processes used the same
snapshot and schedule. All seven complete preflight comparisons passed
over 6,001,215 rows, with equal executed types and source I/O.

## Isolated DATE work

| Case | Median ns/row | Median absolute deviation ns/row |
| --- | ---: | ---: |
| Fresh parser in array loop | 217.213 | 1.275 |
| Lazy reused parser in array loop | 206.539 | 0.986 |
| Fresh parser | 206.386 | 1.156 |
| Lazy reused parser | 193.300 | 0.570 |
| Eager reused parser | 191.900 | 1.226 |
| Parser materialization control | 1.824 | 0.008 |
| Calendar conversion control | 11.115 | 0.093 |

The isolated lazy traces also show lower medians, with six of seven paired
samples faster in each array/parser-only case. Eager reuse is faster than
fresh parsing in all seven. The controls identify parsing the fallback
`YYYY-M-D` strings as the large remaining operation. The measurements
do not add up to whole-query latency because the driver isolates
different operations and the compiler can specialize their call sites.

The candidate uses Arrow's existing short DATE parser for complete
unpadded dates, with a full-input guard against Arrow's additional accepted
formats. The original Sail grammar still handles other inputs.

## Reproduction

The [protocol](../date_1389/attribution.md), [isolated phase
samples](../date_1389/phases.csv) and [whole-query replay
samples](../date_1389/reuse-query-completion.csv.gz) record the two bounded
experiments.

| Build | Git source | Executable SHA-256 |
| --- | --- | --- |
| Current | `a2ca19d3548caf3f749998bb1c1cc75dd8d2e7f0` | `d64696e29d6b4ea02ae65b54e056af5f1a240b89565ecebcc6ca185c42578b32` |
| Restored | `1a23835590ebda9c68b00572e3137c848c65366c` | `0f58e10e7219c7d294868fdb91c406eacc3908c615927e6a9220ad5f7b563236` |

The current release executable was built for the interval comparison.
Its product, workspace lockfile, execution benchmark and workload bytes
match staging `124246cbc61f6b7893df713d18f397bf37e74b6c`. The restored
source is reproducible using the two-source-file replacement described
in the protocol.
