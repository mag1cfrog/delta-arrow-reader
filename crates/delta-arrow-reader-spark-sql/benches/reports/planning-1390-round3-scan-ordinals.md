# Ordered scan ordinals in wide planning (#1390)

The candidate uses scan ordinals for full-width, ordered aliases over
qualified scan columns with unique names. It keeps the existing
resolver for other projections and leaves scan reconstruction in place.
Both ANSI modes passed the wide-query improvement gate. The
native-configured six-table join crossed the regression gate, so the
product change stays out of staging.

## Wide planning

Timings are medians of five process medians, in milliseconds. The
projection rule times use the two observed rule bodies. Physical wall
times use the canonical builds without timing hooks.

| Configuration | Projection rule before | Projection rule trial | Physical before | Physical trial |
| --- | ---: | ---: | ---: | ---: |
| Native plain | 0.502 | 0.507 | 2.165 | 2.155 |
| Native configured | 0.519 | 0.520 | 2.316 | 2.325 |
| Spark ANSI off | 2.834 | 2.008 | 10.395 | 9.728 |
| Spark ANSI on | 2.832 | 2.003 | 10.496 | 9.648 |

The Spark physical phase and the observed projection rule improved in
all five pairs in each mode. The wide physical native A/B floor was
0.004618 ms. Staging retains 2.315 / 2.314 ms of extra projection-rule
work and 8.079 / 8.181 ms of extra physical planning, with ANSI off / on.
The candidate leaves 1.488 / 1.483 ms of extra projection-rule work.
It avoids the required-column lookups in the selected rewrite and
still reconstructs the scan and projection using the original code.

## Native join decision

Both flagged rows are from the same `stress/join_six` control. Values
are in microseconds. The ranges are disjoint and all five pair deltas
are positive for each row.

| Phase | Staging median | Candidate median | Median paired increase | Native A/B floor | Staging maximum | Candidate minimum |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Physical | 935.989 | 939.815 | 3.826 | 3.598 | 936.349 | 937.821 |
| Total | 1,063.947 | 1,066.461 | 4.419 | 1.172 | 1,065.509 | 1,065.640 |

An untimed trace of the exact release binaries records six calls to
`rewrite_projection_given_requirements` for this join in each build.
Its normalized entry instructions differ before any early return.
The compiled stack allocation grows from 1,144 to 1,288 bytes. The
checked source definitions of `RequiredIndices`, `DFSchema`, `Projection`
and `TableScan` are byte-identical. The executed routine differs, so
the rule for unchanged controls leaves the timing gate in force. The
comparison does not isolate which instruction accounts for the time shift.
No other canonical cell breached the gate.

## Validation and replay

All 678 canonical plan traces per build and the eight wide observer
traces matched exactly, including Arrow schemas, plan properties and
zero Delta data reads. The measurements did not execute queries. Linux
formatting and Spark-crate Clippy passed. The full workspace passed
3,298 tests with three ignored, including the reordered scan and short
qualifier-vector regression cases. The comparison protocol was committed
at `5ffaa0f7fad702cdac8e10a8d816515bdad67853` before timing.

- [Protocol](planning-1390-round3-scan-ordinals-protocol.md) and [all 360 comparisons](../planning_round3_1390/scan-ordinal-summary.csv).
- [50 canonical processes and 13,500 retained pairs](../planning_round3_1390/scan-ordinal-paired-samples.jsonl.gz).
- [40 wide observer processes and 360 retained plans](../planning_round3_1390/scan-ordinal-stage-samples.jsonl.gz).
- [Build receipts, trace counts, normalized instructions and state checks](../planning_round3_1390/scan-ordinal-proof.json.gz).

The [candidate bundle](../planning_round3_1390/scan-ordinal-candidate.bundle)
contains the exact product and protocol commits above staging `fe405fb8`.
In a disposable checkout, fetch the bundle's
`refs/heads/perf/1390-wide-projection-required-indices` and check out
`5ffaa0f7` to replay the trial. Both timing builds use product
`e6da2697` for the candidate and `42b02929` for staging. The staging
build inputs match `fe405fb8`.
