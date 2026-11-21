# Summed view lengths in CONCAT (#1388)

The CONCAT candidate sums encoded view lengths before checking Utf8
capacity. It includes the encoded lengths of NULL slots and uses
the existing checked fold where the u32 view-count proof is unavailable.

| Shape | Path | Staging ms [min, max] | Candidate ms [min, max] | Staging ns/row | Candidate ns/row | Native floor ms |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| concat_only | native | 128.015 [127.434, 129.213] | 127.001 [126.112, 127.409] | 21.332 | 21.163 | 0.288 |
| concat_only | spark-off | 132.375 [131.348, 135.591] | 129.907 [128.581, 130.349] | 22.058 | 21.647 | 0.288 |
| concat_only | spark-on | 132.800 [131.817, 135.502] | 130.067 [129.522, 131.328] | 22.129 | 21.673 | 0.288 |
| string_functions | native | 339.436 [333.731, 344.804] | 347.391 [335.054, 352.450] | 56.561 | 57.887 | 0.685 |
| string_functions | spark-off | 328.819 [317.186, 333.465] | 333.603 [316.456, 337.069] | 54.792 | 55.589 | 0.685 |
| string_functions | spark-on | 329.308 [322.283, 342.920] | 327.788 [317.364, 342.951] | 54.874 | 54.620 | 0.685 |

## Paired differences

Candidate minus staging completion time in ms for each process pair.

| Shape | Path | Six process deltas | Median delta |
| --- | --- | --- | ---: |
| concat_only | native | -1.005, -2.452, -0.877, -2.086, -0.025, -1.043 | -1.024 |
| concat_only | spark-off | -3.974, -1.362, -2.877, -2.597, -5.288, -2.059 | -2.737 |
| concat_only | spark-on | -2.058, -2.382, -4.174, -2.717, -2.986, -2.609 | -2.663 |
| string_functions | native | +5.314, +18.719, -1.469, -1.535, +9.039, +9.485 | +7.177 |
| string_functions | spark-off | +7.693, +16.674, -1.969, -2.777, +1.552, +3.604 | +2.578 |
| string_functions | spark-on | -17.944, -4.770, +1.551, +5.062, -12.203, +20.244 | -1.610 |

## Disposition

Isolated CONCAT improved in every process pair in both ANSI modes.
The combined ANSI-off query was slower in four of six pairs, with a
2.578 ms paired median increase against a 0.685 ms native
A/B floor. The same combined native query also shifted by
7.177 ms in paired median time. The candidate changes only the
Spark CONCAT source. The session establishes the retention-rule
failure. It does not establish that the length sum caused the
combined-query increase.

Staging keeps the per-view checked subtraction. In its own arm,
isolated CONCAT remained 4.360 ms and 4.785 ms above native
with ANSI off and on respectively, against the 0.288 ms floor.
The candidate still had a 2.906-3.066 ms gap against its native
arm. The Spark path proves the original Utf8 capacity bound and
propagates NULL, and both builds share the native concat builder.

The [archived trial](https://github.com/mag1cfrog/delta-arrow-reader/tree/perf/1388-sum-view-lengths-before-the-capacity-check)
preserves the exact candidate and preclock protocol at `954b732bac0d3608ef658251235bcf07e0dbd7c5`.

## Validation

The two builds and three execution paths matched all 6,001,215 rows,
SQL and Arrow types, partitions, source file identities and exact bytes
on every timed shape. Untimed results retained their original
comparison scope across both ANSI modes.

Existing tests cover the exact Utf8 limit, one byte above it, cumulative
input bounds, encoded NULL lengths, all string storage and scalar/column
forms, nonstring delegates and reached or suppressed argument errors.

676 selected workspace tests, formatting and Spark-root Clippy passed.

The [matched protocol](../strings_round2_1388/concat-protocol.md) was committed
at `954b732bac0d3608ef658251235bcf07e0dbd7c5` before clocks.
The session ran 2026-11-21T12:51:48Z to 2026-11-21T12:53:23Z.
96 processes retained 288 timing samples.
Raw evidence is under `costs-168-2026-11-20/1388/concat`.

| Build | Source | Executable SHA256 |
| --- | --- | --- |
| staging | `8a384526316d0ce2068927d2874986d41c87c0a7` | `039f220e8e42bf74c0fa5f305d122cf72eda1f112a7e8f1e572b859189283873` |
| candidate | `ae904950d6130d3037cf18dab1a95f24134f7b5a` | `8c2804856932bd752aa7e81f7a56f01365f0c1dfba380fd14aa5d4fd7cc014ce` |
