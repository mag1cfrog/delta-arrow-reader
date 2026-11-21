# Inline ASCII SUBSTRING prefixes (#1388)

The Spark prefix path uses the Arrow view builder when a short prefix
contains only ASCII. A prefix that needs Unicode decoding, a longer
view, a scalar or another typed input uses its existing kernel.

| Shape | Path | Staging ms [min, max] | Candidate ms [min, max] | Staging ns/row | Candidate ns/row | Native floor ms |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| substring_only | native | 189.702 [179.808, 193.319] | 181.944 [177.864, 201.821] | 31.611 | 30.318 | 6.898 |
| substring_only | spark-off | 188.222 [171.950, 203.031] | 160.669 [152.257, 170.108] | 31.364 | 26.773 | 6.898 |
| substring_only | spark-on | 191.827 [177.206, 201.661] | 159.868 [152.991, 168.039] | 31.965 | 26.639 | 6.898 |
| string_functions | native | 342.947 [336.794, 351.354] | 342.919 [334.610, 349.886] | 57.146 | 57.142 | 2.024 |
| string_functions | spark-off | 352.685 [338.262, 359.452] | 326.368 [322.933, 343.377] | 58.769 | 54.384 | 2.024 |
| string_functions | spark-on | 351.941 [340.759, 367.510] | 326.337 [321.507, 331.480] | 58.645 | 54.378 | 2.024 |

## Paired differences

Candidate minus staging completion time in ms for each process pair.

| Shape | Path | Six process deltas | Median delta |
| --- | --- | --- | ---: |
| substring_only | native | +2.477, +13.420, +8.501, -11.704, +0.693, -14.056 | +1.585 |
| substring_only | spark-off | -26.394, -3.633, -27.395, -32.909, -49.401, -21.171 | -26.895 |
| substring_only | spark-on | -29.594, -37.167, -40.401, -9.167, -37.268, -36.152 | -36.659 |
| string_functions | native | -1.947, +3.181, -11.803, +7.522, +8.435, -12.071 | +0.617 |
| string_functions | spark-off | -9.706, -9.848, -29.212, -35.876, -29.380, -10.430 | -19.821 |
| string_functions | spark-on | -35.857, -42.524, -23.388, -20.998, -19.251, -22.402 | -22.895 |

## Disposition

All six Spark process pairs improved on each shape and in both
ANSI modes. The short-prefix and combined queries meet their
native targets. The native process pairs varied in both directions.

The timing covers the SF1 prefixes. The short-prefix guard and
the existing Unicode fallback were checked for correctness.
Unicode fallback throughput is outside this campaign.

## Validation

The two builds and three execution paths matched all 6,001,215 rows,
SQL and Arrow types, partitions, source file identities and exact bytes
on every timed shape. Untimed results retained their original
comparison scope across both ANSI modes.

The new direct comparison checks short, empty and long ASCII inputs,
NULLs, and multibyte prefixes and suffixes against the previous
delegate. Existing storage, bounds and lazy argument tests also passed.

663 selected workspace tests, formatting and Spark-root Clippy passed.

The [matched protocol](../strings_round2_1388/substring-protocol.md) was committed
at `dfbb11251b1afd3f20bb448dbce449ac65a5a238` before clocks.
The session ran 2026-11-21T12:38:15Z to 2026-11-21T12:40:02Z.
96 processes retained 288 timing samples.
Raw evidence is under `costs-168-2026-11-20/1388/substring`.

| Build | Source | Executable SHA256 |
| --- | --- | --- |
| staging | `2bd5002527a7156789115abffb440b96d8af2dc1` | `cb83cc7b345f40d81c6954889a71d8360637fb226799de0957a74c418da1eded` |
| candidate | `8a384526316d0ce2068927d2874986d41c87c0a7` | `039f220e8e42bf74c0fa5f305d122cf72eda1f112a7e8f1e572b859189283873` |
