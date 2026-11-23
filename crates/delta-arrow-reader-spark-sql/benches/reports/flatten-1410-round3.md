# FLATTEN without row selection on valid batches (#1410)

The staging plan checks for NULL inner lists and builds a row selection
before offset projection, even when every visible inner list is valid.
The candidate projects offsets directly for stored arrays on those
batches. A batch with a visible NULL inner list still runs the original
CASE, as do computed inputs and registered views.

| Path | Staging median [range] | Candidate median [range] | Median paired change |
| --- | ---: | ---: | ---: |
| Spark ANSI off | 25.295 [18.960, 32.070] | 3.209 [3.163, 3.505] | -22.128 |
| Spark ANSI on | 30.850 [27.130, 32.853] | 3.207 [3.168, 3.235] | -28.546 |
| Native | 2.395 [2.351, 2.436] | 2.300 [2.270, 2.433] | -0.098 |

Times are ns per input row above projection. Each range spans nine
samples. The paired change is the median of three process differences.
Every Spark pair improved, and both median drops exceeded the
0.018 ns native repeat floor. Native passed its comparison.
The remaining Spark differences against the candidate native median
are 0.909 and 0.906 ns per row. The Spark path still checks inner
validity and prepares the Spark result field around the offset projection.
This comparison does not assign the remaining time to either operation.

## Validation

Native and both ANSI modes agreed on all one million rows and the
executed Int64 and List(Int64) types across both builds. The committed
cases check all four List and LargeList width pairs, slices, empty lists,
outer and inner NULLs, NULL child values and the original result field.
A NULL parent with a nonempty range retains that offset range
before concatenation. Mixed batches with visible NULL inner lists agree
with the original CASE. Formatting, root Clippy and the full Spark
workspace passed with 3,304 tests passed and three ignored. An independent
review checked the fallback and stored-column boundary.

The [protocol](../array_functions_1410_round3/flatten-protocol.md)
was committed in `0164fafd` before attribution and the
[pins](../array_functions_1410_round3/flatten.json) in `f25c42a5`
before timing. The 24-process release comparison ended at
2026-11-23 04:48:16 UTC. [All 72 retained samples](../array_functions_1410_round3/flatten-samples.jsonl.gz)
are committed.

| Build | Source | Executable SHA256 |
| --- | --- | --- |
| Staging | `3f5c501f6850a75ce87ffddf56df46269d10551a` | `818d5fa78d850d164630b3719d0c262d719ae31c89f07d93799e9555446afd33` |
| Candidate | `b9af4664f63a368fec0e27613d4e1cbe7ec55dcb` | `9a0c239b50cbab32b9e748d8879a090fc1ba275bfa72239035f94e200a2e4c2e` |
