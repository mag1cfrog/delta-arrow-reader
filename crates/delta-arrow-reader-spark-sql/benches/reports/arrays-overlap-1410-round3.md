# Direct ARRAYS_OVERLAP from stored BIGINT columns (#1410)

On the fixed case, the Spark cost falls from 83 to 8 ns per
input row in both ANSI modes. Both column operands go directly
to the typed membership kernel, which reads parent validity,
offsets and child validity. The prepared expression keeps the
result field. Computed inputs and registered views keep their
existing operand preparation and conditions.

| Path | Staging median [range] | Candidate median [range] | Median paired change |
| --- | ---: | ---: | ---: |
| Spark ANSI off | 82.926 [82.650, 84.650] | 7.975 [7.927, 8.058] | -74.922 |
| Spark ANSI on | 83.318 [83.027, 84.799] | 7.952 [7.914, 8.058] | -75.369 |
| Native | 17.346 [16.978, 21.115] | 17.695 [17.527, 18.109] | +0.349 |

Times are ns per input row above projection. Each range spans nine
samples, and the paired change is the median of three
candidate-minus-staging process differences. All Spark pairs
improved. Both median reductions exceeded the native A/A floor
of 0.092 ns per row. The fixed Spark case is now below native.

## Attribution

The staging plan prepares a nested result cast, evaluates
array length and applies the boolean guards before returning the
BIGINT kernel's value. The native plan calls `array_has_any`
directly. The attribution run captured both Spark routines,
including three stack hits each for length and membership.
It emitted all three retained samples, a successful case summary,
and the completion marker before LLDB lost the exit connection.
The candidate plans use the BIGINT kernel without length or the
nested result cast. The archived profile captures the paths
and the paired release run measures their cost.

## Native control

The paired changes were +0.207, +0.349 and +0.480 ns per row.
They cross the timing gate. The untimed trace selected the same
native UDF, membership body and three Arrow row converter
routines in the measured builds, through the projection.
Nine native kernel and physical-plan bodies and three scalar
dispatcher bodies match after normalizing linked addresses.
The ArrayHasAny and ProjectionExec
state layouts match at 96 and 104 bytes, respectively, with
8-byte alignment. Relocated routine and data references match
after comparing the loaded literal width and linked data.
The [layout proof](../array_functions_1410_round3/arrays-overlap-native-layout.json.gz)
supports applying the predeclared build-layout rule.

## Validation and reproduction

All three modes matched a million rows and the executed
Int64 and Boolean types across both builds. The direct and
original paths agree for List and LargeList slices, NULL parents
and children, empty rows, both indexed membership directions
and BIGINT limits. The derived-operand error tests passed.
Formatting, root Clippy and the full Spark workspace passed,
with 3,302 tests passed and three ignored. An independent review
verified the scan boundary, including registered views.

The [protocol](../array_functions_1410_round3/arrays-overlap-protocol.md)
was committed at `3a35ff1a` before attribution. The
[pins](../array_functions_1410_round3/arrays-overlap.json) were
committed at `99abbddc` before the paired session. It ended
at 2026-11-23 04:03:31 UTC.
[All 72 release samples](../array_functions_1410_round3/arrays-overlap-samples.jsonl.gz)
are committed, alongside the separate
[profile stacks](../array_functions_1410_round3/arrays-overlap-profile-stacks.jsonl.gz)
and [profiled samples](../array_functions_1410_round3/arrays-overlap-profile-samples.jsonl.gz).

| Build | Source | Executable SHA256 |
| --- | --- | --- |
| Staging | `74503100ad7970eba660c2e1a026f7daab479893` | `d3f33a8101b1cbbb830ac7226295983a0eb88c751d0ab7df396378f0f13594f6` |
| Candidate | `3f5c501f6850a75ce87ffddf56df46269d10551a` | `818d5fa78d850d164630b3719d0c262d719ae31c89f07d93799e9555446afd33` |
