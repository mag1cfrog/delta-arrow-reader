# Direct BIGINT unsigned shift comparison (#1412)

The [attribution](../reports/arithmetic-1412-attribution.md#unsigned-right-shift)
shows DECIMAL CASE arithmetic before both unsigned shift forms.
The candidate views a BIGINT column as UInt64 for the existing shift
and final cast. The minimum BIGINT still calls the original ABS
kernel. Other operands retain the previous conversion.

Use [shift.json](shift.json) to pin the two executables and both
frozen statement shapes. Require exact rows and executed Arrow
data types on both builds and both ANSI modes against native.
The candidate's physical plans must use the unsigned view, and
staging's plans must show the original DECIMAL conversion.

First compare [the SQL boundaries](shift-validation.json) on both
builds and both ANSI modes. Require identical rows and types on
success and the original failures otherwise. Check signed limits,
NULL values and distances, zero, negative and oversized distances,
INT and unsupported TINYINT inputs, computed operands and empty
input. Check an unselected CASE arm and projected constants with
and without an empty LIMIT. LIMIT 0 must plan to EmptyExec with
no scan. The runner's source requirement makes that a plan check.
The unit test compares a sliced buffer and validity bitmap,
including a hidden signed minimum.

Use the [matched Linux release schedule](../window_aggregates_1408/string-aggregation-protocol.md#schedule-and-pass-rule).
The target is at or below candidate native plus the same-session
A/A floor for each shift form in both ANSI modes. Report any residual.
