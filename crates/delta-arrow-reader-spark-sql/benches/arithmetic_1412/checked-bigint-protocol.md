# Bounded ANSI BIGINT arithmetic comparison (#1412)

The [attribution](../reports/arithmetic-1412-attribution.md#integer-arithmetic-division-and-guards)
identifies checked ANSI integer kernels. When both BIGINT arrays
contain only values in the signed INT range, all three operations
fit BIGINT. Use the existing wrapping Arrow kernels in that range.
The original checked kernel handles full-width values and scalars.

Run the [pinned comparison](checked-bigint.json) for the three
unchanged addition, subtraction and multiplication statements.
Require identical rows and executed data types across both builds,
both ANSI modes and native. The frozen fixture meets the INT bounds.
Both modes appear in the schedule, with ANSI off serving as a control.

First compare the [untimed controls](checked-bigint-validation.json).
Use both ends of the INT range in BIGINT arrays, full-width values
that still fit, overflow at both BIGINT ends for all three operators,
NULLs, scalar inputs and empty input. Match rows and executed types
or the exact old failure in both ANSI modes.

Use the [matched Linux release schedule](../window_aggregates_1408/string-aggregation-protocol.md#schedule-and-pass-rule).
The target is at or below candidate native plus its same-session
repeat floor in both ANSI modes. The timing covers bounded data.
