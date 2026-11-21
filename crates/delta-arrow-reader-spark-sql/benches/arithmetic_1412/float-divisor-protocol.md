# Checked floating-point zero divisor comparison (#1412)

The [attribution](../reports/arithmetic-1412-attribution.md#integer-arithmetic-division-and-guards)
identifies a Boolean mask for the checked zero-divisor test. For two
arrays, scan the divisor values and consult both validity masks on
zero slots. Keep the existing scalar broadcasting and length checks.

Run the [pinned comparison](float-divisor.json) for the frozen
division statement. Require exact rows and executed data types on
both builds, in both ANSI modes and on native. ANSI off exercises the
existing NULLIF-based path as a control.

First compare the [untimed SQL controls](float-divisor-validation.json).
Include positive and negative zero, zero masked by a NULL numerator,
NULL divisors, NaN and infinities, scalar operands, floating remainder
and empty input. Both builds must return exactly the same results and
types or the original failure in each mode.

Use the [matched Linux release schedule](../window_aggregates_1408/string-aggregation-protocol.md#schedule-and-pass-rule).
The target is at or below candidate native plus the same-session A/A
floor in both ANSI modes.
