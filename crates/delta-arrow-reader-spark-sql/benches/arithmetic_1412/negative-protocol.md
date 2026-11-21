# Checked BIGINT negation comparison (#1412)

The [attribution](../reports/arithmetic-1412-attribution.md) traces
checked negation to the fallible per-element Arrow kernel. A BIGINT
array without the minimum value can use the wrapping kernel because
every negation fits in BIGINT. Reuse the array's validity mask. Keep
the existing kernel when any slot contains the minimum, and for
scalars and other types.

Require exact rows and executed types on both builds, in both ANSI
modes and on native, for the [frozen negation statement](negative.json).
The unchanged ANSI-off path is a control. First run the [untimed SQL
controls](negative-validation.json) for BIGINT bounds, visible and
masked minimum values, scalar overflow, narrower signed integers,
floating-point and decimal operands, NULL and empty input. Require
the original result or failure in each mode.

Use the [matched Linux release schedule](../window_aggregates_1408/string-aggregation-protocol.md#schedule-and-pass-rule).
The target is at or below candidate native plus the same-session A/A
floor in both ANSI modes.
