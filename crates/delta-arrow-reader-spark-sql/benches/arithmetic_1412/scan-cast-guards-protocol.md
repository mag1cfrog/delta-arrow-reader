# Arithmetic NULL guards on stored columns (#1412)

The [attribution](../reports/arithmetic-1412-attribution.md#integer-arithmetic-division-and-guards)
identifies a CASE that protects casts when the earlier operand is
NULL. For projections over stored columns, prove the selected casts
infallible before omitting that CASE. Name-only projections, aliases
and filters can trace back to the scan. Computed expressions, views
and other cast types keep the original guard.

Use the [pinned comparison](scan-cast-guards.json) for addition,
subtraction, multiplication, division and the decimal division and
ROUND statement. Require exact results and executed Arrow types
across both builds, both ANSI modes and native. The checked division
and decimal precision/rounding behavior remain in their operators.

Before timing, compare rows, types or original errors for the
[untimed controls](scan-cast-guards-validation.json) in both modes.
Confirm the physical NULL CASE disappears on the proven scan
operands, including through a passthrough alias. Also exercise a
computed operand, a fallible cast on a skipped or selected row,
division by zero and empty input. The registered-table regression
checks zero and NULL divisors on the stored-column path. The decimal
controls require the guard with nine integer digits or a scale change
and omit it when the INT cast reserves all ten integer digits.

Use the [matched Linux release schedule](../window_aggregates_1408/string-aggregation-protocol.md#schedule-and-pass-rule).
Each shape targets the candidate native median plus its same-session
A/A floor in both ANSI modes. Report remaining checked operations
or conversions.
