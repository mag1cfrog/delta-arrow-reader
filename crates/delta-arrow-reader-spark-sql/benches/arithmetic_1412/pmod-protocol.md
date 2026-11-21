# Positive BIGINT PMOD comparison (#1412)

The [attribution](../reports/arithmetic-1412-attribution.md#positive-modulo)
identifies two remainder passes and separate sign correction. For two
BIGINT arrays with positive visible divisors, use one remainder and
add the divisor to negative results. Other inputs use the original
kernel. The frozen input has positive or NULL divisors after widening.

Run the [pinned comparison](pmod.json) against both release executables.
Require exact rows and executed Arrow data types on both builds, both
ANSI modes and the native statement. Verify the widening and PMOD
expression in each physical plan.

First compare the [boundary queries](pmod-validation.json) in both
builds and modes. Cover signed endpoints, NULL operands, zero divisor
with and without a NULL dividend, negative divisor and its existing
overflow, a scalar divisor, narrower integers, floats, decimals and
empty input. Match exact rows and types or the original error.

Use the [matched Linux release schedule](../window_aggregates_1408/string-aggregation-protocol.md#schedule-and-pass-rule).
The target is at or below the candidate native median plus its
same-session repeat floor in both ANSI modes.
