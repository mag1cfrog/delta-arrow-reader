# Same-scale Decimal128 precision cast comparison (#1412)

The [attribution](../reports/arithmetic-1412-attribution.md#decimal-division-and-rounding)
recorded 178 Decimal128 precision-check samples. The frozen statement
casts the ROUND output from Decimal128(21, 2) to Decimal128(18, 2).
When every stored value fits, the candidate can share the existing
value and validity buffers. The cast uses Arrow as before when any
value exceeds the new precision or the scale changes.

Use the [pinned comparison](decimal-precision.json) with the frozen
decimal division and ROUND statement. Check all one million rows and
executed Arrow types on both builds, both ANSI modes and native.
The physical plans must retain the fused divide, ROUND and final cast.

Run the [untimed controls](decimal-precision-validation.json) first.
Both builds and both ANSI modes must retain values and executed types,
or the original error. Cover the precision boundary and overflow
with CAST and TRY_CAST, signed half-up ties, NULLs, division by zero,
scale changes, precision 38 and empty input. The new unit test compares
directly with Arrow before checking shared slices and validity.

Use the [matched Linux release schedule](../window_aggregates_1408/string-aggregation-protocol.md#schedule-and-pass-rule).
The target is at or below candidate native plus the same-session A/A
floor in both ANSI modes. Report remaining cast, division and ROUND
costs.
