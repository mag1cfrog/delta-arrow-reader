# Same-scale Decimal128 precision conversion results (#1412)

The final cast narrows Decimal128(21, 2) to Decimal128(18, 2).
When every stored value fits, the cast shares the existing values
and validity. Other inputs retain the Arrow cast and its policies.

## Remaining gaps

- DECIMAL division and ROUND with ANSI off remains 1.273 ns per row above native. Its floor is 0.219 ns.
- DECIMAL division and ROUND with ANSI on remains 6.350 ns per row above native. Its floor is 0.219 ns.

The candidate still checks value magnitudes, computes the fused
quotient and rounds it. The ANSI plan keeps the divisor-NULL guard
and its common divisor projection.

Times are median [min, max] ns per input row above projection from nine
retained samples per arm. Each floor uses three native process medians
per control arm.

| Function | Path | Staging | Candidate | Native A/A floor |
| --- | --- | ---: | ---: | ---: |
| DECIMAL division and ROUND | Native | 27.889 [27.106, 28.664] | 27.839 [27.138, 28.152] | 0.219 |
| DECIMAL division and ROUND | ANSI off | 31.644 [31.307, 31.764] | 29.111 [28.756, 29.372] | 0.219 |
| DECIMAL division and ROUND | ANSI on | 37.143 [35.178, 38.220] | 34.189 [32.341, 34.798] | 0.219 |

Each timed statement matched all 1,000,000 result rows
and their executed Arrow types across both builds, both ANSI modes and native.
Eight SQL controls compare exact rows, types and failures in both
ANSI modes. They cover CAST and TRY_CAST overflow, signed rounding
ties, division by zero, NULL input and divisors, precision 38, scale
changes and empty input. The unit test compares three scales directly
with Arrow and verifies exact shared slices for values that fit.
All 676 selected workspace tests, formatting and Spark-root Clippy passed.

The [protocol](../arithmetic_1412/decimal-precision-protocol.md) was
committed at `6b6bef0111b53461699f47ba8603cbe5550adbb8` before clocks.
The Linux release session ran 2026-11-21T10:31:49Z to 2026-11-21T10:32:22Z.
The 18 comparison processes and 6 native controls retained 72 pairs.
Local evidence is under `costs-168-2026-11-20/1412/decimal-precision`.

| Build | Source | Executable SHA256 |
| --- | --- | --- |
| staging | `3ead5e197afd7ca12c3da1d0cebf22ef032dd92d` | `90a4b19abdf75d5b851672defe96e8e4a94e4d15990cd5b0ca59179d005a2436` |
| candidate | `3e900b8424cf1c8ac5310d8887751d52d722cf89` | `ce8704bcf0d3176de5485f345c1dd0647e02dc20ec8f4cfd028937330b9abd87` |
