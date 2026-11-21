# Arithmetic performance results (#1412)

The [attribution report](arithmetic-1412-attribution.md) records the
physical plans and profiles for all 15 frozen statements. Each row
below uses a single Linux release session. The table reports median
nanoseconds per input row above projection. Its floor comes from the
same session's native A/A process medians. Repeated statements cover
distinct changes, with their own before and after measurements.

| Function | ANSI off before / trial | ANSI on before / trial | Native before / trial | Native floor | Trial disposition |
| --- | ---: | ---: | ---: | ---: | --- |
| [>>>](unsigned-shift-1412.md) | 22.418 / 5.282 | 22.257 / 5.315 | 14.852 / 14.898 | 0.090 | Merged |
| [SHIFTRIGHTUNSIGNED](unsigned-shift-1412.md) | 20.413 / 4.001 | 20.348 / 4.050 | 4.274 / 4.241 | 0.062 | Merged |
| [PMOD](pmod-1412.md) | 16.622 / 6.323 | 17.476 / 6.346 | 8.191 / 8.257 | 0.037 | Merged |
| [DECIMAL division and ROUND](decimal-precision-1412.md) | 31.644 / 29.111 | 37.143 / 34.189 | 27.889 / 27.839 | 0.219 | Merged |
| [DECIMAL division and ROUND](scan-cast-guards-1412.md) | 28.913 / 28.865 | 34.036 / 28.868 | 27.738 / 27.845 | 0.189 | Merged |
| [TRY_ADD](try-integer-1412.md) | 9.726 / 7.665 | 9.737 / 7.404 | 3.333 / 3.294 | 0.039 | Merged |
| [TRY_SUBTRACT](try-integer-1412.md) | 9.740 / 7.342 | 9.710 / 6.919 | 3.310 / 3.335 | 0.045 | Merged |
| [TRY_MULTIPLY](try-integer-1412.md) | 9.814 / 6.998 | 9.726 / 7.193 | 3.465 / 3.507 | 0.030 | Merged |
| [BIGINT +](scan-cast-guards-1412.md) | 8.327 / 3.399 | 7.202 / 7.942 | 3.342 / 3.353 | 0.106 | Merged |
| [BIGINT -](scan-cast-guards-1412.md) | 7.844 / 3.379 | 7.364 / 7.531 | 3.358 / 3.364 | 0.030 | Merged |
| [BIGINT *](scan-cast-guards-1412.md) | 8.426 / 3.576 | 7.489 / 7.544 | 3.516 / 3.499 | 0.035 | Merged |
| [Floating /](scan-cast-guards-1412.md) | 5.111 / 5.092 | 8.446 / 4.984 | 4.490 / 4.432 | 0.131 | Merged |
| [BIGINT +](checked-bigint-1412.md) | 3.415 / 3.394 | 8.130 / 7.531 | 3.353 / 3.358 | 0.026 | Staging kept |
| [BIGINT -](checked-bigint-1412.md) | 3.352 / 3.408 | 7.774 / 7.690 | 3.338 / 3.313 | 0.127 | Staging kept |
| [BIGINT *](checked-bigint-1412.md) | 3.517 / 3.511 | 6.955 / 7.642 | 3.464 / 3.503 | 0.053 | Staging kept |
| [Floating /](float-divisor-1412.md) | 5.074 / 5.084 | 4.997 / 5.010 | 4.325 / 4.316 | 0.032 | Staging kept |
| [NEGATIVE](negative-1412.md) | 2.906 / 2.909 | 3.454 / 2.890 | 2.850 / 2.831 | 0.104 | Merged |
| [TRY_SUM](try-sum-1412.md) | 1.092 / 0.594 | 1.097 / 0.577 | 0.335 / 0.333 | 0.006 | Merged |
| [HEX](numeric-controls-1412.md) | 12.891 / 12.955 | 12.888 / 12.883 | 8.420 / 8.587 | 0.256 | Same executable |
| [BITMAP_BUCKET_NUMBER](numeric-controls-1412.md) | 14.488 / 13.036 | 13.754 / 13.037 | 11.693 / 11.715 | 0.017 | Same executable |

All timed statements matched exact rows and executed Arrow types
across both release builds, both ANSI modes and native. The individual
reports record separate untimed controls for original failures and
fallback inputs. Each comparison protocol was committed before its
run.

## Remaining work

- DECIMAL division and ROUND still costs 1.020 and 1.023 ns per
  input row above the paired native arm. It retains the Spark
  precision and division checks.
- TRY_ADD, TRY_SUBTRACT and TRY_MULTIPLY remain 3.492-4.371 ns per
  input row above native. The batch result still requires checked
  arithmetic and the other inputs use the per-row fallback.
- After removing redundant cast guards, ANSI-on BIGINT addition,
  subtraction and multiplication still cost 4.777, 4.436 and 3.491
  ns per input row above native in the retained staging arm of the
  checked BIGINT trial. The range scan candidate had mixed results.
  Keep the original checked execution. ANSI-off addition and
  multiplication remain 0.062 and 0.053 ns per input row above
  native in that arm. Those small gaps vary across sessions, and
  the profiles do not isolate a separate ANSI-off kernel cost.
- Floating division retains gaps of 0.749 and 0.672 ns per input
  row. Its direct divisor scan trial was within the repeat floor.
  ANSI on still checks for valid zero divisors. ANSI off retains
  NULLIF to produce NULL for zero divisors.
- TRY_SUM remains 0.261 and 0.244 ns per input row above native.
  The optimized batches still verify the element range and have
  to prove that no prefix overflows.
- HEX retains the locked numeric formatter required by the
  existing dispatch test. Both control arms reproduce its gap.
  BITMAP_BUCKET_NUMBER repeats also cost more than native. They
  vary across identical binaries, and both plans use the same
  arithmetic. The [matched controls](numeric-controls-1412.md)
  record the gap and the projection difference without attributing
  the whole gap to that projection.

The bounded round is complete. #1412 stays open with these gaps.
