# Array function performance results (#1410)

The [attribution report](array-functions-1410-attribution.md) compares
physical plans and one profile per measured function. Seven focused
comparisons cover the identified groups of costs.

Each row uses one paired Linux release session. Times are median
nanoseconds per input row above projection. Each before and after
comparison used both executables and retained the same native reference
and executed types. The native floor comes from the candidate A/A
controls in that session.

| Function | ANSI off before / after | ANSI on before / after | Native before / after | Native floor | Target |
| --- | ---: | ---: | ---: | ---: | --- |
| [ARRAY_MAX](array-extrema-1410.md) | 108.616 / 8.062 | 108.231 / 8.059 | 8.480 / 8.486 | 0.065 | Met |
| [ARRAY_MIN](array-extrema-1410.md) | 108.491 / 8.047 | 109.534 / 8.040 | 8.633 / 8.486 | 0.045 | Met |
| [ARRAYS_OVERLAP](array-overlap-1410.md) | 127.281 / 82.331 | 127.228 / 83.559 | 17.345 / 17.505 | 0.109 | Above native |
| [ELEMENT_AT](array-element-1410.md) | 62.662 / 6.434 | 63.174 / 6.451 | 14.240 / 14.313 | 0.099 | Met |
| [TRY_ELEMENT_AT](array-element-1410.md) | 62.530 / 6.448 | 62.767 / 6.443 | 14.285 / 14.233 | 0.311 | Met |
| [FLATTEN](flatten-1410.md) | 52.112 / 22.940 | 52.632 / 29.441 | 2.260 / 2.174 | 0.033 | Above native |
| [VECTOR_INNER_PRODUCT](vector-inner-product-1410.md) | 64.245 / 9.421 | 64.191 / 9.375 | 34.949 / 34.783 | 0.177 | Met |
| [SLICE](array-slice-1410.md) | 37.470 / 25.443 | 36.818 / 25.817 | 22.345 / 22.632 | 0.309 | Above native |
| [SEQUENCE](sequence-1410.md) | 26.372 / 20.768 | 26.784 / 21.486 | 13.412 / 12.374 | 1.084 | Above native |

## Remaining costs

- ARRAYS_OVERLAP is 64.826/66.054 ns per row above native with ANSI
  off/on. The surrounding normalization and CASE gates still evaluate
  operands and select NULL and empty arrays.
- FLATTEN is 20.766/27.267 ns per row above native. Spark still selects
  rows before invoking the native offset projection.
- SLICE is 2.811/3.185 ns per row above native. Spark keeps the field
  adaptation and row-error interface around the native kernel.
- SEQUENCE is 8.394/9.113 ns per row above native. Spark still evaluates
  lazy endpoint expressions and scatters active-row results, while the
  native expression calls a half-open range kernel.

Each comparison checked all one million keyed rows and executed Arrow
types in both builds, both ANSI modes and native before starting clocks.
The individual reports describe the committed differential tests and
local validation, give the release binary hashes, and link the protocols.
All nine shapes qualified for timing. Five met the native target in both ANSI modes.
The four remaining costs keep #1410 open after its one change per group.
