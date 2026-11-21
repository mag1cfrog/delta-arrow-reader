# Generator, UTF-8 and conditional performance results (#1411)

The [attribution report](generators-text-conditionals-1411-attribution.md)
records the plans, one profile per function and the source of the extra
work. Each row below compares the two release builds in its own Linux
session. Values are median nanoseconds per input row above projection.
The floor comes from that session's native A/A process medians.

| Function | ANSI off before / candidate | ANSI on before / candidate | Native before / candidate | Native floor | Outcome |
| --- | ---: | ---: | ---: | ---: | --- |
| [INLINE_OUTER](inline-outer-1411.md) | 99.301 / 18.994 | 98.237 / 19.342 | 0.046 / 0.051 | 0.007 | Cost reference |
| [DECODE](decode-1411.md) | 30.818 / 1.217 | 30.778 / 1.204 | 0.809 / 0.817 | 0.082 | Cost reference |
| [ENCODE](encode-1411.md) | 27.826 / 0.543 | 27.829 / 0.544 | 0.076 / 0.073 | 0.007 | Cost reference |
| [WHEN](when-1411.md) | 12.758 / 0.009 | 13.196 / 0.028 | 0.114 / 0.117 | 0.009 | Target met |
| [INLINE](inline-1411.md) | 15.539 / 15.268 | 15.552 / 15.427 | 7.059 / 7.050 | 0.067 | Staging retained |
| [COUNT_IF](count-if-1411.md) | 8.843 / 1.372 | 8.834 / 1.380 | 1.317 / 1.403 | 0.009 | Target met |
| [NANVL](nanvl-1411.md) | 7.167 / 1.279 | 7.155 / 1.277 | 0.391 / 0.434 | 0.011 | Above native |
| [STACK](stack-1411.md) | 60.283 / 53.035 | 59.277 / 53.027 | 52.788 / 48.875 | 0.768 | Above native |
| [CASE](case-1411.md) | 20.095 / 16.157 | 20.086 / 16.095 | 14.055 / 14.131 | 1.326 | Above native |

Each frozen shape matched both builds, both ANSI modes and the native
reference in values and executed Arrow types. The generator shapes
include 937,500 INLINE rows and 2,000,000 STACK rows from one million
inputs. Each individual report records its untimed boundary controls.

## Remaining work

- INLINE retains the staging runtime path. The candidate reduced
  medians by 0.271 and 0.125 ns per input row with mixed paired process
  results. Staging remains 8.481 and 8.494 ns above its paired native
  control. It still adapts the child validity and expands the generator.
- NANVL remains 0.845 and 0.843 ns above native. It evaluates the
  replacement eagerly and checks the left buffer for NaNs or noncanonical
  NULL storage.
- STACK remains 4.160 and 4.152 ns above native. It still constructs
  the Spark list and projects the expanded result.
- CASE remains 2.025 and 1.964 ns above native. The ELSE cast still
  carries its Spark result annotation.

INLINE_OUTER, DECODE and ENCODE use different-work native controls.
Their reports identify the remaining work on the Spark path. The
bounded round is complete. #1411 stays open with the four gaps above.
