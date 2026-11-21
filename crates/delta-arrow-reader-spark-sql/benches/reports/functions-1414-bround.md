# Literal DOUBLE BROUND scale (#1414)

With a nonempty double column and a literal integer scale, BROUND
computes the scale factor once and uses the existing rounding calculation.
The default scale is zero. The other types and argument shapes keep their
existing path.

The differential test compares exact floating-point bits and NULLs,
including ties, signed zero, NaN, infinity and scale extremes.
Formatting, Spark-root Clippy and 681 selected tests passed.

## Matched release timing

Values are the median and range of process medians, in ns per input row
above projection.

| Function | ANSI | Before [range] | Trial [range] | Paired reduction | Floor | Decision |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| `BROUND` | off | 6.760 [6.737, 6.857] | 5.792 [5.791, 5.843] | 0.969 | 0.060 | Faster |
| `BROUND` | on | 6.824 [6.774, 6.950] | 5.824 [5.811, 5.869] | 0.956 | 0.088 | Faster |

The one-argument BROUND statement returns identical million-row results
and executed types in both builds and ANSI modes. Both paired reductions
clear the improvement rule.

The [pinned comparison](../spark_only_functions_1414/bround-comparison.json)
followed the [shared protocol](../spark_only_functions_1414/protocol.md).
The 12 processes retained 36 samples. Raw evidence is available locally
under `costs-168-2026-11-20/1414/bround`.
