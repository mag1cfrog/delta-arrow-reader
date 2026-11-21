# Literal HLL_UNION configuration flag (#1414)

HLL_UNION passes a non-NULL literal `allowDifferentLgConfigK` directly
to the union loop. The default is false. Column and NULL flags use
the existing row evaluation. Each non-NULL pair still checks its own
sketch configurations and merges them with the same sketch code.

The differential test covers empty batches, NULL and corrupt sketches,
matching and differing configurations, and NULL, true and false flags.
With true and different configurations, it independently checks success,
NULL propagation and the expected estimate. Both builds return identical
million-row results and executed types in each ANSI mode. Formatting,
Spark-root Clippy and 669 selected tests passed.

## Matched release timing

Values are the median and range of process medians, in ns per input row
above projection.

| Function | ANSI | Before [range] | Trial [range] | Paired reduction | Floor | Decision |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| `HLL_UNION` | off | 102.621 [102.492, 102.727] | 102.100 [101.154, 102.675] | 0.393 | 0.761 | Within comparison floor |
| `HLL_UNION` | on | 103.710 [103.527, 105.031] | 102.526 [102.279, 107.875] | 1.184 | 2.798 | Within comparison floor |

Both timing differences fall within their comparison floors. Passing
the flag directly removes its broadcast in both modes.

The [pinned comparison](../spark_only_functions_1414/hll-union-comparison.json)
followed the [shared protocol](../spark_only_functions_1414/protocol.md).
The 12 processes retained 36 samples. Raw evidence is available locally
under `costs-168-2026-11-20/1414/hll-union`.
