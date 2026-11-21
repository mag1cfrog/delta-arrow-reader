# Literal number conversion formats (#1414)

TO_NUMBER and TRY_TO_NUMBER parse the format once per batch. They
previously copied the literal into a full Arrow array before reading
the first entry. The scalar settings path supplies one entry for a
nonempty text column. The existing expansion handles other inputs.

Both builds matched the complete million-row result and executed
Decimal output in each ANSI mode. The local differential test also
compares successful formats, invalid formats and input, NULLs, empty
batches and the strict and tolerant function modes. Formatting,
Spark-root Clippy and 674 selected tests passed.

## Matched release timing

Times are the median and range of process medians, in ns per input row
above the plain projection.

| Function | ANSI | Before [range] | Trial [range] | Paired reduction | Floor | Decision |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| `TO_NUMBER` | off | 508.989 [508.605, 509.017] | 508.407 [506.041, 511.448] | 0.582 | 2.703 | Within comparison floor |
| `TO_NUMBER` | on | 509.852 [508.713, 510.158] | 506.104 [504.767, 512.198] | 3.748 | 3.715 | Faster |
| `TRY_TO_NUMBER` | off | 508.887 [508.796, 509.072] | 507.909 [507.595, 508.345] | 0.979 | 0.375 | Faster |
| `TRY_TO_NUMBER` | on | 512.760 [509.252, 559.789] | 509.252 [506.169, 509.659] | 3.101 | 25.269 | Within comparison floor |

Two comparisons clear the measured improvement rule. Two fall within
the measured floor. All four clear the retention rule and the literal
format no longer needs a per-row copy. The parser still compiles the
format once per batch and matches each input value.

The [pinned comparison](../spark_only_functions_1414/to-number-comparison.json)
followed the [shared protocol](../spark_only_functions_1414/protocol.md).
The session completed 24 processes with 72 retained samples. Raw
results are available locally under `costs-168-2026-11-20/1414/to-number`.
