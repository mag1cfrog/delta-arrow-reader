# Literal URL parts (#1414)

Two-argument PARSE_URL and TRY_PARSE_URL normally expand a literal part
and an absent key to the size of the URL column. For a Utf8 column and a
non-NULL Utf8 part, the trial borrows the part and passes the absent key
as `None`. It uses the existing parser and strict or tolerant handling
on each row. The parser still selects the named part for each URL.

The differential test covers all supported part names, an unknown part,
empty batches, NULLs, malformed URLs and URLs with user information,
ports, queries and fragments. Formatting, Spark-root Clippy and 674
selected tests passed.

## Matched release timing

Values are the median and range of process medians, in ns per input row
above projection.

| Function | ANSI | Before [range] | Trial [range] | Paired reduction | Floor | Decision |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| `PARSE_URL` | off | 177.761 [176.703, 179.117] | 167.589 [167.238, 168.220] | 10.523 | 1.207 | Faster |
| `PARSE_URL` | on | 177.703 [177.671, 178.602] | 167.216 [167.082, 167.705] | 10.621 | 0.465 | Faster |
| `TRY_PARSE_URL` | off | 180.956 [180.478, 181.403] | 171.836 [168.665, 171.953] | 9.567 | 1.644 | Faster |
| `TRY_PARSE_URL` | on | 181.817 [180.240, 188.358] | 169.726 [168.630, 170.490] | 13.187 | 4.059 | Faster |

The paired reductions clear the improvement rule in all four cases.
Both builds produced identical million-row results and executed types
in both ANSI modes.

The [pinned comparison](../spark_only_functions_1414/parse-url-comparison.json)
followed the [shared protocol](../spark_only_functions_1414/protocol.md).
The 24 processes retained 72 samples. Raw evidence is available
locally under `costs-168-2026-11-20/1414/parse-url`.
