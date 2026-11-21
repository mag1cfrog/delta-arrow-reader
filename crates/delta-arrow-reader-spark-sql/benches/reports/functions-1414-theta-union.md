# Literal THETA_UNION sketch size (#1414)

For a valid literal `lgNomEntries`, THETA_UNION validates the size once
and passes it to the shared sketch loop. The default is 12. The existing
row evaluation handles NULL, invalid and column sizes, including the
rule that a NULL sketch suppresses size validation for that pair.

The differential test covers empty batches, NULL and corrupt sketches,
and valid and invalid size boundaries. Its 64 distinct inputs distinguish
sizes 4 and 12. Both builds return the same million rows and executed
types in both ANSI modes. Formatting, Spark-root Clippy and 675 selected
tests passed.

## Matched release timing

Values are the median and range of process medians, in ns per input row
above projection.

| Function | ANSI | Before [range] | Trial [range] | Paired reduction | Floor | Decision |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| `THETA_UNION` | off | 134.761 [134.725, 135.595] | 137.271 [135.948, 144.829] | -2.510 | 4.440 | Within comparison floor |
| `THETA_UNION` | on | 136.600 [135.528, 141.302] | 136.059 [135.588, 137.353] | 1.012 | 2.887 | Within comparison floor |

Both timing differences fall within their comparison floors. The change
removes the size broadcast and repeated validation. The cost of decoding
and merging each pair of sketches remains per row.

The [pinned comparison](../spark_only_functions_1414/theta-union-comparison.json)
followed the [shared protocol](../spark_only_functions_1414/protocol.md).
The 12 processes retained 36 samples. Raw evidence is available locally
under `costs-168-2026-11-20/1414/theta-union`.
