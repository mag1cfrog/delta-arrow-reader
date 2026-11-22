# Literal TRY_AES ECB key and mode (#1414)

For a binary column and three arguments, a valid literal ECB mode and
key prepare the cipher once. The row loop retains its input extraction,
block operations, padding, output assembly and NULL-on-error policy.
Other shapes and the strict functions use the existing evaluation.

The differential test covers 16-, 24- and 32-byte keys, empty and NULL
input, valid ciphertext, invalid lengths and invalid PKCS padding. The
selected tests also include the saved Spark column-argument results.
Formatting, Spark-root Clippy and 681 selected tests passed.

## Matched release timing

Both binaries include the #1405 column-input correction. Values are
the median and range of process medians, in ns per input row above
projection.

| Function | ANSI | Before [range] | Trial [range] | Paired reduction | Floor | Decision |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| `TRY_AES_DECRYPT` | off | 155.857 [154.212, 156.469] | 63.184 [62.848, 63.758] | 92.098 | 1.129 | Faster |
| `TRY_AES_DECRYPT` | on | 155.010 [153.021, 157.829] | 63.693 [63.079, 64.047] | 91.317 | 2.404 | Faster |
| `TRY_AES_ENCRYPT` | off | 179.888 [178.118, 180.547] | 86.711 [84.301, 88.541] | 92.006 | 2.120 | Faster |
| `TRY_AES_ENCRYPT` | on | 178.346 [177.180, 179.637] | 85.867 [85.739, 93.378] | 91.313 | 3.820 | Faster |

The two builds returned identical million-row results and Binary
output types for both functions and ANSI modes. All four paired
reductions clear the improvement rule, from 91.31 to 92.10 ns per
input row.

The [pinned comparison](../spark_only_functions_1414/try-aes-comparison.json)
followed the [shared protocol](../spark_only_functions_1414/protocol.md).
The 24 processes retained 72 samples. Raw evidence is available locally
under `costs-168-2026-11-20/1414/try-aes`.
