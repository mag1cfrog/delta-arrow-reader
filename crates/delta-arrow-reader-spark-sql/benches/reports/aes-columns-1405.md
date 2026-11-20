# AES results on the original #1391 input

The four AES functions now return the expected binary values on column
input in both ANSI modes. Previously the strict calls failed on batches
with more than one row and the tolerant forms returned NULL across the
batch. Neither outcome supplies comparable pre-fix execution timings.

All four statements matched the Spark-derived reference for all one million
rows, including 62,500 NULL inputs, in each ANSI mode. The numbers below are
nanoseconds per input row. Each cell has three process medians from three
samples after a warmup. The original benchmark table and SQL are unchanged.
[Protocol](aes-columns-1405-protocol.md), [preflights and samples](aes-columns-1405-samples.jsonl.gz).

| Function | ANSI off | ANSI on | Off above projection | On above projection |
| --- | ---: | ---: | ---: | ---: |
| `aes_decrypt` | 162.19 | 159.29 | 161.97 | 158.90 |
| `aes_encrypt` | 190.68 | 187.68 | 190.48 | 187.45 |
| `try_aes_decrypt` | 162.43 | 158.27 | 162.21 | 158.07 |
| `try_aes_encrypt` | 190.88 | 186.58 | 190.56 | 186.38 |

## Spark reference and wire format

The fixture records 212 Spark 4.2.0 observations. DAR runs the value, type
and failure comparisons with one-row and multi-row input batches. The
capture covers 128-, 192- and 256-bit keys, supplied IVs, random-IV round
trips, NULL arguments, a valid row beside an invalid row, and corrupt
ciphertext. Existing deterministic CBC and GCM literal results were also
checked in the pre-fix build and match the captured bytes.
[Spark capture and inputs](../../tests/fixtures/aes-column-arguments/spark-results.jsonl).

Spark 4.2.0 provides `try_aes_decrypt`. For DAR's `try_aes_encrypt`, the
fixture uses Spark's strict ciphertext on valid input and records NULL
for rows whose strict call fails. The Spark routine lookup is recorded
separately. Spark-compatible ECB ciphertext has no IV prefix. Use CBC
to read ciphertext produced by the old ECB spelling.

The Linux release measurement used `9e4df1b1` from
2026-11-20T18:18:03Z to 2026-11-20T18:18:38Z, with 72 retained samples.
