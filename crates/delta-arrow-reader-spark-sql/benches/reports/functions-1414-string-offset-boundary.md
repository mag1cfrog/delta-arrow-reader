# Literal broadcast capacity boundary (#1414)

The original adapters broadcast Utf8 literals before the parser runs.
At 8,192 rows, a 262,144-byte literal would exceed the signed 32-bit
Arrow offset limit. A correctness-only SQL replay confirmed that the
original path failed with `offset overflow` in all four cases below.

| Statement | Outcome with the singleton-setting trial |
| --- | --- |
| `FROM_CSV` with a space-padded schema | Returned one million rows |
| `FROM_JSON` with a space-padded schema | Returned one million rows |
| `PARSE_URL` with a space-padded part | Returned one million rows |
| `TO_NUMBER` with an oversized format | Returned an execution error |

The four trials changed the failure behavior. The five affected source
files now match the #1414 baseline exactly, including the shared
TRY_TO_NUMBER and TRY_PARSE_URL paths. The old allocation and offset
failure remain. The ordinary timing records capture the withdrawn
code, including the earlier reductions for URL parsing.

- [TO_NUMBER and TRY_TO_NUMBER trial](functions-1414-to-number.md)
- [FROM_CSV trial](functions-1414-from-csv.md)
- [FROM_JSON trial](functions-1414-from-json.md)
- [PARSE_URL and TRY_PARSE_URL trial](functions-1414-parse-url.md)

The restored release build and the original build both produced the
same process failure code and Arrow offset error for each capacity case.
All six original benchmark statements returned identical complete
million-row results and executed types in both ANSI modes. Formatting,
Spark-root Clippy and 701 selected tests passed. These were correctness
comparisons. The raw preflight evidence is available locally under
`costs-168-2026-11-20/1414/string-offset-boundary` and
`costs-168-2026-11-20/1414/string-offset-restored`.
