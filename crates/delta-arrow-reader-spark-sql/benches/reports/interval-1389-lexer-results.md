# STRING to INTERVAL without Rich lexing on valid rows (#1389)

All eight Spark cast configurations passed the [paired timing
rule](interval-1389-lexer-protocol.md). All five native and projection
controls stayed within the gate. The candidate defers the original
Rich lexer until the fast lex fails.

| Cast | ANSI | Staging ms | Candidate ms | Paired change ms | Staging spread ms | Gap ns/row, staging / candidate | Reference noise ns/row, staging / candidate |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Year-month | off | 50.393 | 43.881 | -6.130 | 1.344 | 1,335 / 699 | 52 / 120 |
| Year-month | on | 49.882 | 44.629 | -5.962 | 2.151 | 1,273 / 790 | 52 / 120 |
| Day-time | off | 57.676 | 46.215 | -11.204 | 0.960 | 2,238 / 848 | 113 / 6 |
| Day-time | on | 57.525 | 46.423 | -11.364 | 1.825 | 2,165 / 821 | 131 / 129 |
| Calendar | off | 68.790 | 51.897 | -16.933 | 1.938 | 3,537 / 1,667 | 203 / 148 |
| Calendar | on | 68.659 | 49.915 | -18.743 | 2.539 | 3,521 / 1,425 | 203 / 148 |
| Quoted year-month | off | 59.276 | 48.780 | -10.496 | 2.746 | 2,392 / 1,245 | 43 / 71 |
| Quoted year-month | on | 58.546 | 48.652 | -9.895 | 2.900 | 2,303 / 1,229 | 43 / 71 |

Each statement consumed 8,184 input rows. Query time is the median of
three process medians. Paired change uses matched replica differences.
Each gap is against the same build's native cast, or its input projection
for day-time. Quote formation is included in the quoted Spark statement.

The [archive](../interval_1389_round3/lexer-samples.jsonl.gz) contains
all 486 retained observations from 162 processes. It pins staging
`310ffad9` and the release candidate `bfde6ba0`. The baseline
runner, protocol and archived samples added since `310ffad9` leave its
product source unchanged. The final test commit corrects the reference
helper and exercises failure in a decoded quote. It changes only the
test module, so the timed release source is identical to the final
candidate's.

All 26 preflight outputs and 29 exact comparisons passed, with identical
executed Arrow and SQL types, rows, and source I/O. All four day-time
cast and projection I/O checks passed. The focused error test, 3,297
Linux workspace tests, formatting and root Clippy passed. Three existing
workspace tests were ignored.

The remaining interval cost is above the reference noise. The issue
stays open while the residual profile identifies where that time goes.
