# Borrowed interval token input trial (#1389)

The candidate stays unmerged. Day-time and calendar improved in both
ANSI modes. The two unquoted year-month cells and the ANSI-off quoted
cell missed the required gain. The original token stream remains in
the batch parser.

| Cast | ANSI | Staging ms | Candidate ms | Paired change ms | Staging spread ms | Gap ns/row, staging / candidate | Reference noise ns/row, staging / candidate |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Year-month | off | 44.578 | 42.389 | -0.106 | 2.602 | 648 / 437 | 63 / 274 |
| Year-month | on | 43.387 | 42.734 | -0.299 | 1.034 | 503 / 479 | 63 / 274 |
| Day-time | off | 46.285 | 42.789 | -3.876 | 1.363 | 613 / 454 | 89 / 205 |
| Day-time | on | 46.249 | 44.354 | -1.817 | 0.923 | 908 / 592 | 57 / 185 |
| Calendar | off | 51.635 | 47.736 | -4.186 | 1.300 | 1,668 / 1,095 | 17 / 34 |
| Calendar | on | 51.135 | 46.535 | -4.752 | 1.470 | 1,606 / 949 | 17 / 34 |
| Quoted year-month | off | 48.554 | 48.662 | +0.887 | 1.040 | 1,193 / 1,160 | 104 / 194 |
| Quoted year-month | on | 48.769 | 47.821 | -0.948 | 0.615 | 1,220 / 1,058 | 104 / 194 |

Times are medians of three process medians for 8,184 rows. Paired
change uses the median of matched replica differences. Gaps are
against native or the same-mode day-time projection in each build.
All five control cells passed.

The [profile](interval-1389-residual-profile.md) attributed 143 and
198 ns per call to the owned stream in day-time and calendar. The
candidate fed the same numeric grammar from the lexed slice and kept
the owned stream for the full grammar. It rebuilt the smaller grammar
for each row to use that slice lifetime. The timing measures the
combined change. It does not isolate the cost of construction alone.

All 26 preflight outputs and 29 exact comparisons passed for rows,
executed Arrow and SQL types and source I/O. All four day-time
cast and projection I/O checks passed. Independent review, the
3,297-test Linux workspace suite, formatting and root Clippy passed.
Three existing workspace tests were ignored.

The [compressed samples](../interval_1389_round3/token-input-samples.jsonl.gz)
retain all 486 observations from 162 processes. Staging and the
candidate were built from `bfde6ba0` and `6e5f080f` respectively.
The [candidate patch](../interval_1389_round3/token-input-candidate.patch.gz)
is retained with the [frozen protocol](interval-1389-token-input-protocol.md).

Staging retains the lexer change and the gaps in the staging columns
above. The owned stream and other grammar and conversion work remain
in the profile. #1389 stays open for these costs and the DATE and
DATE_FORMAT gaps already recorded separately.
