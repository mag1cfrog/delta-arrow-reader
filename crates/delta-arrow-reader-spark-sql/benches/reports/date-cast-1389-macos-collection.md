# Utf8View DATE collection trial on macOS (#1389)

MacBook Pro Mac14,6, Apple M2 Max, 32 GiB, macOS 26.7.3 (25G342).

The candidate passes the timing rule. Unpadded DATE is faster
beyond its floor with ANSI on. Its ANSI-off change is not
resolvable on Apple Silicon at its session floor. The other
Spark cells are faster in both modes and all native controls
are within their floors.

The [phase probe](date-cast-1389-macos-profile.md) isolated
unpadded DATE collection overhead. The candidate reserves
Date32 output for the input array length. Its row parser,
NULLs and first strict failure are unchanged.

**Apple Silicon, native macOS.** Query completion in ms per statement.

| Statement | Configuration | Staging | Candidate | Paired delta | A/A floor | Decision |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| Unpadded DATE | Native | 330.472 | 325.975 | -4.431 | 4.564 | within floor |
| Unpadded DATE | Spark ANSI off | 342.834 | 338.694 | -3.446 | 5.156 | within floor |
| Unpadded DATE | Spark ANSI on | 341.636 | 337.193 | -5.277 | 4.844 | faster |
| Ten-byte DATE | Native | 211.998 | 211.327 | -0.145 | 1.345 | within floor |
| Ten-byte DATE | Spark ANSI off | 215.022 | 210.372 | -5.356 | 1.476 | faster |
| Ten-byte DATE | Spark ANSI on | 214.629 | 210.214 | -4.398 | 1.811 | faster |
| Mixed DATE/DECIMAL | Native | 843.983 | 842.206 | -3.850 | 7.636 | within floor |
| Mixed DATE/DECIMAL | Spark ANSI off | 851.509 | 842.052 | -8.476 | 2.864 | faster |
| Mixed DATE/DECIMAL | Spark ANSI on | 852.500 | 842.140 | -10.360 | 4.688 | faster |

**Apple Silicon, native macOS.** Paired Spark excess over native
in ns per input row.

| Statement | Configuration | Staging gap | Candidate gap | A/A floor |
| --- | --- | ---: | ---: | ---: |
| Unpadded DATE | Spark ANSI off | 2.599 | 2.430 | 1.043 |
| Unpadded DATE | Spark ANSI on | 2.414 | 2.048 | 1.425 |
| Ten-byte DATE | Spark ANSI off | 0.516 | -0.159 | 0.306 |
| Ten-byte DATE | Spark ANSI on | 0.462 | -0.180 | 0.486 |
| Mixed DATE/DECIMAL | Spark ANSI off | 0.997 | 0.120 | 1.273 |
| Mixed DATE/DECIMAL | Spark ANSI on | 1.150 | 0.137 | 1.327 |

Unpadded DATE still exceeds native in both modes. The paired
native gaps for ten-byte DATE and mixed DATE/DECIMAL are not
resolvable on Apple Silicon under this schedule. No separated
DATE phase in this round locates another cause for the
post-change unpadded gap.

The [protocol](../macos_rounds/1389-date-collection.md) pins
staging's sealed release at `c86461ba`, with the same build
inputs as staging `ad5cbf39`, and the candidate release at
`7f38c538`. Nine blocks completed 324 processes and retained
1,620 samples, nine per parsers process and three per
casts-date process as fixed by #1560.

Four DATE tests passed. The added array differential compared
26 cases from the 13 committed input categories, including NULLs
and the first strict error. The SF1 preflight passed 15 exact
comparisons of 6,001,215 rows, including executed SQL and Arrow
types and cross-release source I/O. No statement was excluded.

- [Exact results and release receipts](date-cast-1389-macos-collection-preflight.json)
- [Full paired results](date-cast-1389-macos-collection-comparisons.json)
- [Compressed raw samples](date-cast-1389-macos-collection-samples.jsonl.gz)
