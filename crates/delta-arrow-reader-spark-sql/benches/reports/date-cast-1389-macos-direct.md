# Direct numeric DATE trial on macOS (#1389)

MacBook Pro Mac14,6, Apple M2 Max, 32 GiB, macOS 26.7.3 (25G342).

The direct numeric date parser passes its timing rule. Unpadded
and ten-byte DATE improve beyond the paired floor in both ANSI
modes. The mixed DATE/DECIMAL statement is within its floor
with ANSI off and faster with ANSI on. No control is slower
beyond its floor.

The [phase probe](date-cast-1389-macos-profile.md) measured
the repeated validation before Arrow's date parser. The candidate
validates and extracts complete numeric fields together. It uses
Chrono for calendar validation and Arrow for the Date32 conversion.

**Apple Silicon, native macOS.** Query completion in ms per statement.

| Statement | Configuration | Staging | Candidate | Paired delta | A/A floor | Decision |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| Unpadded DATE | Native | 335.983 | 328.679 | -7.499 | 4.850 | faster |
| Unpadded DATE | Spark ANSI off | 355.003 | 342.232 | -12.500 | 3.362 | faster |
| Unpadded DATE | Spark ANSI on | 353.312 | 340.425 | -13.191 | 3.346 | faster |
| Ten-byte DATE | Native | 211.658 | 212.105 | 0.341 | 1.502 | within floor |
| Ten-byte DATE | Spark ANSI off | 219.648 | 214.915 | -4.732 | 1.287 | faster |
| Ten-byte DATE | Spark ANSI on | 219.493 | 214.688 | -4.600 | 1.177 | faster |
| Mixed DATE/DECIMAL | Native | 843.887 | 845.323 | 0.663 | 2.490 | within floor |
| Mixed DATE/DECIMAL | Spark ANSI off | 856.359 | 849.035 | -6.177 | 11.248 | within floor |
| Mixed DATE/DECIMAL | Spark ANSI on | 856.875 | 850.096 | -4.632 | 3.169 | faster |

**Apple Silicon, native macOS.** Paired Spark excess over native
in ns per input row.

| Statement | Configuration | Staging gap | Candidate gap | A/A floor |
| --- | --- | ---: | ---: | ---: |
| Unpadded DATE | Spark ANSI off | 3.207 | 2.311 | 1.257 |
| Unpadded DATE | Spark ANSI on | 2.931 | 2.163 | 0.695 |
| Ten-byte DATE | Spark ANSI off | 1.331 | 0.444 | 0.330 |
| Ten-byte DATE | Spark ANSI on | 1.346 | 0.417 | 0.267 |
| Mixed DATE/DECIMAL | Spark ANSI off | 1.818 | 0.619 | 2.143 |
| Mixed DATE/DECIMAL | Spark ANSI on | 1.843 | 0.883 | 0.565 |

Both pure DATE statements retain native gaps above their
floors. The mixed statement's ANSI-off gap is not resolvable
on Apple Silicon under this schedule. The unpadded collection
cost measured by the phase probe remains a separate cause.
Ten-byte collection was within its phase floor.

Both releases used Rust 1.96.0 and the same execution overlay.
`4704f188` has the same build inputs as staging `2dbc57fd`.
The candidate release was built at `c86461ba`.

All nine blocks completed 324 processes and retained 1,620
samples. The [protocol](../macos_rounds/1389-direct-date.md)
pins the frozen #1560 schedule. Its prose incorrectly lists
nine retained samples for every process. The pinned schedule
and the raw samples show nine per parsers process and three
per casts-date process. Both releases and both A/A arms use
the same count for each suite.

All three DATE tests passed, including 204 existing fixture
comparisons and 16,632 Arrow calendar comparisons. The SF1
preflight compared all 6,001,215 rows, executed SQL and Arrow
types, and cross-release source I/O. All 15 exact comparisons
passed. There were 18 observations and no exclusions.

- [Exact results and release receipts](date-cast-1389-macos-direct-preflight.json)
- [Full paired results](date-cast-1389-macos-direct-comparisons.json)
- [Compressed raw samples](date-cast-1389-macos-direct-samples.jsonl.gz)
