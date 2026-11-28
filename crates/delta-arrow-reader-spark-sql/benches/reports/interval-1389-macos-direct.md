# Direct numeric interval trial on macOS (#1389)

MacBook Pro Mac14,6, Apple M2 Max, 32 GiB, macOS 26.7.3 (25G342).

The direct parser meets the merge rule. Both ANSI modes are faster
beyond the paired floor for day-time and quoted year-month. The
calendar and unquoted year-month casts also improve in both modes.
The five untouched controls are within their floors.

Staging's release was built at `e6b4b4fd`. The build inputs were
identical to staging at `8f92fecf`, whose intervening commits added
only benchmark evidence and adapters. The candidate release was
built at `4472df8d`. Both used Rust 1.96.0 with the same execution
driver overlay. The [committed protocol](../macos_rounds/1389-direct-interval.md)
contains the fixed schedule and merge rule.

**Apple Silicon, native macOS.** Query completion in ms per statement.

| Cast or control | Configuration | Staging | Candidate | Paired delta | A/A floor | Decision |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| Day-time | Spark ANSI off | 39.918 | 35.373 | -4.870 | 1.353 | faster |
| Day-time | Spark ANSI on | 39.544 | 34.895 | -4.831 | 0.934 | faster |
| Calendar | Native | 33.964 | 34.003 | -0.168 | 0.777 | within floor |
| Calendar | Spark ANSI off | 43.926 | 35.304 | -8.460 | 1.540 | faster |
| Calendar | Spark ANSI on | 43.461 | 35.006 | -8.500 | 1.025 | faster |
| Year-month | Native | 34.151 | 34.492 | 0.182 | 1.428 | within floor |
| Year-month | Spark ANSI off | 37.042 | 35.053 | -2.162 | 1.077 | faster |
| Year-month | Spark ANSI on | 37.190 | 34.905 | -2.104 | 0.963 | faster |
| Quoted year-month | Native | 34.059 | 33.919 | 0.281 | 0.974 | within floor |
| Quoted year-month | Spark ANSI off | 41.990 | 35.336 | -6.548 | 1.087 | faster |
| Quoted year-month | Spark ANSI on | 42.206 | 35.803 | -6.307 | 1.013 | faster |
| Day-time input | Spark ANSI off | 35.267 | 34.888 | -0.029 | 1.453 | within floor |
| Day-time input | Spark ANSI on | 34.871 | 34.965 | 0.081 | 1.393 | within floor |

**Apple Silicon, native macOS.** Paired excess in ns per row.
Day-time uses the same-mode input projection. The other casts use native.

| Cast | Configuration | Staging gap | Candidate gap | A/A floor for the gap |
| --- | --- | ---: | ---: | ---: |
| Day-time | Spark ANSI off | 588.7 | 59.3 | 251.7 |
| Day-time | Spark ANSI on | 564.1 | -43.6 | 232.5 |
| Calendar | Spark ANSI off | 1142.2 | 115.8 | 168.7 |
| Calendar | Spark ANSI on | 1128.2 | 88.0 | 127.9 |
| Year-month | Spark ANSI off | 393.8 | 98.3 | 147.6 |
| Year-month | Spark ANSI on | 407.4 | 26.1 | 133.9 |
| Quoted year-month | Spark ANSI off | 943.5 | 156.0 | 136.4 |
| Quoted year-month | Spark ANSI on | 983.1 | 128.2 | 119.7 |

The day-time projection excess and the native gaps for calendar
and unquoted year-month are not resolvable on Apple Silicon at their
session floors. Quoted year-month still exceeds its native reference
by 156.0 and 128.2 ns/row against floors of 136.4 and 119.7 ns/row.
This is the remaining whole-query difference after the direct parser.
The [local phase attribution](interval-1389-macos-profile.md) used the
pre-change parser. It cannot assign the remaining post-change gap to
individual operations.

Independent staging and candidate builds matched all 105 parser
values, interval qualifiers, failures and diagnostic strings. The
preflight compared the complete 8,184 SQL rows and executed types
for 13 cells across both releases. All 21 decoded comparisons and
four projection source-I/O checks passed. The nine blocks completed
468 processes and retained all 4,212 samples.

The scoped decoded-literal parser test keeps its original assertions.
The test-only counter moved to the entry of the decoded-string scope
so it records a call when the direct path finishes within that scope.
The original 20 analyzer tests and the added differential passed.
Formatting, strict root-package Clippy and the full Spark workspace
suite passed on the product commit. Subsequent commits add evidence.

- [Exact result, fixture and release receipts](interval-1389-macos-direct-preflight.json)
- [Full paired results](interval-1389-macos-direct-comparisons.json)
- [Compressed raw samples](interval-1389-macos-direct-samples.jsonl.gz)
