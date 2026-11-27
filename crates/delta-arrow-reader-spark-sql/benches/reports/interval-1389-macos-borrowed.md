# Borrowed interval token trial on macOS (#1389)

MacBook Pro Mac14,6, Apple M2 Max, 32 GiB, macOS 26.7.3 (25G342).
Both releases were built natively. Staging was `e6b4b4fd` and the
archived patch was reapplied at `cf16828a` without product edits.
[Committed protocol and rule](../macos_rounds/1389-borrowed-tokens.md).

Hold `perf/1389-macos-borrowed-tokens`. Both ANSI modes for day-time
and calendar were within their floors. Calendar ANSI off missed its
floor by 0.007 ms. All five controls and the other four Spark cast
cells stayed within their floors. The target gains are not resolvable
on Apple Silicon under this schedule.

**Apple Silicon, native macOS.** Query completion in ms per statement.
The last column gives the paired change per row over 8,184 rows.

| Cast or control | Configuration | Staging | Candidate | Paired delta | A/A floor | Delta ns/row |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Day-time | Spark ANSI off | 37.928 | 36.820 | -0.948 | 2.211 | -115.9 |
| Day-time | Spark ANSI on | 37.888 | 36.900 | -0.933 | 2.048 | -114.0 |
| Calendar | Spark ANSI off | 41.480 | 40.028 | -1.433 | 1.440 | -175.1 |
| Calendar | Spark ANSI on | 41.271 | 39.940 | -1.327 | 1.398 | -162.1 |
| Calendar | Native | 32.288 | 32.026 | -0.219 | 1.780 | -26.7 |
| Year-month | Spark ANSI off | 35.471 | 35.023 | -0.423 | 1.809 | -51.7 |
| Year-month | Spark ANSI on | 35.476 | 34.817 | -0.427 | 1.265 | -52.2 |
| Year-month | Native | 32.029 | 32.039 | 0.035 | 1.627 | 4.3 |
| Quoted year-month | Spark ANSI off | 39.865 | 39.812 | -0.241 | 0.847 | -29.4 |
| Quoted year-month | Spark ANSI on | 39.621 | 39.722 | -0.086 | 0.962 | -10.5 |
| Quoted year-month | Native | 31.951 | 32.008 | -0.029 | 1.175 | -3.5 |
| Day-time input | Spark ANSI off | 32.582 | 32.660 | 0.062 | 2.868 | 7.6 |
| Day-time input | Spark ANSI on | 32.628 | 32.613 | -0.016 | 2.078 | -2.0 |

**Apple Silicon, native macOS.** Paired excess in ns per row, Spark
ANSI off / on. Day-time uses its same-mode input projection. The
other casts use the native query.

| Cast | Staging gap | Candidate gap | A/A floor for the gap |
| --- | ---: | ---: | ---: |
| Day-time | 649.0 / 649.2 | 522.3 / 524.2 | 434.6 / 344.3 |
| Calendar | 1,105.9 / 1,095.7 | 975.4 / 1,009.5 | 93.5 / 113.1 |
| Year-month | 418.7 / 419.9 | 347.8 / 348.4 | 141.4 / 133.0 |
| Quoted year-month | 960.2 / 944.6 | 933.0 / 941.7 | 87.4 / 115.3 |

The 20 existing Sail analyzer tests passed. The exact preflight made
26 observations and 21 decoded row and type comparisons. The four
day-time input projection I/O comparisons passed. Nine blocks
completed 468 processes and retained 4,212 samples. There were no
above-floor slowdown flags in the 13 paired cells.

- [Exact result, fixture and release receipts](interval-1389-macos-borrowed-preflight.json)
- [Full paired results](interval-1389-macos-borrowed-comparisons.json)
- [Compressed raw samples](interval-1389-macos-borrowed-samples.jsonl.gz)
