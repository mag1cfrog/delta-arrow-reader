# STRING DATE_FORMAT trial on macOS (#1389)

MacBook Pro Mac14,6, Apple M2 Max, 32 GiB, macOS 26.7.3 (25G342).

The fixed UTC string-array formatter passes the timing rule in both
ANSI modes. Query completion falls by 58% in each mode. The native
control stays within its floor. The candidate is also faster than
native in both ANSI modes in this session.

The [phase attribution](date-format-1389-macos-profile.md) isolated
the repeated formatter work. The candidate formats the parsed UTC
timestamps directly into an Arrow Utf8 builder for the fixed
`yyyy/MM/dd HH:mm` pattern. It uses the same input parsing, NULLs
and year-of-era rule. The [protocol](../macos_rounds/1389-date-format.md)
pins the full 6,001,215-row SF1 statement and the decision rule.

**Apple Silicon, native macOS.** Query completion in ms per statement.

| Configuration | Staging | Candidate | Paired delta | A/A floor | Decision |
| --- | ---: | ---: | ---: | ---: | --- |
| Native | 1098.222 | 1101.116 | 4.502 | 5.580 | within floor |
| Spark ANSI off | 1455.641 | 605.373 | -849.289 | 14.996 | faster |
| Spark ANSI on | 1452.450 | 605.357 | -848.240 | 14.433 | faster |

**Apple Silicon, native macOS.** Spark minus native in ns per input row.
The gaps are paired within each release. The A/A series gives the
floor for this reference comparison.

| Configuration | Staging gap | Candidate gap | A/A floor |
| --- | ---: | ---: | ---: |
| Spark ANSI off | 59.187 | -82.704 | 2.578 |
| Spark ANSI on | 58.944 | -82.481 | 3.032 |

Staging's sealed release at `4472df8d` has the same build inputs as
staging `1544eec5`. The candidate release was built at `03e6d16d`.
Both used Rust 1.96.0 and the same execution driver overlay. Nine
reversed blocks completed 108 processes and retained 972 samples.

The differential over the 72 committed timestamp inputs and five
additional cases passed for Utf8, LargeUtf8 and Utf8View. The SF1
preflight passed all five full comparisons across the two releases
and the native reference, covering values, NULLs, executed SQL and
Arrow types, and cross-release source I/O. All six query outputs
had 6,001,215 rows with Arrow types Int64, Int32 and Utf8.

Formatting, strict root-package Clippy and the full Spark workspace
suite passed at the candidate product commit. The remaining commits
add the protocol and evidence.

- [Exact results and release receipts](date-format-1389-macos-preflight.json)
- [Full paired results](date-format-1389-macos-comparisons.json)
- [Compressed raw samples](date-format-1389-macos-samples.jsonl.gz)
