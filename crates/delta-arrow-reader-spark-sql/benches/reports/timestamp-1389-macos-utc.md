# Ordinary UTC TIMESTAMP LTZ trial on macOS (#1389)

MacBook Pro Mac14,6, Apple M2 Max, 32 GiB, macOS 26.7.3 (25G342).

The ordinary UTC candidate meets the timing rule. LTZ is faster
beyond the floor in both ANSI modes and both modes are faster than
their native reference. The flagged native NTZ result is a
build-layout shift under the instruction and state layout rule.

The [phase attribution](timestamp-ltz-1389-macos-profile.md)
isolated repeated UTC localization. The candidate validated the
same fields as staging and converted the UTC clock directly to
microseconds. The [protocol](../macos_rounds/1389-utc-timestamp.md)
pins the frozen full-SF1 SQL, schedules and both release sources.

**Apple Silicon, native macOS.** Query completion in ms per statement.

| Timestamp | Configuration | Staging | Candidate | Paired delta | A/A floor | Decision |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| LTZ | Native | 341.073 | 341.510 | 1.129 | 8.070 | within floor |
| LTZ | Spark ANSI off | 370.508 | 296.368 | -74.146 | 8.812 | faster |
| LTZ | Spark ANSI on | 370.486 | 295.580 | -74.652 | 3.036 | faster |
| NTZ | Native | 310.200 | 311.648 | 1.108 | 0.872 | build-layout shift |
| NTZ | Spark ANSI off | 310.771 | 297.383 | -13.404 | 2.556 | faster |
| NTZ | Spark ANSI on | 310.493 | 297.090 | -13.403 | 5.216 | faster |

**Apple Silicon, native macOS.** Paired Spark excess over the native
statement in ns per input row.

| Timestamp | Configuration | Staging gap | Candidate gap | A/A floor |
| --- | --- | ---: | ---: | ---: |
| LTZ | Spark ANSI off | 4.975 | -7.554 | 1.325 |
| LTZ | Spark ANSI on | 4.847 | -7.653 | 1.471 |
| NTZ | Spark ANSI off | 0.047 | -2.378 | 0.559 |
| NTZ | Spark ANSI on | 0.083 | -2.437 | 0.859 |

The native NTZ plan was the same in both executables. The projection
casts a CASE expression over a string-view input to a microsecond
timestamp without a zone and reads the same Delta scan. The
[layout receipt](timestamp-1389-macos-utc-layout.json) compares the
linked native query preparation, benchmark driver, projection,
CASE, cast, concat, remainder, Arrow string-to-timestamp and scan
routines. All 28,432 normalized instructions across 30 routines
match. Stack and object field offsets are identical, and the
checked rebased data records match after resolving their pointees.
The native NTZ +1.108 ms against a 0.872 ms floor is a build-layout
shift. The Spark NTZ observations are retained as controls.

The staging release at `03e6d16d` has the same build inputs as
staging `a195f53a`. The candidate release was built at `4704f188`.
Both used Rust 1.96.0 and the same execution driver overlay. Nine
reversed blocks completed 216 processes and retained 1,944 samples.

The existing differential passed all 72 inputs in five timestamp
configurations and both safety modes. The SF1 preflight passed
ten complete comparisons of 6,001,215 rows, including NULLs,
executed SQL and Arrow types, and cross-release source I/O. The
12 query outputs preserve UTC microseconds for LTZ and
microseconds without a timezone for NTZ.

- [Exact results and release receipts](timestamp-1389-macos-utc-preflight.json)
- [Full paired results](timestamp-1389-macos-utc-comparisons.json)
- [Compressed raw samples](timestamp-1389-macos-utc-samples.jsonl.gz)
