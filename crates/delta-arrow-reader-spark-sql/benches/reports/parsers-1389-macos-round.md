# #1389 Apple Silicon round

MacBook Pro Mac14,6, Apple M2 Max, 32 GiB, macOS 26.7.3 (25G342).

The Mac round covered the assigned SF1 parser and DATE cases.
Five product changes passed their timing and correctness gates.
The archived borrowed-token change stays held. #1389 remains open
for the Linux x86 confirmation and the residual quoted interval
and unpadded DATE gaps.

| Change or archived trial | Result and evidence |
| --- | --- |
| Archived borrowed interval tokens | [#1581](https://github.com/mag1cfrog/delta-arrow-reader/pull/1581) is held at `perf/1389-macos-borrowed-tokens`. Both day-time and calendar ANSI pairs were within their floors. The [report and samples](interval-1389-macos-borrowed.md) merged in #1582. |
| Direct interval fields | [#1584](https://github.com/mag1cfrog/delta-arrow-reader/pull/1584) removed the per-row lexer and syntax tree for complete numeric units after [#1583's profile](interval-1389-macos-profile.md). The [paired trial](interval-1389-macos-direct.md) passed in both ANSI modes for all four interval shapes. |
| STRING DATE_FORMAT | [#1586](https://github.com/mag1cfrog/delta-arrow-reader/pull/1586) writes the fixed UTC minute pattern directly after the same timestamp parse. The [attribution](date-format-1389-macos-profile.md) and [paired trial](date-format-1389-macos.md) record the formatter cost and gains. |
| Ordinary UTC TIMESTAMP LTZ | [#1588](https://github.com/mag1cfrog/delta-arrow-reader/pull/1588) removed repeated UTC localization after [#1587's profile](timestamp-ltz-1389-macos-profile.md). The [trial](timestamp-1389-macos-utc.md) classified the native NTZ flag as a build-layout shift across 30 routines and 28,432 instructions. |
| Direct DATE fields | [#1590](https://github.com/mag1cfrog/delta-arrow-reader/pull/1590) combined digit validation and field extraction after the [DATE phase probe](date-cast-1389-macos-profile.md). Both shapes improved in both modes. The [trial](date-cast-1389-macos-direct.md) records the frozen sample counts and the incorrect count in its protocol prose. |
| DATE array collection | [#1591](https://github.com/mag1cfrog/delta-arrow-reader/pull/1591) reserved the Date32 array for the input length. The [trial](date-cast-1389-macos-collection.md) passed on the unpadded ANSI-on gain and the other controls. |

The interval rows use the 8,184-row subset. Day-time uses the input
projection of the same ANSI mode as its reference. The remaining
statements each use 6,001,215 rows and a native reference. The
unquoted interval and NTZ rows also record the controls. The DATE
rows below use the collection session and start with the direct
parser already present. The preceding DATE trial gives its own
before and after figures.

**Apple Silicon, native macOS.** Each row uses one paired session.
Query completion medians are in ms. Paired gaps and floors are in ns per input row.

| Statement | ANSI | Spark before / after | Reference before / after | Gap before / after | A/A gap floor | Status |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| Quoted year-month interval | off | 41.990 / 35.336 | 34.059 / 33.919 | 943.513 / 156.005 | 136.374 | still above reference |
| Quoted year-month interval | on | 42.206 / 35.803 | 34.059 / 33.919 | 983.112 / 128.167 | 119.690 | still above reference |
| Day-time interval | off | 39.918 / 35.373 | 35.267 / 34.888 | 588.674 / 59.313 | 251.688 | not resolvable on Apple Silicon |
| Day-time interval | on | 39.544 / 34.895 | 34.871 / 34.965 | 564.078 / -43.622 | 232.532 | not resolvable on Apple Silicon |
| Calendar interval | off | 43.926 / 35.304 | 33.964 / 34.003 | 1142.188 / 115.826 | 168.724 | not resolvable on Apple Silicon |
| Calendar interval | on | 43.461 / 35.006 | 33.964 / 34.003 | 1128.167 / 87.982 | 127.884 | not resolvable on Apple Silicon |
| Year-month interval | off | 37.042 / 35.053 | 34.151 / 34.492 | 393.782 / 98.337 | 147.618 | not resolvable on Apple Silicon |
| Year-month interval | on | 37.190 / 34.905 | 34.151 / 34.492 | 407.436 / 26.123 | 133.884 | not resolvable on Apple Silicon |
| STRING DATE_FORMAT | off | 1455.641 / 605.373 | 1098.222 / 1101.116 | 59.187 / -82.704 | 2.578 | faster than reference |
| STRING DATE_FORMAT | on | 1452.450 / 605.357 | 1098.222 / 1101.116 | 58.944 / -82.481 | 3.032 | faster than reference |
| STRING TIMESTAMP LTZ | off | 370.508 / 296.368 | 341.073 / 341.510 | 4.975 / -7.554 | 1.325 | faster than reference |
| STRING TIMESTAMP LTZ | on | 370.486 / 295.580 | 341.073 / 341.510 | 4.847 / -7.653 | 1.471 | faster than reference |
| STRING TIMESTAMP NTZ | off | 310.771 / 297.383 | 310.200 / 311.648 | 0.047 / -2.378 | 0.559 | faster than reference |
| STRING TIMESTAMP NTZ | on | 310.493 / 297.090 | 310.200 / 311.648 | 0.083 / -2.437 | 0.859 | faster than reference |
| Unpadded DATE | off | 342.834 / 338.694 | 330.472 / 325.975 | 2.599 / 2.430 | 1.043 | still above reference |
| Unpadded DATE | on | 341.636 / 337.193 | 330.472 / 325.975 | 2.414 / 2.048 | 1.425 | still above reference |
| Ten-byte DATE | off | 215.022 / 210.372 | 211.998 / 211.327 | 0.516 / -0.159 | 0.306 | not resolvable on Apple Silicon |
| Ten-byte DATE | on | 214.629 / 210.214 | 211.998 / 211.327 | 0.462 / -0.180 | 0.486 | not resolvable on Apple Silicon |
| Mixed DATE/DECIMAL | off | 851.509 / 842.052 | 843.983 / 842.206 | 0.997 / 0.120 | 1.273 | not resolvable on Apple Silicon |
| Mixed DATE/DECIMAL | on | 852.500 / 842.140 | 843.983 / 842.206 | 1.150 / 0.137 | 1.327 | not resolvable on Apple Silicon |

The unpadded DATE and quoted year-month gaps still exceed their
floors in both ANSI modes. For unpadded DATE, the round tried the
two measured causes, guarded dispatch and array collection. For
quoted year-month it held borrowed tokens and replaced the general
lexer and syntax-tree work on the complete numeric path. The
post-change measurements leave a whole-query residual for each
shape. This round assigned no more of that residual to a named
operation.

The day-time projection excess and the native gaps for calendar,
unquoted year-month, ten-byte DATE and mixed DATE/DECIMAL are not
resolvable on Apple Silicon at their listed floors. The unpadded
ANSI-off effect of the preallocated collection is also not
resolvable on Apple Silicon at its 5.156 ms cell floor. Keep those
measurements and all parts of this round for the x86 confirmation.

All preflights preserved the values, NULLs, executed types and
cross-build source I/O in scope. The product branches passed
formatting, strict root-package Clippy and the full Spark
workspace suite. Each trial report links the raw compressed
samples, build identities and its fixed protocol.
