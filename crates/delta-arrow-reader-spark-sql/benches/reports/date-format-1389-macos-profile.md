# STRING DATE_FORMAT attribution on macOS (#1389)

MacBook Pro Mac14,6, Apple M2 Max, 32 GiB, macOS 26.7.3 (25G342).

The constant-format string array formatter accounts for 97.4% of
the sum of the two measured phase gaps. The probe formats the same
UTC timestamp array through both Spark and DataFusion TO_CHAR.
Spark's array formatter calls the general row formatter for each
non-NULL value. It resolves the zone and abbreviation, builds a
string through the pattern's fields and assembles those strings into
an Arrow array. Native prepares its formatter for the array. The
formatting phase provides the first candidate's scope.

**Apple Silicon, native macOS.** Time in ns per input row.

| Phase | Spark | Native | Paired gap | A/A floor | Decision |
| --- | ---: | ---: | ---: | ---: | --- |
| Parsing | 37.095 | 34.277 | 2.930 | 0.605 | slower |
| Formatting | 348.597 | 239.849 | 108.556 | 16.206 | slower |

Parsing has a smaller excess of 2.930 ns/row. The separate grammar
control cost 228.401 ns/row. It covers a different path and does
not enter the measured gaps. This split cannot assign the small
parsing excess to specific operations. It also excludes planning,
scan and projection. The query-level decision will use a separate
SF1 comparison.

The [committed protocol](../macos_rounds/1389-date-format-profile.md)
pins staging `358f7047`, the unchanged synthetic fixture and the
probe overlay. Its input and formatted value, NULL and Utf8
checks passed in all 18 processes. Nine reversed A/A blocks
retained 810 phase samples. Formatting, strict root-package Clippy
and the full Spark workspace suite passed on the identical product
and test inputs at `4472df8d`.

- [Full paired results](date-format-1389-macos-profile-comparisons.json)
- [Compressed raw samples and executable receipt](date-format-1389-macos-profile-samples.jsonl.gz)
