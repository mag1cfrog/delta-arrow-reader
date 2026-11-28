# Ordinary UTC TIMESTAMP LTZ attribution on macOS (#1389)

MacBook Pro Mac14,6, Apple M2 Max, 32 GiB, macOS 26.7.3 (25G342).

The ordinary UTC route in `TimestampParser::localize` resolves the
same default zone for each row, then calls the general localization
helper. On the already-validated UTC rows, direct conversion
returned the same microseconds. The isolated difference identifies
one change for the ordinary UTC route.

**Apple Silicon, native macOS.** Time in ns per input row.

| Phase | Spark | Reference | Gap | A/A floor |
| --- | ---: | ---: | ---: | ---: |
| Complete cast | 39.650 | 36.239 | 3.395 | 0.564 |
| UTC localization | 31.197 | 1.938 | 29.259 | 0.284 |

The complete cast compares Spark with the native Arrow cast on the
same Utf8View array. The UTC localization phase compares the Spark
helper with direct UTC conversion on preparsed inputs. The isolated
localization difference is 8.6 times the net array gap. The reference
and amount of remaining work differ between the two pairs. The
isolated figure cannot be added to or subtracted from the full cast.
The field parser cost 7.844 ns/row in its separate phase.

The [committed protocol](../macos_rounds/1389-ltz-profile.md) used
staging `155c7b98`. All 18 processes checked exact Spark/native UTC
microseconds, NULLs and type in both ANSI modes and ran the
localization equivalence check. Nine reversed blocks retained
810 phase samples. Formatting, strict root-package Clippy and the
full Spark workspace passed on the same product and test inputs at
`03e6d16d`.

- [Full paired results](timestamp-ltz-1389-macos-profile-comparisons.json)
- [Compressed raw samples and executable receipt](timestamp-ltz-1389-macos-profile-samples.jsonl.gz)
