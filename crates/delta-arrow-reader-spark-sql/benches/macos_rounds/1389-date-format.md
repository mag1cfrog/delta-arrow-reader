# STRING DATE_FORMAT trial on Apple Silicon (#1389)

MacBook Pro Mac14,6, Apple M2 Max, 32 GiB, macOS 26.7.3 (25G342).

The [local phase probe](../reports/date-format-1389-macos-profile.md)
located the main excess in formatting the prepared UTC timestamp
array. The candidate writes the existing constant-pattern output
directly to Utf8 after the same timestamp parser.

The [manifest](1389-date-format.json) pins both release sources, the
fixture receipt and the unchanged #1560 parser statement schedule.
Use the sealed staging execution binary from the direct interval
trial. Its product and driver inputs are byte-identical to current
staging. Build the candidate with Rust 1.96.0 using the identical
execution overlay. The frozen parsers suite calls this string
formatter in DATE_FORMAT alone. Run its native reference and both
ANSI modes over all 6,001,215 SF1 rows.

Before timing require the focused differential over committed
timestamp inputs, formatting and strict root-package Clippy. The
SQL preflight compares every value, NULL and executed type across
the two releases and against the native reference. It verifies
source I/O between releases. A reference mismatch excludes the
statement and goes to the report.

Use the [native session controls](1389-borrowed-tokens.md) and nine
reversed four-arm blocks with two staging A/A arms. Retain nine
query-completion samples per process. Bound each process to five
minutes and the session to three hours. Commit all retained samples.

Merge if the paired median is faster beyond the floor in at least
one ANSI mode and neither mode is slower. If the native reference
is slower beyond its floor, apply the normalized instruction and
state layout rule. Incomplete observations or a failed gate hold
the candidate. Require review and the full Spark workspace tests
before merging.
