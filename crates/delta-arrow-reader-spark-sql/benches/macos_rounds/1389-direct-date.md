# Direct numeric DATE trial on Apple Silicon (#1389)

MacBook Pro Mac14,6, Apple M2 Max, 32 GiB, macOS 26.7.3 (25G342).

The [local probe](../reports/date-cast-1389-macos-profile.md)
located repeated digit validation in the unpadded and ten-byte
DATE casts. The candidate reads the fields during validation,
then uses the same Chrono calendar check and Arrow day conversion.
The existing route handles inputs that the direct path declines.

The [manifest](1389-direct-date.json) pins both native releases
and the frozen SF1 inputs. Use the staging release sealed
for the ordinary UTC timestamp trial and build the candidate
with the same Rust 1.96.0 release and execution overlay.

Run `date_fallback` and `cast_date` in both ANSI modes and their
native references. The mixed `cast_strings` statement also
executes the DATE path. Include all three of its configurations
through [the case adapter](sf1_date.py), which reads the earlier
cast statement without modifying the earlier manifests.

Before timing, require formatting, strict root-package Clippy,
the existing DATE capture tests and the Arrow calendar boundary
differential. All nine SQL cells cover the 6,001,215 SF1 rows.
Preflight the exact decoded values, NULLs, executed SQL and Arrow
types and cross-release source I/O. Check both ANSI modes against
each statement's native reference. A mismatch excludes the
statement and goes to the report.

Follow the [native session controls](1389-borrowed-tokens.md).
Run nine reversed four-arm blocks with two staging A/A arms and
nine retained query-completion samples per process. Bound each
process to five minutes and the session to three hours.

This is one change to the shared DATE converter. Require a
gain beyond the floor in at least one target Spark DATE cell
and no slower Spark cell. Apply the instruction and state layout
rule to a flagged native control. Incomplete observations or a
failed gate hold the candidate. Require review and the full
Spark workspace tests before merging.
