# Utf8View DATE collection trial on Apple Silicon (#1389)

MacBook Pro Mac14,6, Apple M2 Max, 32 GiB, macOS 26.7.3 (25G342).

The [phase probe](../reports/date-cast-1389-macos-profile.md) measured
1.158 ns per input row of unpadded DATE collection overhead at a
0.274 ns floor. The [direct conversion trial](../reports/date-cast-1389-macos-direct.md)
still had an unpadded DATE native gap in both ANSI modes. This change
reserves the Date32 array for the input length while preserving the
same row parser, NULLs and first strict failure.

The [manifest](1389-date-collection.json) pins both native release
sources and the [existing SF1 case adapter](sf1_date.py). Use the
sealed staging release from the direct DATE trial and build the
candidate with Rust 1.96.0 and the same execution overlay.
The unpadded DATE statement is the target. The ten-byte and
mixed DATE/DECIMAL statements use the same array path and
each runs in both ANSI modes and as a native control.

Before timing require formatting, strict root-package Clippy
and the DATE tests, including the array differential over the
committed captures. Preflight all nine cells on 6,001,215 rows.
Compare exact values, NULLs and executed types between builds
and against the native references, with source I/O checked
between builds. A reference mismatch excludes its statement.

Follow the [native session controls](1389-borrowed-tokens.md).
Run nine reversed four-arm blocks with two staging A/A arms.
Use the pinned #1560 sample counts, nine retained samples
for a parsers process and three for a casts-date process.
Cap each process at five minutes and the session at three hours.

Merge if the paired unpadded DATE result is faster beyond the
floor in at least one ANSI mode and all Spark cells pass.
Apply the instruction and state layout rule for an above-floor
native slowdown. Missing observations or a failed gate hold
the candidate. Require review and a passing full Spark workspace
suite before merging.
