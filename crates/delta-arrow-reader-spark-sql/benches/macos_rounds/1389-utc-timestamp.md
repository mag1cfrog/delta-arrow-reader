# Ordinary UTC TIMESTAMP LTZ trial on Apple Silicon (#1389)

MacBook Pro Mac14,6, Apple M2 Max, 32 GiB, macOS 26.7.3 (25G342).

The [local phase probe](../reports/timestamp-ltz-1389-macos-profile.md)
found an excess in localizing already-validated UTC input. The
candidate converts an ordinary, whole-second UTC timestamp to
microseconds directly after the existing field validation. It
keeps the existing conversion for other zones and the grammar
fallback.

The [manifest](1389-utc-timestamp.json) pins both release sources
and the unchanged #1560 SF1 inputs. Use the sealed staging release
from the date format trial. Its build inputs match current staging.
Build the candidate with Rust 1.96.0 and the same execution overlay.

Before timing require formatting, strict root-package Clippy and
the existing differential over all 72 timestamp captures in both
safety modes, covering UTC, two non-UTC zones, an invalid zone
and NTZ. Preflight all 6,001,215 rows in both ANSI modes against
staging and the native reference, with values, NULLs, executed
types and cross-release source I/O. A reference mismatch excludes
the affected statement and goes to the report.

Use the [native session controls](1389-borrowed-tokens.md).
Run LTZ and NTZ with both ANSI modes and their native references.
NTZ uses the shared parser and is a control. Collect nine
reversed four-arm blocks with two staging A/A arms and nine
retained query-completion samples per process. Bound each process
to five minutes and the session to three hours.

Merge if ordinary LTZ is faster beyond the floor in at least one
ANSI mode, neither mode is slower and all controls pass. For a
control above its floor, compare the normalized instructions and
state layouts on its executed path. Incomplete observations or a
failed gate hold the candidate. Require review and the full Spark
workspace tests before merging.
