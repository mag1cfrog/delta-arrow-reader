# #1390 archived scan ordinal trial on Apple Silicon

MacBook Pro Mac14,6, Apple M2 Max, 32 GiB, macOS 26.7.3 (25G342).

The [manifest](1390-scan-ordinals.json) pins staging, the reapplied source
from the scan ordinal bundle, the SQL inventory, and #1560's schedule.
Use the same Rust 1.96.0 release recipe and benchmark overlays for both
executables. The overlay sets and verifies user-interactive QoS on the
benchmark main thread and any workers. Use mains power and High Power
mode, prevent sleep with `caffeinate`, and run without other builds or
benchmarks. CPU pinning, the Linux address-space cap, and peak RSS are
omitted as in the baseline.

Before timing, match all 678 canonical traces per build and all 120
typed result checks per build on the committed fixture. Run the archived
projection differential tests. If a trace, result, or executed type
differs, hold the candidate and record the difference before timing.

Time all 30 paired and stress statements in Spark ANSI off, Spark ANSI
on, native-configured, and native-plain. In each cell run staging,
candidate, and two more staging arms. Run nine blocks, reversing cell
and arm order every block. Each process uses the driver's two warmups
and nine retained samples. Bound the session at two hours and each
process at five minutes. Record logical, physical and total planning
in ns per statement. No query executes inside the timing loop.

For each phase of each cell, use the median of the nine paired
differences of process medians. The floor is the greatest of the
absolute difference of the two A/A medians and the half-range of
each A/A arm. Retain and report incomplete cells. They are ineligible
to pass the merge gate.

The candidate passes if the 400-column physical phase is below
minus its floor in either ANSI mode, neither mode is above its floor,
and no other timed cell or phase exceeds its floor after applying
the layout rule. For an above-floor control outside the intended
change, compare the executed routines using the method documented in
`macos_addendum/literal-utc-layout.json`, including stack and state
offsets. Classify it as a layout shift only when the comparison shows
agreement. A demonstrated instruction difference or an inconclusive
comparison leaves the gate in force. Typed differential tests, review,
formatting, Clippy and the full Spark workspace tests are required
before merge. Hold the branch with the results if a requirement fails.
