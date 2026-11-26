# #1390 archived physical scan renames on Apple Silicon

MacBook Pro Mac14,6, Apple M2 Max, 32 GiB, macOS 26.7.3 (25G342).

The [manifest](1390-physical-renames.json) pins the reapplied archive and
staging head. The staging binary is the candidate release from the previous
scan ordinal session, built on this Mac at `1d74a82d`. The manifest lists
every path changed between that build and staging `cd8351ee`. All release benchmark build
inputs are byte-identical across those two commits. Both releases use
Rust 1.96.0 and the same benchmark overlays. The current session measures
all four arms afresh.

Use the [scan ordinal protocol](1390-scan-ordinals.md) for the host controls
and schedule. The Mac runs on mains power in High Power mode with a
`caffeinate` assertion. The drivers set and verify user-interactive QoS on
their main and worker threads. The four arms run in turn in nine reversed
blocks. Each process retains nine samples after two warmups. The session
covers 30 statements and four configurations and is bounded at two hours,
with five minutes per process. Use both native configurations as controls
for the changed planner. Record logical, physical, and total planning in
ns per statement. Timing ends before any query executes.

Before timing, compare all 678 canonical traces and pass 120 typed result
checks per build, along with the archive's differential test. Hold the
candidate before timing if a comparable trace, result, or executed type
differs.

Compare paired process medians and compute each A/A floor as the greatest
of the absolute difference between the A/A medians and the two half-ranges. The
target is the 400-column physical phase. At least one ANSI mode must be
faster beyond its floor. Neither mode and no other phase or cell may be
slower beyond its floor. Apply the documented normalized-instruction and
state-layout check to any above-floor control outside the intended
change. A demonstrated code difference or inconclusive check leaves the
gate in force. Incomplete cells cannot pass. The candidate also requires
review, formatting, Clippy, and the full Spark workspace tests before
merge. Hold the branch with the numbers and reason if any gate fails.
