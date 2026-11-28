# Batch state for ordered Float64 TRY_AVG (#1516)

MacBook Pro Mac14,6, Apple M2 Max, 32 GiB, macOS 26.7.3 (25G342).

The [profile](../reports/try-avg-1516-macos-profile.md) measured an
outer update gap of 0.703 ns per input row. The compiled Spark loop
checks the count and writes accumulator state for each valid value.
The candidate checks the combined count once and keeps the ordered
sum local. A possibly overflowing batch uses the original row loop.

The [manifest](1516-try-avg-batch-state.json) pins the two native
Rust 1.96.0 releases. Staging uses the sealed #1601 candidate. Its
product inputs match the current staging head. Both binaries use the
same harness overlay.

Require exact complete output, executed types and input guards in
native and both ANSI modes for the original and restricted million-row
fixtures. Require restricted Spark/native agreement. Run the focused
row-loop differential over sliced inputs, NULLs, signed zeros,
cancellation, infinity and count overflow. A preflight mismatch
excludes its statement from timing and holds the candidate.

Follow the [paired protocol](1516-exact-percentile-frame.md) for power
and thread controls, nine reversed blocks, nine retained samples per
process and two staging A/A arms per cell. Use the same metric, paired
median and floor. Time at most 216 processes, each bounded to
130 seconds, in a session capped at 90 minutes. Commit all emitted
raw samples. A change within its floor is not resolvable on Apple Silicon.

The aggregate is the target. The Float64 Spark window is an affected
control. The native references are untouched controls. Merge only if
the target improves beyond its floor in at least one ANSI mode and no
affected cell is slower. For a native control above its floor, compare
the normalized instructions and state layouts. If they match, classify
the difference as a build-layout shift. Hold for any remaining
slowdown, incomplete run, mismatch or failed workspace gate.
