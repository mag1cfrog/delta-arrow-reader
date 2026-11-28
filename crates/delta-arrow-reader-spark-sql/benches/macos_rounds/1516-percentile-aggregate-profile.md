# Approximate-percentile aggregate attribution on Apple Silicon (#1516)

MacBook Pro Mac14,6, Apple M2 Max, 32 GiB, macOS 26.7.3 (25G342).

The [paired window report](../reports/percentile-1516-macos-exact.md)
found an aggregate gap of about 4.7 ns per input row after the window
change. Both aliases use the same aggregate factory and quantile summary.
Profile the ungrouped APPROX_PERCENTILE statement as their representative.
The [manifest](1516-percentile-aggregate-profile.json) pins the staging
input to the release and the unchanged restricted SQL.

The [aggregate adapter](percentile_aggregate_profile_1516.py) reuses
the frame profile's harness in a detached scratch checkout. It adds
clocks around summary flush, sort and compression and reports the
outer update, merge, state and evaluation clocks. Build a native
Rust 1.96.0 release. Before timing, compare exact decoded rows and
executed types with the uninstrumented release in native and both
ANSI modes. Require the source guards to consume the full fixture.

Use mains power, High Power mode and `caffeinate`. The main
thread and all benchmark workers set and verify user-interactive
QoS. CPU pinning, the Linux address-space limit and peak RSS
are omitted. Check for other builds and benchmarks before each
process.

Run two A/A arms of the instrumented staging release for each
mode in nine reversed blocks. Keep nine query/projection pairs
per process after warm-up. Limit a timing process to 130 seconds
and the session to one hour. Keep every emitted timed case
record and phase snapshot. Incomplete phase records fail the
attribution check.

Take the median within each process and report the balanced
two-arm median and the A/A floor. Sum the nonoverlapping
aggregate phases within each record before taking medians. Use
the aligned native and Spark process medians to judge which
outer phase exceeds the same-session A/A floors. Sort is nested
in the buffer flush. Flush and compression can overlap each
other and the outer phases. Report their cost separately.
A cost within its floor is not resolvable on Apple Silicon.
The probe includes clock overhead. Report the measured split
and use the uninstrumented run for the actual native gap.
