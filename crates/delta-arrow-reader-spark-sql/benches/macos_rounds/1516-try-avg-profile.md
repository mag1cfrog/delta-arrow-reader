# Ordered Float64 TRY_AVG attribution on Apple Silicon (#1516)

MacBook Pro Mac14,6, Apple M2 Max, 32 GiB, macOS 26.7.3 (25G342).

The [restricted baseline](../reports/functions-1516-macos-restricted.md)
measured TRY_AVG about 0.671 ns per input row above native AVG in each
ANSI mode. This profile measures the outer update, merge, state and
evaluation phases and Spark's ordered Float64 update inside the outer
update. The preflight checks that the source consumes every input row.

The [manifest](1516-try-avg-profile.json) pins current staging and the
frozen restricted SQL. The [adapter](try_avg_profile_1516.py) builds a
Rust 1.96.0 release with scratch clocks. Before timing, require exact
decoded rows, executed types, physical plans and source guards against
the uninstrumented #1601 release in native and both ANSI modes.

Use mains power, High Power mode and `caffeinate`. The main thread and
all benchmark workers set and verify user-interactive QoS. Linux CPU
pinning, the address-space limit and peak RSS are omitted. Check for
other builds and benchmarks before each process.

Run two A/A arms of the instrumented staging release in each mode.
Use nine reversed blocks with nine retained query/projection pairs
after warm-up. Cap each process at 130 seconds and the session at
one hour. Commit the raw samples and phase snapshots. Incomplete
records make the attribution inconclusive.

Report phase medians and A/A floors per input row. For outer
update, merge, state and evaluation, compare the median of
aligned Spark-minus-native process differences with the larger
of the two modes' A/A floors. Report the full ordered Float64
update as a nested cost and record the number of valid values.
Use the assembly and source of the uninstrumented update to
identify the checks and the exact order of floating-point adds.
Compare against the uninstrumented baseline for the native gap.
