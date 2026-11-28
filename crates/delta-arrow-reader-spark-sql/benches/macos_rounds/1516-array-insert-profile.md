# Literal second-position ARRAY_INSERT attribution (#1516)

MacBook Pro Mac14,6, Apple M2 Max, 32 GiB, macOS 26.7.3 (25G342).

The [restricted baseline](../reports/functions-1516-macos-restricted.md)
measured ARRAY_INSERT about 3.6 ns per input row above native. Its
direct interior copy from the earlier round is already in staging.
The surrounding CASE still evaluates length predicates and chooses
which rows may evaluate the value. Measure that path before changing it.

The [manifest](1516-array-insert-profile.json) pins current staging
and the frozen restricted SQL. The [adapter](array_insert_profile_1516.py)
adds scratch clocks for total CASE, its predicates, ARRAY_LENGTH, the
direct insert kernel and the native slice and concatenation kernels.
Before timing, require exact complete output, executed types, physical
plans and full source consumption against the uninstrumented #1599
release in native and both ANSI modes.

Use mains power, High Power mode and `caffeinate`. The main thread and
all benchmark workers set and verify user-interactive QoS. Linux CPU
pinning, the address-space limit and peak RSS are omitted. Check for
other builds and benchmarks before each process.

Run two A/A arms of the instrumented staging release in each mode.
Use nine reversed blocks with nine retained query/projection pairs
per process and one warm-up. Use the #1560 functions schedule,
a 130-second process cap and a one-hour session bound. Commit all raw
records and phase snapshots. An incomplete phase record makes the
attribution inconclusive.

Report the per-process medians and the two-arm A/A floor in ns per
input row. Compare CASE predicates with the total CASE clock and the
uninstrumented gap. ARRAY_LENGTH is nested in the predicates. The
direct insert kernel is nested in CASE. Subtract the predicate and
insert clocks within each sample to estimate the rest of CASE.
Associate each measured cost with its confirmed physical expression.
Use the uninstrumented baseline for the actual Spark-native gap.
