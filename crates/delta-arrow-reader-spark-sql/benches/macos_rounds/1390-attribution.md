# #1390 planning phase attribution on Apple Silicon

MacBook Pro Mac14,6, Apple M2 Max, 32 GiB, macOS 26.7.3 (25G342).

The [manifest](1390-attribution.json) pins the staging source and the
canonical build it matches. Visit the 30 statements from #1560 in
descending order of their largest above-floor planning difference.
The first block starts with `stress/wide_400`. The profile includes
all four planning configurations on the same instrumented release.
The instrumentation lives in a disposable worktree. The DataFusion
observer uses a separately unpacked package from the locked registry
archive. Require all 678 exact canonical plan traces before profiling.

Use the [native session controls](1390-scan-ordinals.md), including
mains power, High Power mode, `caffeinate`, and verified
user-interactive QoS on the benchmark main and worker threads. No
other builds or benchmarks run during observation. There are nine
blocks and two arms of the same release per cell. Reverse the order of
cells and arms in every other block. Each process retains nine samples
after two warmups. The session is bounded at two hours and each process
at five minutes. The original logical and physical timers remain in
place. No query executes in the observed loop.

Report the logical and physical wall times and the location of phase
spans. Separate logical and physical events outside the recorded
timers. Keep nested spans in separate rows. For each configuration
and span, reduce each process to the median of its retained samples.
Within each block, take the median of its two processes per
configuration, then subtract the native-configured value from the
Spark value. Report the median of those nine differences. Compute
each configuration's A/A floor by #1560's rule and use the sum of
the two floors for attribution. A component above that threshold
supports attribution for a cell already slow in the uninstrumented
baseline. Report its observed share of the instrumented phase gap.
Report smaller differences as unresolved on Apple Silicon. Nested
components overlap. The attributed span sizes include the cost of
the hooks and do not decide product merges.

Record one result for every statement. Start with the projection
rule, initial physical planner and alias conversion for the 400-column
statement. Report the largest attributable causes and any
unresolved remainder. Use the observed source and traces to confirm
the code before proposing a change. Retain every process sample,
including an incomplete capture if one occurs. An incomplete cell
provides no numeric attribution.
