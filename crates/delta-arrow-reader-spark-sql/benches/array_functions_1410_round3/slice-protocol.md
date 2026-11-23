# SLICE remaining adapter cost (#1410)

The [second-round report](../reports/array-slice-1410.md) measured
Spark SLICE 2.811 and 3.185 ns per input row above native. Both
expressions already use native slicing for positive scalar bounds.
The Spark path still declares the input element field and supports
ordered row errors. The past comparison did not separate those costs.

Use staging `81c480e4`, whose product and workspace inputs match
the sealed SEQUENCE candidate `f5ea78cb`. Run correctness-only
preflight for `scalar.array.slice` from the unchanged #1410 manifest.
Require all one million rows and executed types to match native
in both ANSI modes before sampling. Record the physical plans.

Take one ANSI-off stack profile with the existing
[LLDB sampler](../time_conversions_1409_round3/sample_stacks.py).
Use CPUs 2, 3, 4 and 5 and the frozen statement fixture and schedule.
The sampler requests 128 snapshots with 11-17 ms between stops.
Use a fresh output directory, a 390-second child completion bound
and a 400-second outer watchdog. Run the profiled statement once.
Keep the original stack records and all result records with the
report, including its three retained samples if it completes.
Use the stacks for attribution, not for elapsed-time comparison.

Change only a cost tied to a distinct routine on the measured
Spark path and absent from native. If the samples identify the
shared kernel but no such routine, report that the profile cannot
resolve the residual and keep the current code. Do not run a
second profile. Keep ordered errors, NULL behavior, executed
types, fields and existing fixture outcomes in both ANSI modes.

If attribution supports a change, seal one candidate and commit
both source pins before a separate paired timing run. Use the
[paired driver](../time_conversions_1409_round3/run_pair.py) with
24 processes in three alternating blocks, including six native
A/A controls. Require the million-row and type preflight and all
72 retained samples. Bound the session to 3,600 seconds and each
timed process to 130 seconds. Both Spark modes must improve in
each block, and each median paired reduction must exceed the
native A/A floor. The floor is the largest of the absolute A/A
median difference and each repeat arm's half-range of process
medians. Native fails if all three paired changes are positive,
the median increase exceeds the floor, and all candidate medians
exceed all staging medians. Apply the layout exception only with
equal executed normalized instructions and state layouts. Missing
evidence or a failed gate holds the candidate. Keep every raw
sample in either outcome. Run formatting, root Clippy, the full
Spark workspace suite and independent review for a product change.
