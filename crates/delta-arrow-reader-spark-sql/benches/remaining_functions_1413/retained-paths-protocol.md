# Retained function paths at the end of #1413

Run only the frozen cases named in the [comparison configuration](retained-paths-comparison.json). These
cases have no earlier timed comparison in #1413. Both release binaries
run in turn under the original seeded fixture and the
[matched process schedule](../window_aggregates_1408/string-aggregation-protocol.md#schedule-and-pass-rule).
The staging arm is the original #1413 executable. The candidate arm is
the final retained product source after the reviewed trials.

Require every case to consume one million input rows and match its
complete result and executed types across both builds, both ANSI modes
and native before clocks. Record and exclude any mismatch.

There is no new product candidate in this schedule. For each ANSI mode,
report a residual if the final Spark median exceeds the final native
median by more than the final native A/A floor. Any difference between
staging and final alone cannot attribute the cost to a particular node.
Retain the original kernels and report any unresolved difference. No
repeats or replacement samples.
