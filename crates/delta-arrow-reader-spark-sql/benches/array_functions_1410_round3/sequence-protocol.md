# Whole-batch SEQUENCE endpoints (#1410)

The [previous report](../reports/sequence-1410.md) attributes the
remaining INT SEQUENCE gap to selecting lambda arguments and tracking
active rows before the integral generator. Confirm the staged Spark
and native plans and compare all rows and executed types for
`scalar.array.sequence` from the unchanged #1410 manifest. The
existing scatter already returns the original values for all-active
rows, so remove the identity indices and capture copies on that path.

Evaluate endpoints in order over the full batch. Keep the same
selection for a partially active batch and the same ordered retry
when a child errors. Avoid re-evaluating any endpoint to switch paths.
Preserve the previous behavior for empty batches, scalar endpoints,
and captures outside the optimized input type. The integral and
calendar generators and the native range implementation stay intact.

Use committed differential inputs for no NULLs, hidden errors after
a NULL first argument, staggered and all NULLs, empty and sliced
batches, descending ranges, explicit steps and reachable failures
in both ANSI modes. Keep the old fixture outcomes. Run formatting,
root Clippy, full Spark workspace and independent review.

Run three alternating release blocks with the
[paired driver](../time_conversions_1409_round3/run_pair.py)
and the [sealed build pins](sequence.json). Preflight checks
one million result rows and executed Arrow types across both builds,
native and both ANSI modes. A mismatch excludes the case. Require
24 completed processes with 72 retained samples, including six
native A/A processes. Bound the overall schedule to 3,600 seconds
and each timed process to 130 seconds.

Report ns per input row above projection, each arm's nine-sample
median and range, and the three paired process differences. Keep the
candidate when all three pairs improve in both Spark modes, both
median reductions exceed the native A/A floor, and native passes.
The floor is the maximum of the native A/A absolute median difference
and each arm's half-range of process medians. Native fails when all
three paired changes are positive, the median increase exceeds the
floor, and all candidate medians are above all staging medians.
Apply the layout exception only with matching executed instructions
and state layouts. A missing observation or unproved gate holds
the candidate. Commit all samples, including any failed attempt.
