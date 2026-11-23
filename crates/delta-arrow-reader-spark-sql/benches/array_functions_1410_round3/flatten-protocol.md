# Whole-batch FLATTEN when inner lists are valid (#1410)

The [previous report](../reports/flatten-1410.md) identified a
remaining row-selection cost around native offset projection.
The Spark CASE still builds row indices after its bitmap check,
including when the batch has no NULL inner list. The native
kernel maps offsets and carries the outer validity bitmap.

Start at staging `1e55ffd0`. Its product and workspace inputs
match the sealed ARRAYS_OVERLAP candidate `3f5c501f`. Verify
the staged Spark and native physical plans and correct results
for `scalar.array.flatten` from the unchanged #1410 manifest.
Keep the original guarded evaluation for stored arrays with
NULL inner lists and for derived inputs or other types. Try one
change to use the native whole-batch projection when the
visible inner lists are valid.

Test the direct and original paths for both ANSI modes, all
four List and LargeList width pairs, outer and inner NULLs,
NULL child values, empty arrays and sliced offsets. Keep the
existing Map-leaf failure and metadata captures. Require
formatting, root Clippy, full Spark workspace tests and an
independent review.

Run three alternating release blocks with the
[paired driver](../time_conversions_1409_round3/run_pair.py).
Preflight checks every row and executed type on one million
inputs in native and both ANSI modes. Exclude a mismatch.
Require 24 successful processes with 72 retained samples,
including six native A/A processes. Bound the schedule to
3,600 seconds and each timing process to 130 seconds.

Report ns per input row above projection and each arm's median
and range over nine samples. Keep the change when both Spark
modes improve in all three process pairs and each median
paired drop exceeds the native A/A floor. The floor is the
largest of the absolute native A/A median difference and
each repeat arm's half-range of process medians. Reject if
native has three positive candidate-minus-staging deltas, a
median increase above the floor, and all candidate process
medians above all staging process medians. Apply the layout
exception only with matching executed instructions and state
layouts. An excluded case or incomplete proof holds the trial.
Archive every timing sample and report the measured residual.
