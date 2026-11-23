# Writing the default FROM_UNIXTIME format into Arrow (#1409)

Compare against staging `f608f17e`. The [earlier report](../reports/from-unixtime-1409.md)
found row String allocation after zone resolution. The candidate writes
the same year-of-era and local clock fields into the Utf8 builder.
The existing formatter handles other patterns and oversized batches.

Use [the paired function driver](run_pair.py) with committed build
settings and the unchanged #1409 `scalar.datetime.from_unixtime` case.
Use the fixed million-row input, projection reference, affinity, batch
size and Linux release settings from #1391. Before timing, compare
every result and its executed Utf8 type in both builds and all three
modes. Omit any case that differs. Require Linux formatting, root-crate
Clippy, the full Spark workspace and an independent review. The
differential covers year boundaries, timestamp errors, NULLs, empty
inputs, DST transitions and fixed offsets.

Run three alternating blocks. Each runs staging and candidate in
turn for native and both ANSI modes, and native A/A using the
candidate binary. This gives 24 processes and 72 retained timing
pairs. Report ns per input row above the plain projection, with
the range of nine retained samples per arm. Walk the same process
blocks to pair medians. The noise floor is the largest of the
absolute native A/A median difference and either native repeat
arm's half-range of process medians. Keep the session within
3,600 seconds. Each timing process has 130 seconds.

Keep the product change if both ANSI modes improve in all three
blocks and the median paired drop exceeds the native floor.
A native regression triggers when all three paired deltas are
positive, their median exceeds the floor and the candidate's
process-median range lies above staging's range. Apply the
maintainer's layout exception only when the executed normalized
instructions and state layouts match. A missing or unverified
comparison holds the product change.

Use the same-session candidate native median and noise floor to
report any remaining Spark excess. If either ANSI mode remains
above that bound, leave the issue open with the measured remainder.
Publish the raw samples for every measured process in either outcome.
