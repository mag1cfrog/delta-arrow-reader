# Approximate-percentile window attribution on Apple Silicon (#1516)

MacBook Pro Mac14,6, Apple M2 Max, 32 GiB, macOS 26.7.3 (25G342).

The [restricted Mac baseline](../reports/functions-1516-macos-restricted.md)
found its two largest gaps in the approximate-percentile windows.
The two aliases use the same Spark accumulator. Profile the
APPROX_PERCENTILE window as the representative of that path. The
[manifest](1516-percentile-profile.json) pins staging and the
unchanged restricted input and SQL.

Build a native release with Rust 1.96.0 using the
[profiling adapter](percentile_profile_1516.py). The scratch overlay
reuses the #1408 frame clocks and adds clocks and input counts around
Sail's summary insert, compress and query operations. The adapter
restores the files after sealing the executable. Before timing,
require exact rows, executed types and source guards for native
and both ANSI modes.

Follow the [native session controls](1516-restricted-baseline.md).
Run two arms of the same instrumented release for each mode in
nine reversed blocks with nine retained query/projection pairs
per process. Cap a process to 130 seconds and the session to
90 minutes. Retain every phase snapshot and timed case record.

Report per-process phase medians and the A/A floor from the two
arm medians and half-ranges. Keep nested timers separate. The
summary estimate sums insert, compression and query. The frame
work estimate sums creation, discard, update, replay and
evaluation. Its remainder after removing the summary timers
includes slicing, argument conversion and output preparation.
These clocks add overhead. Report the split inside this probe
and keep the uninstrumented baseline separate.

The window has four preceding rows plus the current row. The
fixture has 4,096 groups and NULLs where id is a multiple of 16.
The predicted update and replay counts are in the manifest.
Report the observed row entries and per-frame counts, including
any discrepancy from the prediction. Incomplete phase records
make the attribution inconclusive.

Also run the [counterexample](1516-percentile-counterexample.json)
as an untimed preflight. At accuracy 1 the two prior five-row
frames compress to identical tuples for values 0 and 4.
Dropping 0 and appending 5 then requires different results.
The proof distinguishes reusing an exact frame sequence from
retracting values from lossy summary state.
