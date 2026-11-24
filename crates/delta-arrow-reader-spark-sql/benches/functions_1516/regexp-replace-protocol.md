# Plain literal REGEXP_REPLACE (#1516)

The [attribution](../reports/classified-functions-1516-attribution.md)
measured a 42 ns per row gap on the restricted REGEXP_REPLACE case.
The Spark loop reads fixed arguments and visits a regex cache per
live row. Each match requests captures and parses the same
replacement text.

Try one path for a nonempty scalar pattern, a scalar replacement
without dollar signs or backslashes, and position 1. Compile the
pattern once per batch when any source row is live. Search full
matches without collecting capture groups. Preserve the existing
zero-width search rule and all three Arrow string types. The exact
nonword boundary pattern and other inputs use the existing path.

Compare the fast path directly with the original path over committed
cases covering adjacent empty matches, Unicode, empty and NULL
sources, empty replacements, malformed patterns and all three string
types. Verify the full #1391 input against staging in both ANSI
modes and native, then require exact rows and executed types across
all modes on the restricted input before timing. Hold the candidate
if either check fails. Run formatting, root Clippy and the full
Spark workspace tests.

Build both arms on the same local Linux aarch64 guest using the
existing Rust 1.96.0 release recipe. Use the frozen REGEXP_REPLACE
statement and restriction in [the #1516 manifest](manifest.json).
Run three paired blocks with alternating build order and six native
A/A processes in the same session. Each process has 130 seconds,
the restricted schedule has 3,600 seconds, and the completed run
retains 72 samples from 24 processes.

Both Spark modes must improve in all three blocks and their
median paired reductions must exceed the native A/A noise floor.
Compute the floor as the greatest of the absolute A/A median
difference and the two process-median half-ranges. Native triggers
the control gate only if all three increases are positive, their
median exceeds the floor, and all candidate process medians exceed
the staging process medians. If that gate triggers, compare the
executed instructions after relocation normalization and the
executed state layouts. Matching code and layouts pass the control.
A missing comparison or an unresolved control gate holds the
candidate. Commit all samples from the scheduled run and record
the remaining difference after this single trial.
