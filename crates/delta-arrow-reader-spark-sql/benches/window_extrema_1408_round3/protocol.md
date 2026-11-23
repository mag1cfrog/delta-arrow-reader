# Window extrema ordered by their key (#1408)

Start at staging `8e790f97`. Its product and workspace inputs
match the sealed SEQUENCE release build `f5ea78cb`. Use the same
fixed one-million-row table and ROWS frame as the earlier
[MAX_BY and MIN_BY report](../reports/sliding-extrema-1408.md).
The [manifest](manifest.json) keeps the two previous result
references and adds native MAX and MIN over the same frame.
Those aggregates are scale references for the amount of work.
Their values are checked against Spark MAX and MIN on their own
statements, independently of the by-key statements.

First check whether ordering by the key alone makes a one-position
read equivalent. Capture Spark 4.2.0 (revision `32f72996`) in
UTC with both ANSI settings. Capture MAX_BY, MIN_BY and the two
endpoint rows and values together on nine fixed BIGINT rows.
Include duplicate keys, NULL values, NULL keys, both directions
and both NULL placements. Use bounded ROWS, bounded RANGE,
growing, whole-partition and potentially empty frames.
The [capture script](capture_ordered_extrema.py) writes executed
types and complete rows. Bound the local JVM run to 240 seconds.
Use the actual Spark and DAR rules for ties and NULLs when
interpreting the captures.

If the key-order condition alone is insufficient, report a
concrete counterexample and keep the general accumulator.
In that case run one staging release session for the three
references per function, using the existing schedule and preflight
from #1408. No candidate is built or timed. If the condition is
sufficient, freeze the candidate source, paired schedule and
gates in a separate commit before a paired comparison. Keep the
general accumulator for every other window. Require differential
tests and full local workspace validation in that case.

In the report-only case, run three alternating blocks over both
window functions. For each function and block, run Spark ANSI
off and on, the matching endpoint in native and native MAX or
MIN. Repeat each native reference once in the block for A/A.
This is 36 processes and 108 samples, three samples per process.
Use the #1391 CPU affinity and input, a 130-second process bound
and a 3,600-second overall bound. First require full row and type
agreement for each SQL pair and exclude any mismatch. Report
medians and ranges, in ns per input row above projection, for
the two Spark modes and each native reference. For each native
reference, report the largest of the absolute A/A median
difference and both A/A half-ranges of process medians.
Archive all raw records. The two endpoint references remain
the existing target. Leave #1408 open while the gap remains.
