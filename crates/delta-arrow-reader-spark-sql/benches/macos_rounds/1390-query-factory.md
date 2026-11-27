# #1390 ordinary query parser trial on Apple Silicon

MacBook Pro Mac14,6, Apple M2 Max, 32 GiB, macOS 26.7.3 (25G342).

The [manifest](1390-query-factory.json) fixes both release receipts,
the statement list and the baseline schedule. The sealed staging
binary uses the same release inputs as staging after the delimited
integer grammar merge. Build the candidate natively with Rust 1.96.0
and the same release overlays.

The [local attribution](../reports/planning-1390-macos-query-factory-attribution.md)
identifies complete statement grammar construction on ordinary queries.
The candidate tries the existing scoped Query graph first for SELECT
and WITH. The full statement parser handles other starts and inputs
that the query parser rejects, using the same lexed tokens.

Before timing, require the existing statement grammar differentials,
the focused comparison for query and mixed statement lists, agreement
on all 678 canonical plan traces and 120 typed result checks per build.
A trace, result, executed type or diagnostic difference holds the
candidate before timing.

Follow the [native planning protocol](1390-scan-ordinals.md) for
mains power, High Power mode, sleep prevention and verified QoS.
Compare all 30 statements in native-plain, native-configured and both
Spark ANSI modes. Run nine reversed blocks using both releases and
two staging A/A arms per cell. Retain nine samples per process after
two warmups. Bound each process at five minutes and the session at
two hours.

For each block, release and ANSI mode, take the median of the 25
ordinary logical statement process medians. Apply the paired change
and A/A floor formula to those block values. Require this median
logical cost to improve beyond its floor in at least one ANSI mode
and to stay within its floor or improve in the other. Also require
at least one of the ordinary logical statement cells to improve
beyond its own floor. No other cell or phase may be slower beyond
its floor after the instruction and state layout rule for untouched
controls. A data difference, incomplete cell, unresolved target,
standing slowdown, or failed review or local merge check holds the
candidate.
