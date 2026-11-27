# #1389 borrowed interval token trial on Apple Silicon

MacBook Pro Mac14,6, Apple M2 Max, 32 GiB, macOS 26.7.3 (25G342).

The [manifest](1389-borrowed-tokens.json) pins staging, the candidate,
the archived patch, the SF1 fixture proof and the fixed schedule.
Apply the [archived patch](../interval_1389_round3/token-input-candidate.patch.gz)
without product edits. Its provenance and previous result are recorded
in the [original trial](../reports/interval-1389-token-input-results.md).
Build both execution harness releases on this Mac with Rust 1.96.0
and the same #1560 release overlays.

Verify the committed SF1 fixture hashes. Before timing, compare the
8,184 decoded rows, executed Arrow and SQL types, and source I/O
across releases for each cell. Compare Spark and native where the
reference is typed and exact. For day-time use both Spark ANSI modes
and confirm the same source I/O as the matching input projection.
Run the existing interval parser tests. Exclude any case with a
preflight mismatch and hold if a required cast or control is missing.

Use the [native session controls](1390-scan-ordinals.md) for mains
power, High Power mode, sleep prevention and verified user-interactive
QoS. Leave out Linux CPU pinning, the address-space limit and peak RSS.
Run 13 cells with two staging A/A arms in nine reversed blocks.
Retain nine samples from each process on the #1560 parsers schedule.
The time bound is one hour for the session and five minutes per process.
Use query-completion time and report its per-row equivalent.

For day-time and calendar separately, require at least one ANSI mode
faster beyond its paired A/A floor and neither mode slower. The
unquoted and quoted year-month casts and the projection are controls.
A change within its floor passes the control gate. An above-floor
slowdown holds the candidate when the measured path changed or the
normalized instruction and state comparison cannot prove agreement.
The same rule applies to the native reference controls. With complete
cells, exact results, passing timing gates, review, formatting, Clippy
and the full Mac Spark workspace suite, merge the archived candidate.
Otherwise preserve its branch and samples and record the reason.
