# #1390 delimited integer grammar trial on Apple Silicon

MacBook Pro Mac14,6, Apple M2 Max, 32 GiB, macOS 26.7.3 (25G342).

The [manifest](1390-integer-grammar.json) fixes both release receipts,
the statement list and the fixed baseline schedule. The sealed staging
binary uses the same release inputs as staging after the CASE resolver
report. Build the candidate on this Mac with Rust 1.96.0 and the same
release driver overlays.

The [grammar observation](../reports/planning-1390-macos-integer-grammar-attribution.md)
identified repeated precedence parsing for the IN-1000 literals and
CASE-200 `THEN` results. The trial recognizes an unsuffixed unsigned
integer with a proven following delimiter in the diagnostic-free
statement grammar. The general grammar and Rich diagnostics handle
all remaining inputs through their existing paths.

Before timing, require both statement parser differentials against
the existing Rich reference, agreement on all 678 canonical traces,
and 120 typed result checks per release. A result, executed type,
error or trace difference holds the candidate before timing.

Follow the [native planning protocol](1390-scan-ordinals.md) for
mains power, High Power mode, sleep prevention and verified QoS.
Compare all 30 statements in native-plain, native-configured and both
Spark ANSI modes. Run nine reversed blocks using both releases and
two staging A/A arms per cell. Retain nine samples per process after
two warmups. Bound each process at five minutes and the session at
two hours.

For each of CASE-200 and IN-1000, require logical planning to be
faster beyond its A/A floor in at least one ANSI mode and slower in
neither. No other cell or phase may be slower beyond its floor.
Judge each cell by the paired median of changes against its own
A/A floor. Compare normalized instructions and state layouts for
any above-floor untouched control. Only a demonstrated match
waives the slowdown. Incomplete cells, a correctness failure, an
unresolved target, a standing slowdown, or failed review or local
merge checks hold the candidate.
