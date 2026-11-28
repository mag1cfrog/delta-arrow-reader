# #1389 direct numeric interval trial on Apple Silicon

MacBook Pro Mac14,6, Apple M2 Max, 32 GiB, macOS 26.7.3 (25G342).

The [manifest](1389-direct-interval.json) pins both releases, the
fixture receipt and the unchanged #1560 parsers schedule. The sealed
staging release has the same build inputs as staging after the profile
report. Build the candidate natively with Rust 1.96.0 and identical
execution driver overlays.

The [local profile](../reports/interval-1389-macos-profile.md) locates
lexer and AST work in the runtime interval parser. The candidate reads
complete numeric unit strings and simple single-quoted wrappers. The
quoted text still enters its decoded parser scope. Standard units and
mixed units share the existing conversion and accumulator rules. Every
other input follows the existing grammar and diagnostic handling.

Before timing, require the complete committed parser differential
against independently built staging, the existing analyzer tests and
interval cast tests in both ANSI modes. The SF1 preflight compares
all 8,184 rows, exact executed types, and source I/O for each cell.
The day-time cast and its same-mode projection must agree on input
I/O. Exclude a case with a reference mismatch and preserve the reason.

Follow the [native session and fixture controls](1389-borrowed-tokens.md)
for mains power, High Power mode, sleep prevention, QoS and the frozen
SF1 data. Run 13 cells with two staging A/A arms in nine reversed
blocks, nine retained samples per process, at most five minutes per
process and one hour for the session.

For quoted year-month and day-time separately, require a paired
query-completion gain beyond the floor in at least one ANSI mode.
Neither mode nor any other cell may be slower after the instruction
and state layout rule for untouched controls. Unquoted year-month,
calendar, the projection and native references are controls. Require
complete cells, exact preflight, review, formatting, Clippy and the
full Spark workspace suite before merging. A failed gate holds the
candidate and its samples.
