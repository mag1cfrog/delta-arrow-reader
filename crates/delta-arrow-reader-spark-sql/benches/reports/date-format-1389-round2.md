# STRING DATE_FORMAT on the SF1 snapshot (#1389)

Reusing ordinary timestamp parsing reduces query completion by about 38%.
The candidate remains measurably slower than native in both ANSI modes.

| Path | Staging ms (ns/row) | Candidate ms (ns/row) | Candidate native gap ns/row |
| --- | ---: | ---: | ---: |
| Native | 1,161.352 (193.519) | 1,156.487 (192.709) | |
| ANSI off | 1,931.925 (321.922) | 1,207.810 (201.261) | 8.552 |
| ANSI on | 1,932.088 (321.950) | 1,203.209 (200.494) | 7.785 |

The candidate's native A/B repeat spread is 0.191 ns per row. Each cell
uses 6,001,215 input rows and the median of three process medians.
The [90 samples](../date_format_1389/query-completion.csv.gz) and
[protocol](../date_format_1389/comparison.md) describe the 30-process
same-session release comparison. All seven full decoded comparisons passed.
Each of the six preflight results had the same SQL schema and exact Arrow
column types `Int64, Int32, Utf8`. Source I/O matched and all shapes were
eligible.

## Remaining formatting cost

The [candidate phase probe](../date_format_1389/candidate-phases.csv) isolates
the existing formatting path after the parsing change.

| Phase | Candidate Spark ns/row | Native ns/row | Difference ns/row |
| --- | ---: | ---: | ---: |
| Parse strings to microseconds | 55.399 | 80.409 | -25.010 |
| Format the same parsed array | 258.051 | 230.148 | 27.903 |

Spark's `format_timestamp_array_microsecond` calls
`format_timestamp_value` for each value. That path resolves timezone
and abbreviation details and builds a separate string per row before
collecting the result array. The native constant-format kernel constructs
one array formatter and reuses its output buffer. The phase probe
locates the remaining excess in formatting. It does not allocate that
excess to individual operations or combine with the SF1 time.

The ordinary/fallback unit test, three parser differential tests,
14 temporal contract tests, formatting and root-crate Clippy passed.

## Build identities

| Build | Git source | Executable SHA-256 |
| --- | --- | --- |
| Staging-equivalent | `7e027c72ac4e6595d3c62996ee92fc78fb4c894c` | `6e879737e02d1022717e2725e25512df1bd6fd87b38d5676d4646fbd50e7c73b` |
| Candidate | `497d2531a866c4fd6a8ec1339c16c19a150ebec2` | `00945506e253c13dca152ef3fc4a8cd2cd1161dbadc68443035ad8146b72bac5` |

The reused before executable has the same product, execution benchmark
and frozen workload sources as staging
`2135e3044fdc5825246be043427e0b7f77811c36`. The optimized parser applies
to exact ordinary whole-second timestamps. Both paths use the existing
timezone and formatting code. The original grammar handles other inputs.
