# STRING casts after the DATE change (#1385)

The DATE change brings both affected shapes below the completion line in both
ANSI modes. Isolated DATE completion is now 194.525 ms with ANSI off and
193.684 ms with ANSI on, versus 545.464 ms and 554.339 ms before.
The combined shape is also below the line. The DECIMAL change gate is false,
so no DECIMAL implementation or diagnostic profile is needed.

## Change and matched results

`SparkDate` now reuses Arrow's existing Date32 parser only for exact ten-byte
ASCII `YYYY-MM-DD`. Both parsers use the same Chrono calendar validation and
epoch-day conversion on this subset. Every other form and unsuccessful native
attempt still reaches the original parser and NULL/error formatter. Formatted
dates, STRING-to-TIMESTAMP, DECIMAL, planning and dependency-fork sources are
unchanged.

Before values are the matched [PR 1 baseline](casts-1385.md), measured at
`03313bab7739e0945be5d2e872ad52a7591b8483`. Fresh after values were measured at
`d25b1b228a5405be4f4d812b36af3a50d0823787`, after its code/protocol commit and
exact-source rebuild. DECIMAL was unmodified and was not timed again: its three
values below are carried from PR 1, with fresh current correctness evidence only.

| Shape | Path | Before ms | After or carried ms | Timing source | Spark/native | Above line |
| --- | --- | ---: | ---: | --- | ---: | --- |
| Combined | Native | 688.293 | 677.533 | DATE stage | - | - |
| Combined | Spark ANSI off | 1035.473 | 686.026 | DATE stage | 1.013x | no |
| Combined | Spark ANSI on | 1035.847 | 686.998 | DATE stage | 1.014x | no |
| DATE only | Native | 180.170 | 179.232 | DATE stage | - | - |
| DATE only | Spark ANSI off | 545.464 | 194.525 | DATE stage | 1.085x | no |
| DATE only | Spark ANSI on | 554.339 | 193.684 | DATE stage | 1.081x | no |
| DECIMAL only | Native | 533.725 | 533.725 | PR 1 carried | - | - |
| DECIMAL only | Spark ANSI off | 514.605 | 514.605 | PR 1 carried | 0.964x | no |
| DECIMAL only | Spark ANSI on | 510.977 | 510.977 | PR 1 carried | 0.957x | no |

Completion includes planning through stream drain. A shape is above the line only
when it is more than 1.25 times the same-stage native reference and more than
50 ms slower. Decisions use unrounded nanoseconds. Each median is the median of three process medians,
each based on three retained iterations after a separate warmup.

Native A/B noise is the absolute difference between its two arm medians:

| Shape | Before noise ms | After or carried noise ms |
| --- | ---: | ---: |
| Combined | 0.817 | 0.603 |
| DATE only | 0.221 | 0.029 |
| DECIMAL only | 3.155 | 3.155 (PR 1 carried) |

The [after results CSV](casts-1385-date-results.csv) retains all 102 metric
rows, including min/max/IQR, first-batch intervals, planning/execution, pool/RSS
and source counters. The [noise CSV](casts-1385-date-noise.csv) retains all 34
after noise rows. There are no pool-threshold cases.

## Correctness and source I/O

Fresh preflight covered all three shapes in native SQL and both Spark ANSI modes:
nine full exports, six current native/Spark comparisons and nine baseline/current
decoded comparisons. Every result has 6,001,215 rows, with the same values, NULLs,
multiplicity and executed SQL primitive types under the original normalization
rules. Keys remain Int64/Int32; outputs remain Date32 and/or Decimal128(15,2).

All nine configurations read the same six source objects and exact baseline bytes:

| Shape | Delivered bytes in each configuration |
| --- | ---: |
| Combined | 57,151,761 |
| DATE only | 25,585,934 |
| DECIMAL only | 57,127,165 |

All 90 retained after iterations matched their preflight rows, exact source path
sets and bytes. Counters were final, every recorded range sum equaled received
bytes, and output partition count remained two. The
[plan archive](casts-1385-date-plans.tar.gz) contains nine fresh plans and six
native/Spark diffs before export sorting. All nine plan texts match the baseline;
the execution change is inside the existing DATE function.

The [committed differential inputs](../../vendor/sail/crates/sail-function/tests/fixtures/date-cast-1385-inputs.json)
passed 204 strict/null-on-error comparisons against the original parser.
Caller tests passed CAST/TRY_CAST in both ANSI modes for scalar, Utf8, LargeUtf8,
Utf8View and dictionary inputs, including NULLs and malformed dates. Fmt and
strict root-package all-target Clippy passed. The full Spark workspace passed
3,164 tests with zero failures and three ignored tests. All 3,165 old named
outcomes are unchanged; only two new test names were added. The complete new
inventory is included in the [full preflight](casts-1385-date-preflight.json.gz).

## Measurement identity and decision

The [frozen DATE protocol](casts-1385-date-protocol.md) retained Rust 1.96.0
release with debug information, CPU affinity `2,3,4,5`, two runtime workers and
target partitions, batch size 8,192, the same session setup and retained SF1
fixture. One run used 12 noise plus 18 main processes: 30 warmups and 90 retained
iterations. No DECIMAL timing, concurrency, profiles or repeated schedule ran.

The actual timing cache executable was:

```text
/home/hanbo/.cache/delta-arrow-reader/casts-1385-2026-11-19/release/deps/execution-87be9fc3f891d603
```

Its post-timing SHA-256 is
`4757aff7514261ffe18f96ffae227070197d7505f1f8649506188b006bd6a81c`, matching
both preflight and the independent read-only `date-d25b1b22-execution` copy.
The sealed baseline copy also remains intact. The
[metadata](casts-1385-date-metadata.json) records both distinct artifact paths,
inodes, source/tree hashes, commands, flags and the post-timing binding.

The isolated DECIMAL baseline flags were false and the combined after-DATE flags
are false. Under the frozen completion-only gate, PR 3 is not eligible. The
affected measured shapes meet the target, and unmodified DECIMAL retains its
below-line baseline classification. All three have fresh current correctness/I/O
proof. This evidence completes the planned DATE repair without a second round.

These results cover ordinary inputs on one SF1 fixture and worker. Fallback
formats were checked for semantics, not separately timed. Isolated overheads
remain non-additive because projections differ. Native A/B noise measures
within-run repeatability, not variation between campaigns. STRING-to-TIMESTAMP
remains unmodified and unmeasured; the baseline report records its similar
per-row parser-construction pattern.

[Compressed raw samples](casts-1385-date-samples.jsonl.gz),
[summary flags](casts-1385-date-summary.json),
[validation and independent audit](casts-1385-date-validation.json), and the
[bound preflight receipt](../casts/date/preflight-receipt.json) retain the proof.
All 12 frozen paths and 29 original measurement assets remain unchanged.
