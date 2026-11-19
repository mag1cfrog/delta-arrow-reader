# STRING parser costs before changes (#1389, PR 1)

All six shapes with native references exceed their same-binary A/B noise floors
in both ANSI modes. The date fallback and timestamp excesses are about 100-124
ns per returned row; date_format adds about 139 ns/row. On the fixed reduced
interval input, year-month/calendar excesses are about 16,000/30,000 ns/row.
The day-time interval shape costs about 24,100 ns/row above its same-mode input
projection. These are query-level costs, including matched source/input work
and planning, rather than an isolated measurement of parser CPU time.

This PR contains measurement and source attribution only. Product source remains
staging `298015f9def26cc97a271bdbc0316e64083d7874`. The
[protocol](parsers-1389-protocol.md),
[source inventory](parsers-1389-source-inventory.md),
[queries](../parsers/queries.json) and [schedule](../parsers/schedule.json) were
reviewed and committed before collection. Complete preflight used
`766d704c398dc20087bebeac00279029deb5441f`; the receipt and exact-source timing
rebuild used
[`997278f7f73ad53b050f3d4b4ca381dd5eaf5e34`](https://github.com/mag1cfrog/delta-arrow-reader/commit/997278f7f73ad53b050f3d4b4ca381dd5eaf5e34).
Their executable SHA-256 is identical. No product change or after measurement
is included.

## Native-reference completion

Each value is the median of three process medians, each using three retained
iterations after a separate warmup. Completion starts before query planning
and ends after the full stream drains. Minimum, maximum and IQR for each
configuration are retained in the [compressed summary](parsers-1389-summary.json.gz).
Numbers below are rounded; decisions use the unrounded nanoseconds.

| Shape | Rows | Native ms | ANSI off ms | ANSI on ms | Native ns/row | Off ns/row | On ns/row |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `date_fallback` | 6,001,215 | 265.448 | 863.532 | 869.327 | 44.232 | 143.893 | 144.858 |
| `timestamp_ltz` | 6,001,215 | 418.647 | 1,162.267 | 1,157.214 | 69.760 | 193.672 | 192.830 |
| `timestamp_ntz` | 6,001,215 | 353.717 | 952.847 | 952.630 | 58.941 | 158.776 | 158.740 |
| `interval_year_month` | 8,184 | 38.423 | 169.837 | 168.771 | 4,694.835 | 20,752.295 | 20,622.027 |
| `interval_calendar` | 8,184 | 37.857 | 282.676 | 280.681 | 4,625.776 | 34,540.032 | 34,296.363 |
| `date_format_string` | 6,001,215 | 1,159.649 | 1,990.886 | 1,991.691 | 193.236 | 331.747 | 331.881 |

Noise is the absolute difference between the native A/B arm medians under the
same aggregation. Every Spark result above has `spark - native > noise` in both
modes; equality would be false. The older 1.25x/50 ms cutoff does not apply.

| Shape | Native A ms | Native B ms | Floor ms | Floor ns/row | Off excess ns/row | On excess ns/row |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `date_fallback` | 263.890 | 263.679 | 0.212 | 0.035 | 99.661 | 100.626 |
| `timestamp_ltz` | 418.834 | 418.176 | 0.658 | 0.110 | 123.911 | 123.070 |
| `timestamp_ntz` | 352.642 | 353.015 | 0.373 | 0.062 | 99.835 | 99.799 |
| `interval_year_month` | 38.667 | 38.178 | 0.490 | 59.822 | 16,057.460 | 15,927.192 |
| `interval_calendar` | 38.689 | 38.330 | 0.359 | 43.894 | 29,914.255 | 29,670.587 |
| `date_format_string` | 1,153.152 | 1,153.810 | 0.658 | 0.110 | 138.511 | 138.646 |

## Day-time interval above its input projection

The day-time output is Duration(Microsecond). The native string-to-interval
conversion produces a calendar interval, whose cast to Duration rejects the
nonzero day component in this input. There is no native reference or native
target flag for this shape. Each Spark mode is compared with a plain projection
of the same input column, using the identical predicate and observed source I/O.
All rows below use exactly 8,184 returned rows.

| ANSI | Interval ms | Projection ms | Excess ms | Interval ns/row | Projection ns/row | Excess ns/row | Noise ms | Noise ns/row |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Off | 236.522 | 39.315 | 197.207 | 28,900.531 | 4,803.917 | 24,096.614 | 5.030 | 614.576 |
| On | 236.539 | 38.993 | 197.547 | 28,902.641 | 4,764.484 | 24,138.157 | 11.402 | 1,393.146 |

The noise floor here is the absolute difference between A-arm and B-arm
interval-minus-projection excesses, calculated separately per ANSI mode.
The raw interval and projection arm spreads remain in the
[714-row metrics archive](parsers-1389-metrics.json.gz). Both measured excesses
are larger than these floors, but this comparison does not establish native
parity. Its different output type is part of the stated query/control method.

## Exact inputs, results and source I/O

The retained [SF1 fixture](execution-1380-fixture.json) has 6,001,215 genuine
lineitems and six Parquet files. DATE, both TIMESTAMP shapes and date_format use
all rows. The three interval shapes and projection use the fixed predicate
`l_orderkey BETWEEN 1 AND 8192`: exactly 8,184 key/line pairs, including 90 NULL
input rows. The [independent count proof](../parsers/interval-input-count.json)
was derived from a fully decoded earlier native export. This is a selected SF1
subset, not a regenerated fixture or a full-scale interval completion result.

All 22 complete preflight exports and 14 decoded comparisons passed: 12
native/Spark pairs and two off/on pairs for the day-time shape and control.
Every compared value, NULL, multiplicity, executed SQL type and row count agreed.
No shape was excluded. Keys remain Int64/Int32; outputs remain Date32,
microsecond timestamps with UTC/no zone, Interval(YearMonth),
Duration(Microsecond), Interval(MonthDayNano), or STRING. Actual string storage
is Utf8 for date_format and Utf8View for the input projection; the established
string-storage normalization is the only storage difference permitted.

All pairs read the same six source objects with complete final counters and
identical received/range byte totals. Full date/timestamp/date_format shapes
read 25,585,934 bytes per execution. Interval shapes and their projection each
read 25,946,748 bytes despite returning fewer rows. Every one of the 378 retained
iterations matched its exact preflight rows, two output partitions and source
I/O. The denominator includes NULL rows. Reduced interval ns/row therefore
includes the matched scan and planning cost; it must not be extrapolated to
6,001,215 interval rows or compared with full-input ns/row as a kernel ratio.

The [complete preflight](parsers-1389-preflight.json.gz) and
[bound receipt](../parsers/preflight-receipt.json) retain types, IPC checksums,
source ranges, exact expected counts and bindings. The
[36-member plan archive](parsers-1389-plans.tar.gz) contains 22 physical plans
before export sorting and 14 diffs. Spark reaches spark_date, to_timestamp,
to_timestamp_ntz, all three spark interval UDFs and spark_date_format. Common
input construction uses the native ViewTable in both engines, and unused input
expressions are pruned. The source inventory also records transitive casts,
comparisons, result/nested coercion and temporal functions. TIME uses Chrono;
JSON/XML/CSV schema parsing runs per batch, not per data row.

## Allocation evidence and measurement identity

The first full-input interval preflight at `fe91cc8b` aborted with SIGABRT and
an allocation-failure diagnostic under the original 16 GiB address-space cap.
Ten earlier exports and six date/timestamp comparisons completed; no timing ran.
The failure was retained and the reduced-input amendment was reviewed before
rebuilding. It is an operational failure, not a semantic exclusion or an
interval timing number.

A separate untimed release probe retained no parser outputs. Across 4,096
ordinary interval parses, VmSize grew from 7,080 to 65,960 KiB, approximately
14.375 KiB/call; the date control stayed at 7,080 KiB. The source chain
`create_interval_literal_parser -> interval_literal -> expression` constructs
recursive parser graphs with owned clones in pinned Chumsky 0.12.0. That source
strongly supports retained cycles as the cause; the observation does not assign
every retained byte to a particular allocation. The
[allocation archive](parsers-1389-allocation.tar.gz) retains probe/count source,
protocol, logs, failed-run diagnostics and earlier completed export/comparison
records. No sealed executable or raw IPC is included in Git.

The reduced timing campaign completed 126 fresh processes, 126 warmups and
378 retained iterations: 60 noise processes, then 66 main processes. There were
no profiles, concurrency cases, pilot timings or repeated timing schedules.
The largest recorded interval process peak RSS was 585.375 MiB, covering setup,
warmup and three retained iterations. Measured pool peaks were zero; parser and
other allocations are outside that pool, so zero does not mean zero allocation.

The run used local Linux on an AMD Ryzen 7 8845HS, Rust 1.96.0 release
opt3/debug1, two build jobs, no release overrides, CPU affinity 2,3,4,5,
two runtime workers/target partitions and batch size 8,192. Recorded governors
were powersave. The original 2/4 GiB measured/preflight pools, 12/16 GiB address
caps and 8 GiB spill ceiling were unchanged. Setup and registration stay outside
the timers. Separate planning medians span 1.766-4.290 ms; planning, execution
completion and both first-batch intervals are retained independently in the
metrics archive. Their medians must not be added to reconstruct completion.

The timing executable and independent seal matched preflight and were verified
after collection with SHA-256:

```text
58d5145dae8e11e49daca77a966bdfad65d51cfc5f9ca91487c16ea668fd6100
```

The [metadata bundle](parsers-1389-metadata.json.gz) records the exact build
command/CWD, flags, source objects, artifact paths/profile, compiler identity,
fixture audit, preflight audit and post-collection binary identity. The
[raw samples](parsers-1389-samples.jsonl.gz), summary and all supporting metrics
are preserved. The [evidence index](parsers-1389-evidence.json) gives artifact
SHA-256 values and every tar member's size/hash; JSON archives remove whitespace
without changing values. All 86 protected historical assets remain unchanged.

## Scope of the next reviewed slice

PR 1 must land before product work starts. PR 2 may change only how often the
existing parsers are constructed on the inventoried row paths: SparkDate's
fallback, SparkTimestamp LTZ/NTZ, SparkDateFormat's STRING-to-display path and
all three interval UDF paths through parse_interval_literal. The date/timestamp
built grammar and rich-error behavior stay the same. Per-batch schema parsing,
TIME, explicit input-format parsing, SQL statement parsing, dependency forks,
core and CLI/Python remain outside this product change.

The slice must preserve every value, NULL and failure in CAST/TRY_CAST and both
ANSI modes, retain existing tests/outcomes, add direct differential checks over
committed date/timestamp/interval inputs, and follow Sail's UPSTREAM.md rules.
It needs its own reviewed pre-timing protocol and the same affected before/after
shapes, including the fixed interval subset. Any path still constructing per
row must be named with its reason. No optimization design or implementation is
part of this report. Ordinary timestamp-specific work is a later gated slice,
considered only after PR 2's measurements. The issue remains open.
