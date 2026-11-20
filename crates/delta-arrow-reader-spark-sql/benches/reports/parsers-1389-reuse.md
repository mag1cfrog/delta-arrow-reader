# Parser construction candidate results (#1389, PR 2)

The measured construction candidate has a DATE regression. Spark DATE completion
rises by 96.992 ms with ANSI off and 91.234 ms with ANSI on, while native DATE
changes by -0.221 ms. The other six Spark shape medians are lower, and the
previously observed linear interval-memory retention is absent in the allocation
probe. All six shapes with native references still exceed their noise floors
in both modes. This does not establish the issue's performance target.

Before merge, the staging patch will restore the original DATE fallback. The
final retained binary is planned for correctness checks only. All timings in
this report belong to the full candidate
`1f6cb4fcd6ebbce3b993f411b9f66571ec6d230d`, including DATE reuse. There are no
samples after DATE omission. Removing that route removes a measured regression.
It is not another DATE optimization candidate.

## Exact candidate, before references and completion

Product baseline is merged PR 1 at
`be3c663a8380d5094cb52003f351d5b9a5d48383`. Before measurements are the
[PR 1 campaign](parsers-1389.md) at
`997278f7f73ad53b050f3d4b4ca381dd5eaf5e34`. Candidate preflight used
`687b55849a9271dce2094d47000084fb5679801a`. The exact-source timing rebuild at
[`1f6cb4fcd6ebbce3b993f411b9f66571ec6d230d`](https://github.com/mag1cfrog/delta-arrow-reader/commit/1f6cb4fcd6ebbce3b993f411b9f66571ec6d230d)
matched its sealed executable byte-for-byte. The reviewed
[protocol](parsers-1389-reuse-protocol.md), fixed
[manifest](../parsers/queries.json) and [schedule](../parsers/schedule.json)
preceded collection. No shapes were excluded or valid timing schedules repeated.

Each value is the median of three process medians, each based on three retained
iterations after one warmup. Completion includes planning through stream drain.
Date/timestamp/date_format use 6,001,215 returned rows. Intervals use 8,184 rows
from the unchanged SF1 predicate `l_orderkey BETWEEN 1 AND 8192`, including 90
NULL input rows. All denominators include NULLs. Change is candidate minus before.
Negative means lower completion. Decisions use unrounded nanoseconds.

| Shape | Path | Before ms | Candidate ms | Change ms | Before ns/row | Candidate ns/row |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| `date_fallback` | Native | 265.448 | 265.227 | -0.221 | 44.232 | 44.196 |
| `date_fallback` | ANSI off | 863.532 | 960.524 | 96.992 | 143.893 | 160.055 |
| `date_fallback` | ANSI on | 869.327 | 960.561 | 91.234 | 144.858 | 160.061 |
| `timestamp_ltz` | Native | 418.647 | 422.906 | 4.259 | 69.760 | 70.470 |
| `timestamp_ltz` | ANSI off | 1,162.267 | 1,041.483 | -120.784 | 193.672 | 173.545 |
| `timestamp_ltz` | ANSI on | 1,157.214 | 1,041.847 | -115.367 | 192.830 | 173.606 |
| `timestamp_ntz` | Native | 353.717 | 353.928 | 0.211 | 58.941 | 58.976 |
| `timestamp_ntz` | ANSI off | 952.847 | 855.733 | -97.114 | 158.776 | 142.593 |
| `timestamp_ntz` | ANSI on | 952.630 | 858.076 | -94.555 | 158.740 | 142.984 |
| `interval_year_month` | Native | 38.423 | 40.000 | 1.578 | 4,694.835 | 4,887.635 |
| `interval_year_month` | ANSI off | 169.837 | 125.665 | -44.172 | 20,752.295 | 15,354.982 |
| `interval_year_month` | ANSI on | 168.771 | 125.951 | -42.819 | 20,622.027 | 15,389.938 |
| `interval_day_time` | ANSI off | 236.522 | 192.555 | -43.967 | 28,900.531 | 23,528.175 |
| `interval_day_time` | ANSI on | 236.539 | 198.754 | -37.785 | 28,902.641 | 24,285.656 |
| `interval_calendar` | Native | 37.857 | 39.555 | 1.697 | 4,625.776 | 4,833.186 |
| `interval_calendar` | ANSI off | 282.676 | 258.027 | -24.649 | 34,540.032 | 31,528.212 |
| `interval_calendar` | ANSI on | 280.681 | 255.023 | -25.659 | 34,296.363 | 31,161.153 |
| `date_format_string` | Native | 1,159.649 | 1,163.881 | 4.233 | 193.236 | 193.941 |
| `date_format_string` | ANSI off | 1,990.886 | 1,844.718 | -146.168 | 331.747 | 307.391 |
| `date_format_string` | ANSI on | 1,991.691 | 1,845.068 | -146.622 | 331.881 | 307.449 |

The [candidate summary](parsers-1389-reuse-summary.json.gz) retains minimum,
maximum and IQR for all completion rows. The
[714-row metrics archive](parsers-1389-reuse-metrics.json.gz) also retains
planning/execution/first-batch, source and memory statistics. Before/after
campaigns are separate collections. Their differences are not paired causal
estimates or additive kernel costs.

## Native drift, noise and the remaining target

Each floor is the absolute difference between native A/B arm medians under the
same aggregation. All 12 candidate native-reference flags are true under the
strict rule `spark - native > noise`. Equality is false. Native drift is the
candidate's main native median minus the original main native median. A/B noise
within one campaign does not measure variation between campaigns.

| Shape | Native drift ms | Before floor ms | Candidate floor ms | Candidate floor ns/row | Off excess ns/row | On excess ns/row |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `date_fallback` | -0.221 | 0.212 | 2.364 | 0.394 | 115.859 | 115.865 |
| `timestamp_ltz` | 4.259 | 0.658 | 0.502 | 0.084 | 103.075 | 103.136 |
| `timestamp_ntz` | 0.211 | 0.373 | 2.533 | 0.422 | 83.617 | 84.008 |
| `interval_year_month` | 1.578 | 0.490 | 0.059 | 7.199 | 10,467.348 | 10,502.304 |
| `interval_calendar` | 1.697 | 0.359 | 0.363 | 44.294 | 26,695.026 | 26,327.967 |
| `date_format_string` | 4.233 | 0.658 | 0.643 | 0.107 | 113.450 | 113.508 |

The ordinary timestamp gate is eligible for subsequent review: LTZ still has
618.576/618.941 ms excess with ANSI off/on against a 0.502 ms floor. NTZ still
has 501.805/504.148 ms against 2.533 ms. Eligibility does not authorize a new
implementation here. The remaining costs execute unchanged grammar and
conversion/display logic, but this collection does not isolate their shares.
There is no measured attribution of each remaining excess to a particular
parser operation. #1389 remains open.

## DATE regression and intended omission

DATE completion rises from 863.532 to 960.524 ms with ANSI off, and from
869.327 to 960.561 ms with ANSI on. That is +16.162/+15.203 ns per returned row.
The execution ranges of process medians are disjoint in both modes:

| DATE path | Before execution median ms | Candidate execution median ms | Before execution range ms | Candidate execution range ms | Planning before / candidate ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| Native | 263.582 | 263.009 | 263.150-264.497 | 262.765-263.495 | 2.080 / 2.108 |
| ANSI off | 859.968 | 956.161 | 857.148-862.962 | 952.367-959.493 | 3.564 / 3.885 |
| ANSI on | 866.339 | 957.118 | 858.637-868.337 | 955.105-959.870 | 3.520 / 3.318 |

These observations place the regression on the execution side of the new DATE
route. They do not identify its internal cause. Planning medians remain around
3-4 ms, native completion remains near 265 ms, and old/new decoded values, types,
rows and source I/O match. There was no profile or controlled kernel experiment
to distinguish a specific closure, branch, layout or parser operation. Naming
one as the cause would exceed the evidence.

The staging patch will restore SparkDate byte-identically to the merged PR 1
source and remove the unused date_parser reuse API and its direct reuse test
calls. It will retain the DATE corpus, frozen conversion oracle, connected SQL
and existing DATE tests. The timestamp LTZ/NTZ, STRING date_format, three interval
array reuse paths and interval-only weak graph ownership are independently
separable and are planned to remain. No second DATE strategy or optimization
round is being attempted. This report's candidate measurements must not be
presented as measurements of that later source after omission.

## Day-time interval above its same-mode projection

Duration(Microsecond) has no matching native string conversion for this input's
nonzero days. It retains its plain same-input projection reference separately
in each ANSI mode. The main table gives its absolute interval completion.
Below are both projection references and subtractions over all 8,184 rows.

| ANSI | Projection before ms / ns per row | Projection candidate ms / ns per row | Excess before ms / ns per row | Excess candidate ms / ns per row | Excess-noise before / candidate ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| Off | 39.315 / 4,803.917 | 39.940 / 4,880.299 | 197.207 / 24,096.614 | 152.614 / 18,647.876 | 5.030 / 1.417 |
| On | 38.993 / 4,764.484 | 39.769 / 4,859.317 | 197.547 / 24,138.157 | 158.985 / 19,426.339 | 11.402 / 2.648 |

The excess floor is the absolute difference between the A-arm and B-arm excesses
for that mode. Both candidate excesses remain above those floors. This is an
interval-minus-projection query difference with different output types, not a
native parity flag. The reduced interval shapes still scan all six files and
25,946,748 delivered bytes per execution, so these ns/row values include matched
scan and planning costs and cannot be extrapolated to full-SF1 interval completion.

## Correctness, actual types and fixed source work

All 22 candidate exports passed 14 native/mode comparisons and 22 fully decoded
old/new comparisons. Every value, NULL, multiplicity, executed SQL type, actual
Arrow storage/unit/zone type, row count and source-I/O requirement agreed with
its original output. The before and candidate outputs have Int64/Int32 keys and:

| Shape | Rows | Actual result type | Source bytes per execution |
| --- | ---: | --- | ---: |
| DATE fallback | 6,001,215 | Date32 | 25,585,934 |
| TIMESTAMP LTZ | 6,001,215 | Timestamp(Microsecond, UTC) | 25,585,934 |
| TIMESTAMP NTZ | 6,001,215 | Timestamp(Microsecond, no zone) | 25,585,934 |
| Year-month interval | 8,184 | Interval(YearMonth) | 25,946,748 |
| Day-time interval | 8,184 | Duration(Microsecond) | 25,946,748 |
| Calendar interval | 8,184 | Interval(MonthDayNano) | 25,946,748 |
| date_format | 6,001,215 | Utf8 | 25,585,934 |
| Day-time input projection | 8,184 | Utf8View | 25,946,748 |

Every one of the 378 retained candidate iterations matched preflight rows, two
output partitions, completed counters, source object identities and exact
received/range byte totals. Input text construction remains the common native
ViewTable work in both engines. The
[full candidate preflight](parsers-1389-reuse-preflight.json.gz),
[separate receipt](../parsers/reuse-preflight-receipt.json) and
[36-member plan archive](parsers-1389-reuse-plans.tar.gz) retain complete proofs,
ranges and physical plans before export sorting. The plan archive contains 22
plans and 14 diffs. All seven shapes use their original SQL and executed types.

The exact candidate passed formatting, strict root-package all-target Clippy and
full local Linux workspace validation: 3,181 passed, zero failed, three ignored.
All 3,178 prior named outcomes were unchanged and six test outcomes were added.
The direct corpus covers 30 dates, 39 timestamps and 47 intervals, including
recursive grammar, errors/positions, zones/fractions/DST/leap seconds, string
encodings, dictionaries, NULL/empty/scalar/array forms and both CAST/TRY_CAST ANSI
modes. These are candidate correctness results, not approval of its performance
or a validation result for the future DATE omission.

## Allocation evidence and named residual construction

The same untimed release probe parsed 4,096 interval strings without retaining
results. Before the change, VmSize rose from 7,080 to 65,960 KiB. In this
candidate it starts at 7,184 KiB, reaches 7,192 KiB by call 128 and remains there
through call 4,096. RSS remains 5,216 KiB after call 128. The DATE control's
VmSize stays at 7,184 KiB. The previous linear retention is absent in this probe.
The interval-only expression factory's owned cycles were replaced with weak
recursive backedges while retaining the same combinators. This supports the
ownership explanation. It does not assign every process allocation to that graph
or claim zero allocation.

Maximum recorded interval process peak RSS across the timing campaign falls as
follows. Each peak includes setup, warmup and three retained iterations, rather
than one kernel call:

| Shape | Before peak MiB | Candidate peak MiB |
| --- | ---: | ---: |
| Year-month interval | 584.715 | 109.406 |
| Day-time interval | 584.691 | 109.641 |
| Calendar interval | 585.375 | 112.020 |

Measured pool peaks remain zero. These parser and other allocations are outside
that pool. The [validation archive](parsers-1389-reuse-validation.tar.gz)
retains original/new probe logs, unchanged probe source, commands/input hashes,
Linux logs, complete named-outcome inventory and gate results.

The candidate's quoted interval residual is still
`IntervalExpr::Literal -> parse_interval_string -> parse_interval_literal`.
Decoded temporary Strings retain their scoped parser per decoded layer. A test
counts these calls and compares values/errors. The ownership repair applies to
those temporary graphs too. With the intended DATE omission, DATE fallback will
also retain its original per-row parser construction. Neither path is concealed
by the ordinary-input results or declared repaired by the measured candidate.

## Archive identity and measurement bounds

The candidate ran once: 126 fresh processes, 126 warmups and 378 retained
iterations, with 60 A/B noise processes preceding 66 main processes. It retained
Rust 1.96.0 release opt3/debug1, jobs=2, affinity 2,3,4,5, two runtime workers and
partitions, batch 8,192 and the original fixture/session/UTC settings. Measured/
preflight pool limits remain 2/4 GiB, address caps 12/16 GiB and spill 8 GiB.
No profiles, concurrency, pilots or unchanged timing repetitions ran.

The preflight, exact-source rebuild and post-collection executable/seal hashes
agree:

```text
5b8c6152ce210a427e2610739237487c80b07d89a17c8aa15a3626bb3aa8265c
```

[Raw retained samples](parsers-1389-reuse-samples.jsonl.gz),
[summary](parsers-1389-reuse-summary.json.gz),
[all metrics](parsers-1389-reuse-metrics.json.gz) and the
[metadata bundle](parsers-1389-reuse-metadata.json.gz) preserve this exact
candidate. Metadata includes the complete before/after comparison, approved
source and Linux gate bindings, build commands/profile/paths, fixture identity,
preflight audit and post-collection binary identity. The
[evidence index](parsers-1389-reuse-evidence.json) lists artifact and tar-member
SHA-256 values. JSON archives remove whitespace without changing values. Raw
samples and tar members retain their bytes. Sealed binaries and raw IPC remain
outside Git. PR 1 evidence and all 86 protected historical assets are unchanged.
