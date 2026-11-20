# Ordinary STRING timestamp results (#1389, PR 3)

Both ordinary timestamp shapes have no Spark completion excess above native
noise in either ANSI mode. LTZ completes in 395.803/396.243 ms against native
413.586 ms. NTZ completes in 267.312/267.468 ms against native 353.169 ms.
All four strict `spark - native > native A/B noise` flags are false. This is
the fixed ordinary UTC workload result. General timestamp forms were checked
for correctness and were not separately timed. #1389 remains open.

## Exact measured identities

The three campaigns below are:

- [PR 1 baseline](parsers-1389.md), timed at
  `997278f7f73ad53b050f3d4b4ca381dd5eaf5e34`.
- [PR 2 full construction candidate](parsers-1389-reuse.md), timed at
  `1f6cb4fcd6ebbce3b993f411b9f66571ec6d230d`, including the rejected DATE reuse.
- PR 3 ordinary timestamp candidate, timed at
  [`8f23729b56aa1b665f95f47b21c382e2644fd8a6`](https://github.com/mag1cfrog/delta-arrow-reader/commit/8f23729b56aa1b665f95f47b21c382e2644fd8a6).

PR 3 product baseline is merged PR 2 at
`bc5ac56fc6630c76dadbba6582f704616e22eae0`. The retained PR 2 source after DATE
omission had correctness checks only. It has no before timing campaign. The
PR 2 column is the historical full candidate, not that retained binary.
Separate campaigns do not establish paired causal effects. Native drift and
spreads below qualify the comparison.

The committed [protocol](parsers-1389-timestamp-protocol.md) preceded collection.
PR 3 preflight source was `98aa647c5860301f1ebc5b2061c18fb9a7b6924c`. Its sealed
executable and the exact timing-source rebuild match byte-for-byte:

```text
1228376c67f9eab307b06297ad412f7b7aed1029b3fa4eb3c6502a9730288451
```

## Completion, noise and phases

Each value is the median of three process medians, each from three retained
iterations after one warmup. Completion covers planning through stream drain.
Each execution returns 6,001,215 rows, including NULLs. Cells give ms / ns per
returned row. Decisions use unrounded nanoseconds.

| Shape | Path | PR 1 ms / ns per row | PR 2 full candidate ms / ns per row | PR 3 ms / ns per row |
| --- | --- | ---: | ---: | ---: |
| LTZ | Native | 418.647 / 69.760 | 422.906 / 70.470 | 413.586 / 68.917 |
| LTZ | ANSI off | 1,162.267 / 193.672 | 1,041.483 / 173.545 | 395.803 / 65.954 |
| LTZ | ANSI on | 1,157.214 / 192.830 | 1,041.847 / 173.606 | 396.243 / 66.027 |
| NTZ | Native | 353.717 / 58.941 | 353.928 / 58.976 | 353.169 / 58.850 |
| NTZ | ANSI off | 952.847 / 158.776 | 855.733 / 142.593 | 267.312 / 44.543 |
| NTZ | ANSI on | 952.630 / 158.740 | 858.076 / 142.984 | 267.468 / 44.569 |

Native drift is PR 3 minus the indicated historical native median. Each floor
is the absolute difference of same-binary native A/B arm medians. This noise
check does not estimate variation between campaigns. Signed Spark excesses
below are candidate Spark minus its candidate native reference.

| Shape | Native drift vs PR 1 / PR 2 ms | Noise PR 1 / PR 2 / PR 3 ms | PR 3 floor ns/row | Off / on excess ms | Above floor off / on |
| --- | ---: | ---: | ---: | ---: | --- |
| LTZ | -5.061 / -9.320 | 0.658 / 0.502 / 2.205 | 0.367 | -17.782 / -17.343 | false / false |
| NTZ | -0.547 / -0.759 | 0.373 / 2.533 / 0.455 | 0.076 | -85.858 / -85.701 | false / false |

The execution reductions are much larger than native drift. Neither selected
Spark median regresses against either historical campaign. Planning remains
around 2-3 ms. The following ranges and IQRs describe process medians, not all
individual iterations. First batch is relative to execution start.

| Path | Execution PR 2 / PR 3 ms | PR 3 planning ms | PR 3 first batch ms | PR 3 completion range / IQR ms |
| --- | ---: | ---: | ---: | ---: |
| LTZ Native | 421.067 / 411.830 | 1.817 | 2.872 | 413.534-413.615 / 0.040 |
| LTZ ANSI off | 1,038.480 / 392.627 | 3.137 | 2.782 | 395.707-397.701 / 0.997 |
| LTZ ANSI on | 1,038.559 / 393.418 | 2.765 | 2.653 | 396.028-397.933 / 0.952 |
| NTZ Native | 352.068 / 351.458 | 1.719 | 2.584 | 352.049-354.890 / 1.421 |
| NTZ ANSI off | 852.374 / 264.077 | 3.235 | 2.657 | 267.160-269.468 / 1.154 |
| NTZ ANSI on | 855.227 / 264.558 | 2.805 | 2.879 | 265.742-268.067 / 1.163 |

The [170-row metrics archive](parsers-1389-timestamp-metrics.json.gz) retains
all phase, first-batch, memory/pool and source statistics. Historical phase and
spread data remain in the [PR 1 metrics](parsers-1389-metrics.json.gz) and
[PR 2 metrics](parsers-1389-reuse-metrics.json.gz). These query differences do
not assign each remaining cost to a parser operation.

## Complete correctness and regression checks

All 22 exports passed 14 native/mode comparisons and 22 fully decoded old/new
comparisons with the original outputs. The retained PR 2 correctness proof
also matches those originals. Every value, NULL, multiplicity, executed SQL
type, actual Arrow storage/unit/zone, row count and exact source objects/bytes/
ranges agreed. No shape was excluded.

Both timed shapes preserve Int64/Int32 keys. LTZ results are
Timestamp(Microsecond, UTC), and NTZ results are Timestamp(Microsecond, no zone).
Every one of the 90 retained iterations matched complete preflight rows, two
output partitions, completed counters, six source files and 25,585,934 delivered
bytes. The unchanged common native ViewTable builds input text inside timers.
The [full preflight](parsers-1389-timestamp-preflight.json.gz),
[receipt](../parsers/timestamp-preflight-receipt.json) and
[36-member plans archive](parsers-1389-timestamp-plans.tar.gz) retain complete
comparisons, source ranges, 22 physical plans and 14 diffs.

Formatting and strict root-package all-target Clippy passed. The first full
Linux workspace invocation failed in the unchanged dictionary string sweep:
`collect_set(s)` returned the same eight strings in different orders. One full
retry at the identical source passed 3,182 tests, with zero failures and three
ignored. All 3,183 prior named outcomes were unchanged and two tests were added.
No code, test or expected result changed for that retry. The
[validation archive](parsers-1389-timestamp-validation.tar.gz) retains the first
failure, the successful retry, their commands/source hashes, complete named
inventory and focused check logs.

The 72 added inputs cover 19 ordinary calendar cases and 53 fallback cases,
including DST gaps/overlaps, fractions, explicit zones, leap seconds, year/
calendar/clock limits, controls and Unicode. Direct tests compare exact values
and error variants/text with the original normalized parser and count fallback
calls. Connected CAST/TRY_CAST tests cover both ANSI modes, all three string
encodings, SQL dictionaries, NULLs and UTC/New_York/Kathmandu. Invalid session
zones also have direct safe/strict coverage. Existing differential tests passed.
The original localization, formatted-input and error/leap routes are unchanged.

## Remaining seven-shape scope

Only LTZ/NTZ received a PR 3 timing campaign. The other five shapes below carry
historical measurements from their stated identities. They are not a final
seven-shape timing of the PR 3 binary. Values are native/reference ms and Spark
ANSI off/on completion ms.

| Shape | Actual measured identity | Native / projection reference ms | Off / on ms |
| --- | --- | ---: | ---: |
| DATE fallback | PR 1, restored route | 265.448 | 863.532 / 869.327 |
| Year-month interval | PR 2 full candidate | 40.000 | 125.665 / 125.951 |
| Day-time interval | PR 2 full candidate | 39.940 / 39.769 | 192.555 / 198.754 |
| Calendar interval | PR 2 full candidate | 39.555 | 258.027 / 255.023 |
| STRING date_format | PR 2 full candidate | 1,163.881 | 1,844.718 / 1,845.068 |

DATE reuse was rejected after +96.992/+91.234 ms completion regressions in the
full PR 2 candidate. SparkDate was restored byte-identically to merged PR 1.
It retains its original noncanonical per-row parser and the existing exact
ASCII DATE fast route. The table carries the old baseline measurement. There
was no final DATE retime after restoration.

The other four rows are the actual PR 2 full candidate measurements. Their
remaining native-reference or same-input projection excesses stayed above
noise. Interval rows use the fixed 8,184-row real-SF1 subset, including 90 NULL
inputs, and scan 25,946,748 bytes. STRING date_format uses all 6,001,215 rows.
Day-time's distinct Duration(Microsecond) output has no matching native pair.
Its same-mode projection subtraction was 152.614/158.985 ms against excess
floors 1.417/2.648 ms. These reduced-row ns/row costs cannot be extrapolated to
full-SF1 interval conversion.

The [PR 2 report](parsers-1389-reuse.md) retains interval allocation-probe
results and qualified ownership evidence. Scoped decoded quoted intervals
still construct their original parser per decoded layer. The general timestamp
fallback also remains for unsupported/invalid spellings. These residuals and
the other kernels keep #1389 open. No further candidate or timing round is
included here.

## Fixed collection and archives

The [unchanged manifest](../parsers/queries.json) and
[schedule](../parsers/schedule.json) retain workload origin
`298015f9def26cc97a271bdbc0316e64083d7874`. PR 3 selects only the original LTZ/NTZ
entries in their original order: 12 native A/B noise processes, then 18 main
processes, for 30 warmups and 90 retained iterations. Collection ran once, with
no pilots, profiles, concurrent queries or valid timing repetitions.

The local Linux AMD Ryzen 7 8845HS run retains Rust 1.96.0 release opt3/debug1,
jobs=2, affinity 2,3,4,5, two runtime workers/partitions, batch 8192, measured/
preflight pools 2/4 GiB, address caps 12/16 GiB, spill 8 GiB and powersave
settings. The [metadata bundle](parsers-1389-timestamp-metadata.json.gz) preserves
both build commands/artifact profiles, exact source and executable identities,
fixture/session/schedule, gate results, preflight audit and complete comparison.
Post-collection executable and seal hashes still match the preflight hash.

[Raw samples](parsers-1389-timestamp-samples.jsonl.gz),
[summary](parsers-1389-timestamp-summary.json.gz), all metrics, plans and
validation logs remain linked above. The
[evidence index](parsers-1389-timestamp-evidence.json) records archive and member
SHA-256 values. JSON whitespace is compacted without changing values. Raw
samples and tar members retain their bytes. Sealed binaries and raw IPC stay
outside Git. Prior PR 1/PR 2 evidence and all 86 protected assets are unchanged.
