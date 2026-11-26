# SQL baseline on Apple Silicon (#1560)

MacBook Pro Mac14,6, Apple M2 Max, 32 GiB, macOS 26.7.3 (25G342).
Source `70a77d2df0ab6a475211a361c6e5048f01c73df3`. [Committed protocol](../macos_baseline/protocol.md).

The adapter passed 12 of 12 end-to-end self-checks.
The verified SF1 snapshot has 6,001,215 rows and 81
columns, with byte-identical Parquet files and Delta logs. [Fixture proof](macos-baseline-1560-fixture.json).
The host-specific generator executable and preparation manifest have different
hashes. A full export through the stock CLI matched every decoded row. The
export proof records the Parquet and executed Arrow types for all 81 columns.
[Stock CLI export proof](macos-baseline-1560-stock-export.json).
[Harness self-checks](macos-baseline-1560-self-check.json).

## A/A floors

**Apple Silicon, native macOS.** Function values are ns per input row above
projection. Planning, SF1 and NULLIF values are ns per statement.

| Cell | First 3 blocks | 9 blocks, 3 samples | 9 blocks, 9 samples | Final floor / cost |
| --- | ---: | ---: | ---: | ---: |
| function-near-20 | 9.09752 | 19.4723 | 26.7856 | 173.821% |
| function-near-100 | 11.4919 | 21.7352 | 29.3915 | 229.265% |
| function-near-700 | 91.1986 | 340.114 | 904.91 | 208.180% |
| wide-400-planning | 9.32073e+06 | 9.58883e+06 | 2.2407e+07 | 199.019% |
| interval-day-time | 1.2125e+07 | 3.78259e+07 | 7.95394e+07 | 209.670% |
| sf1-substring | 2.8849e+07 | 1.00676e+08 | 2.80056e+08 | 203.411% |
| nullif-columns | 174376 | 527062 | 737833 | 306.340% |

Nine blocks are fixed. Retained samples per process are casts-date 3, execution 3, footprint 1, functions 9, functions_1406 3, nullif_620 9, nullif_620_recheck 3, parsers 9, pending_range_1099 7, pending_range_1099_isolated 7, planning 3, planning_attribution 9, regression_windows_1512 3, strings 9.

## Baseline

Function preflight accounted for all 633 entries and admitted 472 to
the initial sweep. The nine-sample repeat covered 15 functions
that were slower than their native reference in at least one ANSI mode.
[Function eligibility](macos-baseline-1560-function-eligibility.json.gz).

Codex Desktop restarted in block nine. The continuation verified and kept
the 1,230 complete process records, reran the interrupted shard from its
first arm, and completed the schedule on the same Mac and boot. The interrupted
process's 24 retained samples are archived separately from the comparison.
[Recovery protocol](../macos_baseline/protocol.md),
[interrupted samples](macos-baseline-1560-functions-interrupted.jsonl.gz).

**Apple Silicon, native macOS.** The [baseline CSV](macos-baseline-1560.csv) has
both ANSI rows for all 633 function entries and the planning, parser, string,
and NULLIF statement cells. It includes the exact reason for each exclusion
and each incomplete measurement. Cases with the same physical functions and
complete typed results retain their verified inventory status. Spark-only
costs keep their projection reference.

| Owner | Spark cells slower than their native counterpart beyond the floor |
| --- | ---: |
| #620 | 88 |
| #1389 | 7 |
| #1390 | 170 |
| #1409 | 6 |
| #1411 | 8 |
| #1412 | 9 |
| #1413 | 2 |

The statement archive contains 9,900 successful process records across nine
blocks. There are 290 above-floor rows and 2
untimable rows. 0 above-floor rows have no open issue owner
and are assigned to #1560.

Each x86 difference stays open for confirmation. The historical comparison
uses statements with the same inputs and references. Mac comparisons that
fall under their A/A floor are not resolvable on Apple Silicon.

**Apple Silicon, native macOS.** The CSV records a Mac status for each
historical gap and links it by report filename. The #1513 correction rows
carry the corrected Spark cost because the prior implementation returned
different results. #1414's Spark-only functions have projection costs.

| Issue | Historical cells with a Mac status or exclusion |
| --- | ---: |
| #620 | 4 |
| #1388 | 2 |
| #1389 | 14 |
| #1390 | 10 |
| #1409 | 18 |
| #1410 | 6 |
| #1411 | 8 |
| #1412 | 22 |
| #1413 | 38 |
| #1516 | 20 |

[Noise samples](macos-baseline-1560-noise.jsonl.gz),
[function sweep samples](macos-baseline-1560-functions.jsonl.gz),
[function repeat samples](macos-baseline-1560-repeat.jsonl.gz), and
[planning, parser, string and NULLIF samples](macos-baseline-1560-statements.jsonl.gz).
