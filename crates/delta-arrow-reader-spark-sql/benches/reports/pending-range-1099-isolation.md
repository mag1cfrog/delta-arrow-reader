# Isolated Spark RANGE search trial (#1099)

The native cumulative RANGE control exceeded the regression gate.
Spark retained the pending-search gain.
At one million input rows and 8,192 rows per batch, its medians were
105.7830 and 110.9970 ms. The median paired increase was 4.6713 ms,
or 4.42% of the staging median. Repeat noise was 1.7129 ms. The
one-sided p value of 0.000244 met the Holm-adjusted cutoff.

## Correctness and native execution

Linux formatting, root-crate Clippy and the full Spark workspace
passed with 3,294 tests and 3 ignored. Both release builds matched
all 63 retained traces and the three million-row native controls.
The 42 Spark traces matched the independent Spark 4.2.0 capture.
Eighteen wrapped comparisons with different native outcomes stayed
outside the timed matrix.

Shared range, aggregate and window state sizes were 56, 128 and
152 bytes in both builds, with eight-byte alignment and identical
reported field offsets. The native evaluator and window-state source
are identical to staging.

The [instruction comparison](pending-range-1099-isolation-instructions.json)
covers five native bodies. Plain and sliding stateful evaluation
match after relocation normalization at 3,857 and 3,018 bytes.
RANGE calculation matches at 7,611 bytes. The two aggregate result
loops retain 3,936 and 4,304 bytes. Their
[three remaining differences](pending-range-1099-isolation-instruction-differences.txt)
are relocated vector loads at offsets 1,860 in the smaller loop and
1,291 and 2,420 in the larger loop. Each load reads the same 16-byte
value, `35000000000000000000000000000000`.
The cause of the remaining native timing difference is unresolved.

## Timing

The [frozen protocol](pending-range-1099-isolation-protocol.md) at
`d0c8a54e` paired staging `459ac5d4` and candidate
`264af7dd` on Linux. All 48 processes finished and retained
[10,080 raw samples](pending-range-1099-isolation-samples.jsonl.gz).
Times are milliseconds. The paired change is the median of block
differences and can differ from subtracting the arm medians.

### Native controls

| Workload | Staging | Candidate | Paired change | Repeat noise | Regression |
| --- | ---: | ---: | ---: | ---: | --- |
| Constant key, 1,024 rows / batch 1,024 | 0.2636 | 0.2587 | -0.0026 | 0.0015 | No |
| Constant key, 1,024 rows / batch 1 | 31.8720 | 31.3350 | -0.5238 | 0.1394 | No |
| Constant key, 1,024 rows / batch 32 | 1.2262 | 1.2057 | -0.0212 | 0.0079 | No |
| Constant key, 2,048 rows / batch 1,024 | 0.5672 | 0.5592 | -0.0102 | 0.0054 | No |
| Constant key, 2,048 rows / batch 1 | 122.8160 | 120.8368 | -1.8929 | 0.5459 | No |
| Constant key, 2,048 rows / batch 32 | 4.2778 | 4.2045 | -0.0693 | 0.0201 | No |
| Constant key, 4,096 rows / batch 1,024 | 1.3570 | 1.3284 | -0.0251 | 0.0202 | No |
| Constant key, 4,096 rows / batch 1 | 480.9992 | 473.5919 | -7.1474 | 2.3416 | No |
| Constant key, 4,096 rows / batch 32 | 15.9229 | 15.6102 | -0.2891 | 0.1371 | No |
| Cumulative RANGE, 1M rows | 105.7830 | 110.9970 | +4.6713 | 1.7129 | Yes |
| One preceding and following, 1M rows | 773.2616 | 762.3558 | -10.1743 | 8.6299 | No |
| Partitioned one preceding and following, 1M rows | 750.1142 | 741.7632 | -8.7452 | 8.9546 | No |

### Spark constant-key controls

| Rows | Batch | ANSI off staging | ANSI off candidate | ANSI on staging | ANSI on candidate |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1,024 | 1 | 32.8976 | 2.7431 | 32.8908 | 2.7518 |
| 1,024 | 32 | 1.1719 | 0.2045 | 1.1729 | 0.2046 |
| 1,024 | 1,024 | 0.1762 | 0.1187 | 0.1773 | 0.1186 |
| 2,048 | 1 | 125.8475 | 5.8880 | 125.8869 | 5.9005 |
| 2,048 | 32 | 4.1999 | 0.3997 | 4.1947 | 0.4006 |
| 2,048 | 1,024 | 0.3931 | 0.2186 | 0.3932 | 0.2193 |
| 4,096 | 1 | 492.5184 | 13.3163 | 491.8833 | 13.3678 |
| 4,096 | 32 | 15.8620 | 0.8297 | 15.8517 | 0.8304 |
| 4,096 | 1,024 | 1.0018 | 0.4192 | 1.0026 | 0.4195 |

The 4,096-row, single-row-batch Spark controls were 36.99 times
faster with ANSI off and 36.80 times faster with ANSI on. The
[candidate patch](pending-range-1099-isolation-candidate.patch.gz)
preserves the tested source for reproduction. The native cumulative
control leaves #1099 open and the candidate outside staging.
