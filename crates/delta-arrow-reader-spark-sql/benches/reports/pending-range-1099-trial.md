# Pending RANGE frontier trial (#1099)

The candidate eliminated repeated Spark boundary comparisons. At 4,096
rows with one row per batch, both ANSI modes dropped from 8,390,656
comparisons before EOF to 4,096. The same query dropped from about
495 ms to 13.3 ms. Four of nine native controls failed the timing gate
specified before the run. #1099 remains open.
[Evaluated commit `7517b3ea`](https://github.com/mag1cfrog/delta-arrow-reader/tree/7517b3eac29ef50fc026d0a2f46f7bc23e33e18e)

## Execution cost

Medians in milliseconds from 21 samples per cell across three processes.
Each process runs both Spark ANSI modes and the plain native control from
the same Linux release binary. This table holds the input at 4,096 rows.

| Batch rows | Native before / after | Spark ANSI off before / after | Spark ANSI on before / after |
| ---: | ---: | ---: | ---: |
| 1 | 471.839 / 475.482 | 495.354 / 13.315 | 495.900 / 13.332 |
| 32 | 15.631 / 15.723 | 15.942 / 0.837 | 15.897 / 0.834 |
| 1,024 | 1.335 / 1.349 | 1.007 / 0.426 | 1.005 / 0.422 |

The timing rule flags a regression when all three candidate process medians are above all
three baseline medians and the overall rise exceeds the larger
within-revision spread. All nine native medians rose. The four cells
below met both conditions.
[Native timing rule](https://github.com/mag1cfrog/delta-arrow-reader/blob/7517b3eac29ef50fc026d0a2f46f7bc23e33e18e/crates/delta-arrow-reader-spark-sql/benches/pending_range_1099/comparison.md)

| Rows / batch | Native before (ms) | Native after (ms) | Change |
| --- | ---: | ---: | ---: |
| 1,024 / 1 | 31.382 | 31.593 | +0.67% |
| 2,048 / 1,024 | 0.561 | 0.572 | +2.05% |
| 4,096 / 1 | 471.839 | 475.482 | +0.77% |
| 4,096 / 32 | 15.631 | 15.723 | +0.59% |

The native work counters are identical. At 4,096 rows with batch size 1,
the separately instrumented native range region rises from 456.518
to 459.995 ms. That observation includes instrument overhead. It does
not isolate the native timing change to one instruction or distinguish
it from between-build or host effects. The candidate failed to
establish native timing non-regression.

## Correctness and remaining work

All 63 canonical contexts are byte-identical to the baseline,
including both ANSI modes, native, progressive output, complete rows,
Fields, and the wrapped overflow phase. The observed trace preserves
all 21 native work snapshots. At every eligible batch size, each of the
27 pending Spark contexts performs one upper comparison per input row.
The wrapped Spark/native pairs remain outside all clocks.

The committed accumulator range stays separate from the pending cursor.
The candidate uses it only for a supported frame over a stable physical
column. The comparison still evaluates the target before using the hint.
Existing progress, failure, NULL, DATE, Float and Decimal controls passed.
The focused differential checks compare appends, unchanged-input retries,
pruning, completion and late failures against the existing computed-key path.

Each growing input buffer still copies 8,390,655 row equivalents at
4,096 one-row batches. The aggregate still evaluates 8,390,656 argument
array rows before EOF. Each input buffer retains 4,096 unfinished rows.
These remaining costs follow the original buffer growth and evaluation
paths. The single bounded trial is complete.

## Evidence

[Baseline attribution](pending-range-1099-attribution.md) |
[All before and after cells](pending-range-1099-comparison.csv) |
[Candidate walls and regions](pending-range-1099-candidate-regions.csv) |
[Candidate observations and sample receipt](pending-range-1099-candidate.json.gz)

The candidate ran six scheduled processes with one wall schedule.
Together with the eight baseline processes and shared Spark capture,
the comparison used 15 runtime processes. A four-row Spark discovery
was run separately before the protocol was frozen. Its first attempt used
mismatched Python interpreters. All reported comparisons use the full
capture from the corrected environment.

Formatting, strict root Clippy and the full Linux workspace passed at
the tested source commit.
All 3,189 baseline outcomes were preserved. The four new tests passed,
bringing the totals to 3,190 passed, zero failed and three ignored.
