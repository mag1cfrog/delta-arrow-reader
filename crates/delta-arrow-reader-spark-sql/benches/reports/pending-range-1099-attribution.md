# Pending RANGE search and buffering costs (#1099)

The sorted constant-key query defers its first row until the input closes.
With 4,096 one-row batches, it performs 8,390,656 upper-bound comparisons
before EOF and 4,096 more when the first row completes. The executor keeps
4,096 rows in each input buffer. Each buffer copies 8,390,655 row equivalents
as it grows. The two costs have separate call sites.

## Reproduction and work

The probe plans `COUNT(*) OVER (ORDER BY k ASC NULLS FIRST RANGE BETWEEN
UNBOUNDED PRECEDING AND 1 FOLLOWING)` through the public SQL entry points.
The physical plan contains one bounded window operator and orders on the
actual input column. A gated sorted source releases each batch in turn and
withholds EOF. Both Spark ANSI modes and plain DataFusion are in the same
release binary.

| Input rows | Batch rows | Upper comparisons before EOF | Rows copied at each buffer before EOF |
| ---: | ---: | ---: | ---: |
| 1,024 | 1 | 524,800 | 524,799 |
| 2,048 | 1 | 2,098,176 | 2,098,175 |
| 4,096 | 1 | 8,390,656 | 8,390,655 |
| 4,096 | 32 | 264,192 | 264,160 |
| 4,096 | 1,024 | 10,240 | 9,216 |

For N rows in K equal batches of B rows, the first row searches
`B*K*(K+1)/2` positions before EOF. The partition and
original-input buffers each concatenate the same prefixes minus the first
batch. The argument and ORDER BY evaluation volumes also follow the full
prefix sum. ORDER BY reads a column with shared storage. Row equivalents
describe the work requested by those operations.

The accumulator does no updates before EOF. After EOF it consumes each row
once. In the progressive control the four zero keys become available as
soon as key 2 arrives, with EOF still withheld. The fifth row waits for EOF.

## Correctness and native eligibility

Every counts-only context preserves its complete observation, including the
required Spark Field stages and wrapped ANSI-on error phase. The wrapped
INT_MIN PRECEDING shape is excluded from timing. Spark ANSI off returns the
partition count after EOF, Spark ANSI on fails during collect, and native
returns zero counts. The ordinary constant contexts match native in both
ANSI modes. The independent reference uses Spark 4.2.0 at revision
`32f7299601108917fb01920a54e084595b7b3bf8`.

The preflight caught two compile-time API mistakes and a drain that treated
Tokio's cooperative yield as input starvation. The revised drain checks the
source's own waiting signal. These corrections were benchmark-only and made
before collecting timings.

## Linux release timings

| Batch rows, 4,096 input rows | Native wall (ms) | Spark ANSI off wall (ms) | Spark ANSI on wall (ms) | Spark ANSI off range region (ms) |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 471.839 | 495.354 | 495.900 | 481.641 |
| 32 | 15.631 | 15.942 | 15.897 | 15.611 |
| 1,024 | 1.335 | 1.007 | 1.005 | 0.964 |

The wall columns are medians of 21 samples from three fresh release
processes. The same binary measures all three modes and rotates their
order. These are executor timings on a Ryzen 7 8845HS with CPU 2 pinned.
They include source gating and collecting the batches.

The separate instrumented run places the largest measured cost in
`WindowFrameStateRange::calculate_index_of_row` through
`get_result_column`. The latter defers before committing the candidate
range to the accumulator. At one row per batch, the ANSI-off range region
takes 481.641 ms. Its partition and original-input concats take 2.881 and
2.433 ms, respectively. Argument evaluation takes 1.442 ms and order
evaluation 0.423 ms. All figures in this paragraph include observer
overhead and combine work before and after EOF. The range region
accounts for 98.47% of recorded region time at batch 1 and 97.66% at
batch 32 with ANSI off. These shares cover the recorded source regions.

This selects the repeated search for the bounded change. Retaining a
pending frontier requires a stable input column and must preserve the
committed range and completion checks. Buffer copying and whole-prefix
argument work still grow with the batch count. The executor must retain
unfinished rows until it can emit their results.

## Evidence

[Protocol](../pending_range_1099/README.md) |
[Complete work counters](pending-range-1099-work.csv) |
[Per-context timing table](pending-range-1099-wall-and-regions.csv) |
[Observations, reference and samples](pending-range-1099-evidence.json.gz)

The final schedule has two counts-only processes, one observed region
process, and three canonical timing processes. An earlier failed canonical
diagnostic and the corrected observed smoke probe ran without clocks.
The two preflights bring the baseline to eight runtime processes, one
above the planned per-revision cap. The six candidate processes and the
shared Spark capture still fit the 15-process total. No optional profile
was run, and the timing schedule ran once.

Linux formatting, strict Clippy and the full Spark workspace passed.
All 3,189 named outcomes matched the preceding staging inventory,
with 3,186 passing and three ignored.
