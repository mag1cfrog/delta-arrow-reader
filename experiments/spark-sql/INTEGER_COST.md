# Checked integer arithmetic costs

[Issue 195](https://github.com/mag1cfrog/delta-arrow-reader/issues/195) owns this
performance investigation. The correctness baseline was merged through
[PR 194](https://github.com/mag1cfrog/delta-arrow-reader/pull/194) at
`34c3d4e6e7c4639c91fb43f3080979e30b378bcf`. This first performance slice identifies
three cost sources and removes one duplicate column filter. Column reuse is the
experimental starting point for the next slice. It reduces measured allocation
work; a stable whole-query speedup has not been established, and the performance
issue remains open.

## What the diagnostic separates

[integer_cost_probe.rs](integer_cost_probe.rs) evaluates prebuilt physical
expressions over 1,048,576 rows. It compares native wrapping arithmetic, native
checked arithmetic and the Spark checked scalar function. It covers INT/BIGINT
addition, subtraction and multiplication with array, scalar and nullable inputs,
plus nested nullable addition. Each batch size, 8,192 and 256, has four processes
in forward/reverse/reverse/forward order, four warmups and 15 measured samples.
Every output value and type is checked outside timing.

Repeated execution of the same native plan and the same Spark plan provides
calibration. All four paired ratios must remain within +/-5% for both plans to
support a timing claim for that case. Only 17/20 large-batch cases and 9/20
small-batch cases pass. Failed controls and the earlier smoke run remain in the
archive; smoke timings are excluded.

Representative calibrated results before the optimization:

| Case and batch size | Wrapping ms | Native checked ms | Spark checked ms | Spark/native paired ratio |
| --- | ---: | ---: | ---: | ---: |
| INT array addition, 8,192 | 0.127 | 0.905 | 0.925 | 1.022 |
| INT array addition, 256 | 0.636 | 1.409 | 2.038 | 1.450 |
| INT nested NULL addition, 8,192 | 0.221 | 1.537 | 7.675 | 4.987 |
| BIGINT nested NULL addition, 8,192 | 0.689 | 1.765 | 14.270 | 8.091 |

These are physical-expression diagnostics, without query planning, scanning or
collection. Timing retains an inactive allocation-counting hook. Allocation
counters run in a separate untimed pass. The whole-query comparison below uses
the original allocator without this hook.

The native checked path is already substantially slower than wrapping on these
inputs. Arrow 58.4.0 selects its fallible checked kernels through `try_op` and
`try_binary`/`try_unary`; wrapping takes the infallible path. This does not prove
that the measured cost is unavoidable or establish a specific vectorization
cause. The Spark wrapper adds allocation/dispatch work: INT array addition uses
1,152 allocations versus 384 for the native checked plan at batch size 8,192.
Nested NULL evaluation adds selection and scattering work on top of those costs.

Native eager evaluation is only a valid-input diagnostic control. The executable
also checks an all-NULL left operand with an overflowing right child: native
eager evaluation errors, while the Spark path returns NULL. Replacing the Spark
path wholesale with that native expression would lose required behavior.

## One column reuse change

[datafusion-integer-null-selection.patch](datafusion-integer-null-selection.patch)
applies to the isolated DataFusion physical-expr source after
[the correctness patches](INTEGER_OVERFLOW.md). The partial-NULL path already
filters the input batch. When the left operand is a `Column`, it can reuse that
filtered column instead of filtering the same array again. Computed left operands
keep their separate selection and single evaluation. Other evaluation paths keep
their existing behavior. The only changed runtime source is `scalar_function.rs`.

The following counts cover one execution over 1,048,576 rows. They include output
and temporary allocations, exclude prebuilt input/plan storage, and return to zero
live measured bytes after execution. Peak bytes are measured heap allocations,
not process RSS. Before counts agree across all four diagnostic processes; after
counts agree between the repeated Spark modes in one process per batch size.

| Nested NULL case | Allocation calls, before -> after | Allocated bytes, before -> after | Peak live bytes, before -> after |
| --- | ---: | ---: | ---: |
| INT, batch 8,192 | 6,144 -> 5,376 | 31,107,396 -> 27,356,632 | 141,777 -> 117,776 |
| BIGINT, batch 8,192 | 6,144 -> 5,376 | 53,280,136 -> 45,934,256 | 282,233 -> 234,816 |
| INT, batch 256 | 176,128 -> 151,552 | 41,382,468 -> 36,214,488 | 5,753 -> 4,705 |
| BIGINT, batch 256 | 176,128 -> 151,552 | 63,718,536 -> 54,955,440 | 10,185 -> 8,257 |

Other diagnostic cases retain identical allocation counts. The repeated filter
is removed, but checked-kernel, wrapper and remaining selection/scatter costs
are still owned by issue 195.

## Whole-query timing and correctness

The unchanged [integer_overflow_bench.rs](integer_overflow_bench.rs) compares the
merged checked baseline with this candidate on equivalent semantics. It validates
48 query/mode observations per process, using MemTable input, both ANSI modes and
both batch sizes. Planning and execution are timed separately. Sixteen processes
per batch size provide four balanced before/after pairs, two before/before pairs
and two after/after pairs. Same-binary ratios must all stay within +/-5% per case
and phase; otherwise timings are inconclusive. All 32 processes are retained.

| ANSI nested NULL query | Batch | Before ms | After ms | Median paired change | Calibration |
| --- | ---: | ---: | ---: | ---: | --- |
| INT | 8,192 | 8.960 | 6.375 | -27.5% | Fails |
| BIGINT | 8,192 | 10.475 | 8.976 | -12.4% | Fails |
| INT | 256 | 19.001 | 15.994 | -16.6% | Fails |
| BIGINT | 256 | 22.216 | 19.010 | -16.6% | Fails |

The medians suggest improvement but do not establish it. Only 5/48 large-batch
and 9/48 small-batch execution comparisons pass calibration; planning passes
31/48 and 2/48 respectively. No calibrated execution comparison exceeds a 5%
slowdown, but most comparisons are inconclusive, so this is not evidence that
all unaffected paths are free of regression. No unchanged reruns were used to
search for a more favorable result. Original samples, pair ratios and whole-
process RSS are retained separately from the expression allocation counts.

The candidate preserves all 616 focused overflow observations. All 6,784 existing
observations retain their status and successful values/types, with agreement at
6,403 and no previously agreeing regressions. All 116 Delta observations match
the merged baseline, and all 18 adapter checks pass. Rust tests pass: 38 planner
and 28 runner tests, including four Delta lifecycle tests. Existing error-field
differences remain: the strict Delta/Spark report is still 47 matches, 58
differences and 11 pending host cases. These checks do not establish complete
Spark compatibility.

## Evidence and reproduction

[integer-cost-results.json](integer-cost-results.json) records the summary and
archive hash. [integer-cost-runs.json.gz](integer-cost-runs.json.gz) contains the
protocols, every capture/sample, source and binary identities, diagnostic library
hashes, test logs, source audit and analysis/build commands. It references the
committed correctness archive for the unchanged baseline sources. The candidate
build restores all 83 shared source/lock paths and three executable slots.

Run the read-only evidence check from the repository root:

```sh
python3 - <<'PY'
import gzip, json
from pathlib import Path
p = Path('experiments/spark-sql/integer-cost-runs.json.gz')
exec(json.loads(gzip.decompress(p.read_bytes()))['files']['check-archive.py'])
PY
```

The archive's build scripts record the isolated research environment; they are
not a new portable build interface. Normal-build and CI integration remain owned
by issue 165. Column reuse is retained in the optional experimental baseline;
the default vendored runtime remains unchanged. This does not accept the remaining
performance costs or close issue 195.
