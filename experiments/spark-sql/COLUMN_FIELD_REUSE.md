# Reuse column field references

[Issue 195](https://github.com/mag1cfrog/delta-arrow-reader/issues/195) owns this
allocation change. It follows the NULL-column reuse checkpoint `c786dea` and
changes one line of Rust: `Column::return_field` clones the schema's existing
`Arc<Field>` instead of copying the field, its name and metadata into a new Arc.
Ordinary checked array/array arithmetic drops from nine allocations to five per
batch. Query latency remains inconclusive; the performance issue stays open.

## Change and contract

[datafusion-column-field-reuse.patch](datafusion-column-field-reuse.patch)
applies to the isolated DataFusion physical-expr 54.1.0 source used by the optional
runtime. The existing bounds check stays in place. The returned field still comes
from the requested schema index, including its name, type, nullability and
metadata. Sharing the immutable field also allows other scalar-function and
planning callers to avoid copies. Native arithmetic and plain reads remain
execution controls; other scalar functions can benefit from the same helper.

A standalone Rust check uses the built library to verify full field equality,
nonzero indices, expression aliases, nested field types, shared ownership,
copy-on-write isolation and out-of-bounds errors. The executable source, command
and output are in the evidence archive. No dependency or public API was added.

## Allocation and hardware work

The unchanged [physical diagnostic](integer_cost_probe.rs) validates every value
and type over 1,048,576 rows. One before and one after process run at each batch
size. Repeated Spark modes have identical allocation counts, and all native modes
retain their previous counts. The counting pass is untimed; its incidental timing
samples are retained without using them to claim a speedup.

| INT case | Batch | Allocation calls, before -> after | Requested bytes, before -> after |
| --- | ---: | ---: | ---: |
| Array + array | 8,192 | 1,152 -> 640 | 4,285,696 -> 4,252,672 |
| Array + scalar | 8,192 | 1,280 -> 1,024 | 4,291,200 -> 4,274,688 |
| Array + array | 256 | 36,864 -> 20,480 | 7,118,848 -> 6,062,080 |
| Array + scalar | 256 | 40,960 -> 32,768 | 7,294,976 -> 6,766,592 |
| Nested NULL addition | 256 | 151,552 -> 135,168 | 36,214,488 -> 35,157,720 |

The array/array reduction is four allocations per batch: two field objects and
their two names. BIGINT has the same allocation-call reduction. Counts include
output and temporary allocations, exclude prebuilt inputs/plans and return to
zero live measured bytes after execution. These are requested allocation bytes,
not process RSS.

A separate before/after/after/before hardware-counter comparison uses the
unchanged complete-query benchmark, with its original allocator. All counters
run for 100% of their enabled time. Median paired user-instruction counts fall
0.039% at batch 8,192 and 0.929% at batch 256. These whole-process totals include
input setup, validation, warmups, planning, execution and serialization across
all 48 query/mode cases. They support reduced work in this harness, without
establishing per-query latency or attributing the totals to a single operator.

## Query timing and regression checks

The same frozen 32-process protocol as the previous slice measures planning and
execution separately, with four balanced change pairs and four same-binary
control pairs per batch size. Every query output is validated outside timing.
All controls must stay within +/-5% for a case and phase to pass calibration.

| ANSI case | Batch | Before ms | After ms | Median paired change | Calibration |
| --- | ---: | ---: | ---: | ---: | --- |
| INT array addition | 8,192 | 1.949 | 2.114 | +8.8% | Fails |
| INT array addition | 256 | 5.774 | 5.611 | -2.2% | Fails |
| INT scalar addition | 256 | 5.308 | 5.200 | -1.1% | Passes |
| INT nested NULL addition | 8,192 | 6.401 | 6.284 | -2.0% | Passes |
| BIGINT nested NULL addition | 256 | 18.781 | 18.245 | -1.9% | Passes |

Execution calibration passes for 7/48 large-batch and 22/48 small-batch cases;
planning passes for 32/48 and 21/48. No calibrated execution case exceeds a 5%
slowdown. Most cases remain inconclusive, and the small calibrated differences
do not establish a consistent query speedup or general absence of regression.
All observations, including unfavorable medians, are retained. There were no
unchanged reruns to obtain a favorable result.

Correctness matches the preceding checkpoint: 616/616 focused observations;
unchanged status and successful values/types in all 6,784 regression observations,
with 6,403 agreement; 116 unchanged Delta observations; and 18 passing adapter
checks. The 38 planner and 28 runner tests pass, including four Delta lifecycle
tests. Existing strict Delta/Spark differences remain unchanged at 47 matches,
58 differences and 11 pending host cases.

## Evidence and remaining work

[column-field-reuse-results.json](column-field-reuse-results.json) identifies
the source, binaries and summary. [The raw archive](column-field-reuse-runs.json.gz)
retains all protocols, samples, hardware-counter CSV files, field checks, build
commands and restoration records. It references the preceding
[integer cost archive](integer-cost-runs.json.gz) for the unchanged baseline.
All 83 previously recorded source identities are preserved; the column source is
recorded separately before and after. The build restores 84 source/lock paths and
three executable slots.

Run the evidence check from the repository root:

```sh
python3 - <<'PY'
import gzip, json
from pathlib import Path
p = Path('experiments/spark-sql/column-field-reuse-runs.json.gz')
exec(json.loads(gzip.decompress(p.read_bytes()))['files']['check-archive.py'])
PY
```

This removes field cloning, while argument vectors, native checked kernels and
remaining NULL selection/scatter work stay open in issue 195. A separate assembly
audit records operand stack stores in the checked INT addition loop; it does not
constitute a kernel optimization. The optional runtime retains field reuse for
further work. Default-build/CI integration remains owned by issue 165.
