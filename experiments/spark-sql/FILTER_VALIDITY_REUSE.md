# Reusing the filter validity bitmap

The candidate removes two allocations per affected NULL-filtering batch by
skipping a bitmap that must be all valid. It remains unselected while performance
controls are unresolved. The selected optional runtime is `d2c5755`, and
[issue 195](https://github.com/mag1cfrog/delta-arrow-reader/issues/195) stays open.

The partial-NULL scalar-function path selects rows using the left operand's
validity bitmap. Arrow's shared `filter_null_mask` helper filters that same
bitmap, counts its set bits and discards it because every selected row is valid.
[The patch](arrow-filter-validity-reuse.patch) adds an identity check using the
existing `BooleanBuffer::ptr_eq`, which compares address, bit offset and length.
An identical bitmap can return `None` immediately. Copied, shifted, shorter and
unrelated masks retain the original path. Filter strategy selection is unchanged.
The patch adds no unsafe code, public API or dependency.

The helper serves multiple array types. Both variants pass 353 existing Arrow
tests plus 34,560 array and 34,560 record-batch comparisons against independent
`take` results across 20 types. The contracts cover offsets, empty inputs,
shared/copied/shifted/prefix masks, nullable masks, nested arrays and optimized
or iterator filter strategies. All 1,440 oversized-mask error pairs match.

## Allocation and native filter evidence

Separate untimed allocation captures cover 40 cases at each batch size. Nullable
shared-mask cases remove two calls per batch: the raw bitmap and its shared
buffer owner. Requested bytes decrease; measured peak live bytes do not increase.
Nonnullable counts, requested bytes and peaks remain exact, and every capture
returns to zero live bytes. The initial one-allocation prediction failed. Its
captures and assertion are retained; the owner allocation was identified and the
expectation corrected before timing, without repeating those captures.

The native timing probe uses the default allocator, 1,048,576 rows, four warmups,
15 samples and four balanced processes per batch size. It retains the original
40 filter cases: INT/BIGINT, two/eight columns, five mask patterns, and nullable
or nonnullable inputs. Both same-variant ratios must be within +/-5%.

Calibration passes 36/40 large-batch and 26/40 small-batch cases. Among the
two-column nullable cases, calibrated examples improve by 10%-30% at batch
8,192 and 6%-25% at batch 256. The small-batch nonnullable BIGINT fragmented-sparse
control retains a calibrated +8.4% flag, with change ratios 1.102 and 1.066.

A separate 32-process diagnostic counts only measured filtering after validation
and warmup. It reuses the existing perf FIFO helper and the exact native libraries.
Nullable dense-mask INT/BIGINT cases use 30%-38% fewer instructions. Sparse-mask
nonnullable controls do not add instructions. Instructions and branches calibrate
all eight comparisons, but cycles calibrate only five; the flagged BIGINT
control's cycle comparisons fail at both batch sizes. These counters do not clear
the elapsed-time flag or establish full-query latency.

The linked native helper checks absent/all-valid NULL buffers before the new
identity comparison. That return path skips the comparison and loses one
register move in this build. This agrees with the nonnullable instruction
counts, but does not explain or dismiss the elapsed-time flag.

The original native harness selected a six-byte tail-jump wrapper only for the
candidate. A follow-up changes that one timed function pointer so both variants
call their library function directly. Both compiled filter libraries and their
Arrow type dependencies retain their hashes. Relinking needed the freshly
validated Arrow facade because its old transitive arithmetic artifact had been
replaced; the first failed compile is retained. The probe uses only the facade's
unchanged array/schema reexports. Its values, cases, schedule and thresholds are
unchanged, and the unused wrapper disappears from the linked symbol table.

This direct-call comparison calibrates 34/40 and 33/40 cases. Two-column nullable
dense-mask INT/BIGINT cases improve 29.3%/27.3% at batch 8,192 and 24.2%/20.6% at
batch 256. The original small-batch BIGINT control fails calibration, so its old
flag is unresolved. A different small-batch nonnullable INT eight-column sparse
control has a +6.5% median with change ratios 1.037 and 1.093. Neither the call
correction nor favorable target results justify dropping those limits.

## Matched SQL runtime comparison

The before and after builds use the same local Arrow-select 58.4.0 path, dependency
lock and selected runtime sources. Their 88 recorded source/lock entries differ
only in `filter.rs`. The local dependency redirect changes no version or dependency.
The original selected binaries remain the semantic reference. SSE2 candidates
are excluded. Temporary sources and executable slots are restored after each build.

Both builds preserve 616 focused observations, 131 exact errors, 6,784 existing
outcomes (6,403 Spark agreements) and all 116 Delta outcomes. The 18 adapter,
38 planner and 28 runner checks pass for each build. In the separate allocation
diagnostic, nested-NULL Spark additions remove two calls per batch. Requested
bytes fall by 952 bytes per batch at 8,192 rows and 120 bytes at 256 rows; measured
peak live bytes remain unchanged. Other diagnostic allocations remain exact.

The frozen full-query protocol retains 32 processes and all 48 query/mode cases,
with planning separate from execution. Execution calibration passes 6/48 and
27/48 cases at batches 8,192/256; planning passes 27/48 and 26/48. No calibrated
query or planning median slows by more than 5%. Failed controls remain
inconclusive, including both large-batch ANSI nested-NULL targets.

| Calibrated batch-256 nested NULL query | Execution change |
| --- | ---: |
| ANSI INT addition | -11.9% |
| ANSI BIGINT addition | -8.9% |
| ANSI-off INT control | -0.4% |
| ANSI-off BIGINT control | -0.05% |

The eight whole-process counter captures use the original allocator and validate
all query outputs. Instructions fall 1.612%/1.321% and cycles fall 1.666%/0.819%
at the two batch sizes. These totals include setup, validation, warmup and
measurement; they do not measure an individual operator's latency.

The allocation reduction and calibrated small-batch query gains are supported.
The native unaffected-path flags and failed query calibrations remain open, so
this candidate is retained as evidence without changing runtime selection.
The remaining argument ownership, checked-kernel and NULL-selection costs also
remain owned by the performance investigation.

## Rechecking the evidence

[Results](filter-validity-reuse-results.json) retain all cases and controls.
[The archive](filter-validity-reuse-runs.json.gz) includes the native sources,
contracts, exact build identities, matched runtime builds, original measurements
and preparation failures. Its verifier uses the existing comparison functions
and references the immutable selected-runtime archive for baseline captures.
No private cache or benchmark rerun is needed to verify the recorded comparisons.

```sh
python3 - <<'PYTHON'
import gzip, json
from pathlib import Path
p = Path('experiments/spark-sql/filter-validity-reuse-runs.json.gz')
exec(json.loads(gzip.decompress(p.read_bytes()))['files']['check-archive.py'])
PYTHON
```
