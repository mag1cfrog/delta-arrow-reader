# Rejected fixed-block addition follow-ups

Two follow-ups to the [earlier reductions](CHECKED_ADD_ALTERNATIVES.md) remain
slower than the selected Arrow checked loop. Neither enters the SQL runtime.
The selected optional runtime remains `d2c5755`, and
[issue 195](https://github.com/mag1cfrog/delta-arrow-reader/issues/195) stays open.

## What changed

The fixed-chunk candidate replaces variable-length eight-row slices with
`as_chunks::<8>()` and constructs `PrimitiveArray` directly from a `Vec`.
This removes the earlier builder's extra successful-result allocation. The
first row is still checked before allocation, and any remaining error is
reported before processing the next eight-row block.

The signed-mask candidate keeps that construction and block size. It replaces
each boolean overflow comparison with `((sum ^ left) & (sum ^ right))`, ORs
the masks, and tests the combined sign bit. Only signed non-null INT/BIGINT
arrays use this path; unsigned and smaller integers retain the original path.
The original ordered checked scan supplies the first error within a failed
block. The bit-operation bounds apply only to private integer helpers.

Neither candidate adds an API, dependency or unsafe code. Generic Arrow
callbacks, NULL evaluation and other arithmetic operators retain their
original implementations.

## Validation and measurement

Both pass the same 214 existing Arrow tests, 131,072 exhaustive small-width
pair comparisons, and 1,902 boundary/slice/NULL/first-error comparisons.
The mask candidate separately checks its signed formula and adds 1,080
underflow-followed-by-overflow block comparisons for INT/BIGINT, including
offset inputs, tail lengths and each early block position.

The native and error probes retain the [earlier fixed protocol](CHECKED_ADD_ALTERNATIVES.md).
Each candidate has eight native processes and four error processes, with
before/after/after/before order, four warmups and 31 samples. Native cases
use 1,048,576 rows at batches 8,192 and 256. Error cases use 1,024 calls per
sample at zero-based positions 0, 1, 7, 8, 9, middle and last, plus success.
Untimed allocation binaries are separate from default-allocator timing.
Both same-variant ratios must stay within 5%; failed calibrations are retained.

| Native array addition | Batch | Fixed-chunk ratio | Signed-mask ratio |
| --- | ---: | ---: | ---: |
| INT | 8,192 | 2.803 | 1.949 |
| INT | 256 | 1.781 | 1.480, calibration failed |
| BIGINT | 8,192 | 1.815, calibration failed | 1.495 |
| BIGINT | 256 | 1.341 | 1.037, calibration failed |

Ratios are after/before. Native calibration passes 18/24 and 20/24 cases
for fixed chunks, and 16/24 and 10/24 for signed masks. Error/success
calibration passes 25/32 and 30/32, respectively. Both retain calibrated
success and later-error regressions above the declared 5% gate.

All successful calls now have the baseline's three allocations and requested
bytes. First-row errors avoid the output buffer; later errors retain their
original two allocations and bytes. Every allocation capture returns to
zero live bytes. The allocation improvement does not offset the observed
execution costs.

Unchanged controls also retain flags. Fixed chunks slow calibrated INT array
multiplication at both batch sizes. The signed-mask build slows large-batch
INT scalar addition and BIGINT array multiplication, and small-batch INT
scalar addition. No cause is assigned to those unchanged-path observations.

## Decision and compiler evidence

Both measured binaries still use scalar arithmetic in their addition helper;
their recorded INT/BIGINT helper bodies contain no packed integer addition.
The fixed-chunk candidate removes the earlier per-block `memcpy` call, but
that removal alone is insufficient. The signed-mask candidate also fails
the gate, despite reducing some of the scalar work.

Compiler diagnostics are retained separately. The original release settings
produce messages without source locations. A diagnostic-only build with
`debuginfo=1` locates unsuccessful SLP vectorization at the block addition
and fallback scan. That build is never used for timings and is not evidence
of identical code generation or a complete explanation for the measured
cost. No global CPU feature or optimization flag was changed in a measured run.

The rejected results bound these implementations. They do not establish that
checked arithmetic cannot vectorize, or that its remaining cost is accepted.
Any further candidate must preserve first-error behavior, clear the same
success/error/control gate, and then pass full SQL validation before selection.

## Evidence

[Results](fixed-checked-blocks-results.json) contain every comparison.
[The archive](fixed-checked-blocks-runs.json.gz) retains full sources, patches,
build/library identities, contracts, assembly, compiler diagnostics, original
samples and allocation captures. It retains preparation failures too: the
first fixed-chunk source edit failed an assertion before building; an initial
mask preparation failure was followed by an unintended build of the unchanged
source copy. Those initial records are separate and never used for timing or
the candidate decision. The corrected candidate was built and verified before
its single fixed timing protocol.

```sh
python3 - <<'PYTHON'
import gzip, json
from pathlib import Path
p = Path('experiments/spark-sql/fixed-checked-blocks-runs.json.gz')
exec(json.loads(gzip.decompress(p.read_bytes()))['files']['check-archive.py'])
PYTHON
```

The verifier uses repository artifacts only. It checks source/library identities
and recomputes all controls, ratios and decisions from the original captures.
