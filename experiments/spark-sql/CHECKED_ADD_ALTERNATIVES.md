# Rejected checked-addition reductions

Neither native prototype improves the selected checked-integer implementation.
Both are rejected under [issue 195](https://github.com/mag1cfrog/delta-arrow-reader/issues/195).
The optional SQL runtime remains at `d2c5755`; no default-build source changes.

The experiment asks whether pure non-null integer array addition can reduce
overflow flags before checking for failure. It leaves generic Arrow callback
order unchanged. Removing a branch is useful only if successful execution,
first-error behavior and unaffected operations remain acceptable.

## Candidates and checks

The whole-array version reuses Arrow's existing output loop. Each addition
records overflow in a `Cell`; after filling the array, a failed reduction
triggers a checked scan to return the original first error. The block version
checks row zero before allocating, then uses the existing `PrimitiveBuilder`
to process at most eight rows before checking overflow. Only non-null 32/64-bit
integer arrays enter the block path. Neither adds an API, trait, dependency or
unsafe code. Their patches and complete source copies are retained in the archive.

Both versions pass 214 existing Arrow tests and 131,072 exhaustive i8/u8 pair
comparisons against standard checked addition. The block candidate leaves
these smaller integer types on their original path and separately checks its
overflow algebra. Larger signed/unsigned boundaries, first-error identity,
multiple errors, masked values, slices, empty arrays and unequal lengths add
1,362 comparisons for the whole-array candidate and 1,902 for the block candidate.
The unchanged library probe also checks generic callback order and early exit.

## Method and results

Both candidates link the same recorded selected Arrow baseline and the same
seven dependency libraries. The native probe is byte-identical to the earlier
[buffer experiment](CHECKED_BUFFER_LENGTH.md). Each candidate has eight
processes: before/after/after/before at batches 8,192 and 256, using 1,048,576
rows, four warmups and 31 samples on CPU 2. Its 24 cases cover INT/BIGINT
addition, subtraction and multiplication with arrays, NULLs, scalars and
wrapping controls. Before/before and after/after ratios must both stay within
5% before interpreting a comparison. All original samples are retained.

| Native array addition | Batch | Whole-array ratio | Eight-row ratio |
| --- | ---: | ---: | ---: |
| INT | 8,192 | 4.301 | 2.985, calibration failed |
| INT | 256 | 2.628 | 2.349 |
| BIGINT | 8,192 | 2.599 | 1.955 |
| BIGINT | 256 | 1.698 | 1.510 |

Ratios are after/before, so larger is slower. Calibration passes 19/24 cases
at each batch size for the whole-array candidate, and 17/24 and 20/24 for the
block candidate. The block build also has calibrated slowdowns in unchanged
scalar paths: large-batch INT subtraction/multiplication and BIGINT
addition/subtraction, plus small-batch INT subtraction. Those observations
are retained without assigning them to source changes that those paths do
not execute. Whole-process counters include setup and checks; they are not
per-operation latency measurements.

A separate probe tests errors at the first, middle and last row, plus success.
The block version adds positions 1, 7, 8 and 9, counted from zero. Each uses
four balanced processes, four warmups and 31 samples of 1,024 calls. Timing
uses the default allocator. A separate untimed binary counts allocations
with prebuilt inputs excluded and result/error cleanup included.

The whole-array candidate passes all 16 error/success calibrations. At batch
8,192, first-row errors become 114.26 times slower for INT and 103.81 times
slower for BIGINT. Every error adds one allocation and 56 requested bytes
because the output is finalized before reporting failure. Successful
allocation counts remain unchanged.

The block candidate passes 30/32 error/success calibrations. Checking the
first row before allocating removes one output-buffer allocation on that
error path. Later errors retain their original allocation counts. However,
calibrated middle/last errors slow by 38%-256%; successful calls add one
allocation and 96 requested bytes. All counted calls return to zero live
bytes after cleanup. Avoiding early-error regressions alone does not make
this candidate acceptable.

## Implementation decision

The recorded assembly explains why the intended vectorization did not happen.
The whole-array loop writes its `Cell` flag back to memory on every row.
The block loop uses scalar comparisons and stack temporaries, then calls
`memcpy` while appending each block. It does not emit packed integer addition.
The candidate's safe builder also takes a different array-finalization path.
These observations explain concrete extra work, without claiming they account
for every timing change or unchanged-control slowdown.

Both candidates fail the declared success/error performance gate. They never
enter the full SQL runtime, so a full SQL regression run would not justify
selecting them. This rejects these two implementations, not all possible
checked-kernel optimizations. Native checking, UDF argument ownership, NULL
selection/scatter and whole-query acceptance remain open in issue 195.

## Evidence

[Results](checked-add-alternatives-results.json) retain all native and error
comparisons. [The archive](checked-add-alternatives-runs.json.gz) includes both
patches, sources, build/library hashes, contracts, original samples, counters,
allocation captures and assembly. It also retains the block prototype's
initial compile failure from an incorrect builder import and the corrected
build. No failed timing run was replaced by a favorable retry.

```sh
python3 - <<'PYTHON'
import gzip, json
from pathlib import Path
p = Path('experiments/spark-sql/checked-add-alternatives-runs.json.gz')
exec(json.loads(gzip.decompress(p.read_bytes()))['files']['check-archive.py'])
PYTHON
```

The read-only verifier recomputes every ratio and calibration from original
captures and checks the source/library relationship to the prior buffer archive.
