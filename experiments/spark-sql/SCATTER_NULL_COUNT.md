# Count the scatter validity bitmap once

[Issue 195](https://github.com/mag1cfrog/delta-arrow-reader/issues/195) owns this
follow-up to [shared argument collection](ARGUMENT_VALUE_COLLECTION.md).
`scatter_null_mask` counts its input bitmap and then constructs a `NullBuffer`
that counts it again. Remove the first count and use the constructor's stored
null count. This deletes five Rust lines without changing the scatter algorithm,
filtering, selectivity threshold or public API.

## Change and validation

[datafusion-scatter-null-count.patch](datafusion-scatter-null-count.patch)
changes only the private helper in DataFusion physical-expr-common 54.1.0.
When source values have no NULLs, construct their validity from the mask and
retain it only if it contains NULLs. When source values contain NULLs, the
existing scatter and validity construction remain unchanged; their former
unused mask count disappears as well. Public all-true, all-false and empty-mask
fast paths are unchanged. The patch adds no unsafe code.

Shared callers include primitive, Boolean, string/binary, view, fixed-size
binary and dictionary arrays. An independent Arrow take reference checks both
versions across 20 types, seven mask patterns, three mask offsets, two source
offsets, three validity modes and eight lengths around empty/bitmap boundaries.
All 40,320 observations agree. Thirteen existing scatter tests pass for each
version. These embedded modules omit only an unrelated inherent implementation
that cannot be compiled outside its defining crate; they are untimed checks.
The archived verifier checks the exact derivation from both complete sources.

The baseline is optional runtime `5232f91`. The local crate copy matches its
published source except for `utils.rs`. The local override removes only that
crate's registry identity/checksum from the lock; versions and dependency graph
stay unchanged. The build restores 87 recorded source/lock paths and three
executable slots. Default vendored sources remain unchanged.

The linked diagnostic confirms that the helper's direct `count_set_bits` call
is gone and `NullBuffer::new` remains. Symbol and relocation records identify
the calls. Its machine-code size grows from 322 to 397 bytes, so removing a
bitmap traversal alone is not treated as proof of lower elapsed time.

The full runtime retains 616/616 focused agreements, including 131 identical
error payloads. All 6,784 existing observations retain status and successful
values/types, with 6,403 agreement and no previously agreeing regressions.
All 116 Delta observations match the baseline; 18 adapter checks, 38 planner
tests and 28 runner tests pass. Strict Delta/Spark results remain 47 matches,
58 differences and 11 pending host cases. All 100 physical-diagnostic allocation
observations at each batch size retain identical calls, requested bytes and
peak live bytes, with zero measured live bytes after evaluation.

## Query measurements

The unchanged comparison retains 32 processes, 48 query/modes, 1,048,576 rows,
four warmups and 15 samples, with separate planning and execution phases.
Every same-binary pair must stay within +/-5% for calibration. All original
samples remain in the archive; no unchanged comparison was repeated.

Execution calibration passes only 1/48 large-batch cases and 0/48 small-batch
cases. The single calibrated query, ANSI INT nested NULL addition, changes
from 5.492 to 5.498 ms, with a +0.6% median paired ratio. This is within the
control tolerance and establishes no latency change. Every small-batch query
and the remaining large-batch queries are inconclusive, regardless of their
favorable or unfavorable medians. Planning calibration passes 13/48 and 0/48
cases, with no calibrated flag above 5%. These results leave full-query and
planning acceptance open.

All counters in the separate eight-process comparison run for 100% of enabled
time. Whole-process instructions change by -0.005% at batch 8,192 and -0.013%
at batch 256; cycles change by -1.026% and -1.327%. These totals include setup,
validation, planning, execution and serialization. They do not resolve the
failed query controls or isolate the scatter kernel's elapsed time.

## Evidence and remaining work

[scatter-null-count-results.json](scatter-null-count-results.json) records the
sources, binaries, complete validation, helper disassembly and measurement
limits. [The archive](scatter-null-count-runs.json.gz) retains the protocols,
all original samples, shared-caller checks, build/restoration records and
symbol/relocation evidence. Its baseline is the
[argument-collection archive](argument-value-collection-runs.json.gz).

```sh
python3 - <<'PYTHON'
import gzip, json
from pathlib import Path
p = Path('experiments/spark-sql/scatter-null-count-runs.json.gz')
exec(json.loads(gzip.decompress(p.read_bytes()))['files']['check-archive.py'])
PYTHON
```

The verifier needs no private cache or rebuild. This checkpoint removes one
redundant bitmap traversal. Required selection/scatter, the two ordinary UDF
vectors, scalar-function dispatch, native checking work and complete
query/planning acceptance remain in issue 195. It does not establish wrapping
parity or default-build integration.
