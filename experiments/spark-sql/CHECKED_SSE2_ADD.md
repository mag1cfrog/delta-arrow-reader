# Unselected vector checked-addition experiment

The x86_64/SSE2 prototype improves several native addition cases and passes
the frozen SQL regressions. It remains unselected because unaffected native
paths retain timing flags and the full-query comparison does not clear
performance acceptance. The selected optional runtime stays at `d2c5755`.
[Issue 195](https://github.com/mag1cfrog/delta-arrow-reader/issues/195) stays open.

## Implementation and scope

The [previous fixed-block reductions](FIXED_CHECKED_BLOCKS.md) did not
vectorize. This candidate uses local SSE2 intrinsics for non-empty, equal-length,
non-null INT/BIGINT array addition. It checks row zero before allocating, then
adds eight rows at a time. The signed overflow mask uses each integer's sign
byte; an ordered checked scan returns the original first error in a failed
block. At most seven later pure additions execute before reporting an error.

The unsafe loads and stores stay within live eight-element input/stack arrays.
Compile-time x86_64/SSE2 guards protect the implementation. Other targets retain
the original source path, but were not cross-compiled or tested here. Smaller
and unsigned integers, NULL arrays, scalar inputs and other operators also
retain their original arithmetic paths. No generic callback is reordered.

The second candidate moves eligibility checks into the existing equal-shape
dispatch arm. Mixed scalar/array inputs keep their original branches. Both
candidates use the same vector helper, block size and error behavior. Neither
changes dependencies, global CPU flags or the default vendor build.

## Native results and layout attribution

Each candidate passes 214 existing Arrow tests, 131,072 exhaustive small-width
pair comparisons, 1,902 boundary/slice/NULL/error comparisons, 1,080 signed
block comparisons and 6,358 sign-byte/lane comparisons. The unchanged native
probe also preserves its callback and value checks. Recorded native and full
runtime assembly contains packed integer additions.

The native protocol is unchanged: eight processes per candidate, batches
8,192/256, before/after/after/before order, four warmups, 31 samples and
1,048,576 rows. Both same-variant ratios must be within 5%. Separate default
allocator error probes cover eight positions/success cases at both widths
and lengths. Allocation probes run separately.

| Native array addition | Batch | Initial SSE2 ratio | Array-dispatch ratio |
| --- | ---: | ---: | ---: |
| INT | 8,192 | 0.625 | 0.627 |
| INT | 256 | 0.803, calibration failed | 0.800 |
| BIGINT | 8,192 | 1.137, calibration failed | 0.982 |
| BIGINT | 256 | 0.947 | 0.913 |

Ratios are after/before. Error/success calibration passes 31/32 and 26/32
cases, with no calibrated slowdown above the declared gate. First-row errors
avoid one output allocation; other error and successful allocation counts
remain unchanged. Every allocation capture returns to zero live bytes.

The initial build retains calibrated flags for large-batch BIGINT scalar
multiplication (+17.3%) and small-batch INT scalar addition (+20.4%). Their
hot-loop bytes are identical before/after, but their addresses differ by 16
bytes relative to a 32-byte boundary. A diagnostic link moves only those four
functions inside fixed slots. All 4,012 other text symbols and all four loop
byte sequences remain identical across the two layouts.

In that separate 16-process diagnostic, splitting the old BIGINT multiplication
loop across the boundary slows its large-batch measurement 15.0%, calibrated.
The corresponding new-code control fails calibration; the INT scalar flag
is not explained. This supports a layout contribution on this AMD Ryzen
7 8845HS. It neither explains every flag nor adopts a production alignment rule.

A separate 32-process selected-phase counter probe reuses the existing perf
FIFO helper and exact native libraries. With those loops equally aligned,
small-batch INT scalar addition adds 0.637% instructions and 1.606% cycles,
both calibrated. Large-batch BIGINT scalar multiplication stays within 0.22%.
INT array-add instructions fall 47.93%/34.66%; BIGINT falls 22.14%/21.98%.
Instrumented elapsed samples are not latency acceptance evidence.

Moving eligibility into array dispatch improves the target native additions,
but retains calibrated flags in unchanged subtraction, multiplication and
scalar-addition paths. All original controls remain in the results. The
layout diagnostic does not clear these later flags.

## Full optional-runtime diagnostic

The dispatch candidate was built against the exact selected `d2c5755` runtime,
with the same lock file and override graph. Only private Arrow `numeric.rs`
changes. All 88 temporary source/lock paths and three executable slots were
restored and checked after the build. The default vendor tree is unchanged.

Validation retains 616 focused agreements, 131 exact error payloads and all
6,784 existing outcomes, of which 6,403 agree with Spark. All 116 Delta
observations match the selected baseline; 18 adapter, 38 planner and 28 runner
checks pass. Existing Spark differences remain differences. Allocation calls
are unchanged in all 200 diagnostic observations. Only nested NULL additions
request fewer bytes, because the temporary vector does not round capacity to
Arrow's buffer alignment. Unrelated allocation counts, bytes and peaks are exact.

The unchanged 32-process full-query protocol retains all samples and controls.
Execution calibration passes 9/48 large-batch and 24/48 small-batch cases.
Planning passes 35/48 and 30/48. Calibrated ANSI INT array addition improves
78.9% at batch 8,192, and ANSI BIGINT nested NULL addition improves 9.5% at
batch 256. These are measurements of this full binary, not isolated SIMD gains.
An unchanged ANSI-off INT scalar-addition query also improves 79.6%, so native
kernel improvements alone cannot explain the full-binary timing changes.

Large-batch ANSI BIGINT NULL addition retains a +14.0% execution flag, with
change-pair ratios 1.151/1.147/1.134/0.799. Small-batch ANSI-off INT multiplication
and ANSI BIGINT multiplication retain planning flags of +11.3% and +8.5%,
with uneven change-pair ratios. Failed calibrations remain inconclusive,
including a large ANSI-off INT array-addition slowdown. No unchanged timing
run was repeated to replace these observations.

The eight whole-process counter captures are eligible. Instructions fall
0.287%/0.203% and cycles change +0.263%/-0.248% at the two batch sizes. These
counts include setup, validation, planning and execution; they cannot accept
individual query latency or explain the outstanding flags.

## Evidence and decision

[Results](checked-sse2-add-results.json) contain every comparison.
[The archive](checked-sse2-add-runs.json.gz) retains both source candidates,
build identities, contracts, assembly, linker scripts, original captures and
restoration records. It also retains the layout diagnostic's initial link
failure and a timing launch blocked before any sample by an external build.

```sh
python3 - <<'PYTHON'
import gzip, json
from pathlib import Path
p = Path('experiments/spark-sql/checked-sse2-add-runs.json.gz')
exec(json.loads(gzip.decompress(p.read_bytes()))['files']['check-archive.py'])
PYTHON
```

The verifier reads repository artifacts only and recomputes every control and
ratio. Both implementations remain unselected. Further attribution must use
the recorded binaries and preserve these flags; this experiment does not
establish wrapping parity, zero regression or an unavoidable checking cost.
