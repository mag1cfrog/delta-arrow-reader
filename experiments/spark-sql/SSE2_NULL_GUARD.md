# Earlier NULL eligibility checks

Checking NULLs earlier removes about nine instructions per nullable native
addition call in the unselected SSE2 candidate. It does not resolve that
candidate's earlier full-query or unaffected-path flags. The selected optional
runtime remains `d2c5755`; [issue 195](https://github.com/mag1cfrog/delta-arrow-reader/issues/195)
remains open.

The [phase-counter follow-up](SSE2_QUERY_COUNTERS.md) observed about 33 extra
instructions per BIGINT NULL batch. This experiment changes two lines in the
existing eligibility condition: check the input NULL counts before checking
empty/equal lengths. It compares against the archived unselected array-dispatch
candidate, not against the selected runtime. The vector helper, block size,
generic fallback and exact error behavior remain unchanged.

All 214 existing Arrow tests, 131,072 exhaustive small-width pairs, 1,902
boundary comparisons, 1,080 signed block comparisons and 6,358 sign-byte/lane
comparisons pass. Separate success/error allocations are unchanged and return
to zero live bytes. This narrow candidate was not built into the SQL runtime.

The unchanged native protocol retains eight processes and all 24 cases at
batches 8,192/256. Calibration passes 20/24 and 18/24 cases. The error protocol
retains four processes and passes 28/32 calibrations. Neither has a calibrated
slowdown above 5% in both change pairs, but failed controls remain inconclusive.
Both INT array-addition calibrations fail, as does small-batch BIGINT array
addition. A comparison against an already unselected candidate cannot clear
earlier flags with different baselines.

A separate 32-process counter comparison uses the unchanged selected-case
native probe, original libraries and existing perf FIFO helper. There is no
fixed-layout linker script. It selects nullable and non-null INT/BIGINT addition,
with the same four warmups, 31 samples and balanced four-process schedule.
Instruction and branch controls pass all eight comparisons; cycles pass 5/8
and branch misses 5/8. Each nullable case removes approximately nine instructions
per call at both batch sizes. Non-null instruction changes stay below 0.001%.
Instrumented elapsed times are not latency acceptance evidence, and failed
cycle controls are retained without retries.

This bounds the effect of one guard-order change. It neither removes the full
previous 33-instruction difference nor explains the earlier query-level flag.
The vector candidate remains unselected while the investigation continues on
the selected runtime's remaining NULL selection work.

[Results](sse2-null-guard-results.json) retain every comparison.
[The archive](sse2-null-guard-runs.json.gz) contains the two-line patch, complete
sources, build identities, contracts, assembly, allocations and original samples.
It references the immutable vector-experiment archive for the native baseline.

```sh
python3 - <<'PYTHON'
import gzip, json
from pathlib import Path
p = Path('experiments/spark-sql/sse2-null-guard-runs.json.gz')
exec(json.loads(gzip.decompress(p.read_bytes()))['files']['check-archive.py'])
PYTHON
```
