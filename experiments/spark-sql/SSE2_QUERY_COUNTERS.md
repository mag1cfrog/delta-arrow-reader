# Query-phase attribution for the unselected SSE2 candidate

The [vector addition experiment](CHECKED_SSE2_ADD.md) reduces measured INT
addition work, but the selected optional runtime remains `d2c5755`.
This follow-up does not clear its elapsed-time flags or select the candidate.
[Issue 195](https://github.com/mag1cfrog/delta-arrow-reader/issues/195) stays open.

## Method

Reuse the exact baseline phase-counter executable from the earlier
[scatter comparison](INTEGER_PHASE_COUNTERS.md). Its source and runtime build
record match the selected baseline. Link the byte-identical counter source
against the vector candidate's verified original libraries. No runtime source,
query, value check, sample count or instrumentation changes.

The six existing query/mode combinations cover ANSI INT array addition, both
changed wrapping controls, the flagged ANSI BIGINT NULL addition and the two
multiplication planning flags. Each runs separately at batches 8,192 and 256,
with planning and execution counted separately. The existing sixteen-process
balanced schedule, four warmups and fifteen samples produce 384 captures.
Every value is checked before counting; SQL, types, plans and output sizes must
match the original full-query captures exactly.

The existing perf FIFO helper enables counting only around the selected measured
phase, including loop bookkeeping and the handshake. Setup, input creation,
warmups, value validation, the other phase and serialization are disabled.
All same-binary controls must remain within 1% for instructions/branches and
5% for cycles/branch misses, with counters scheduled at least 99% of the time.
Instrumented elapsed times are retained but are not latency acceptance evidence.

## Findings

Instruction and branch calibration passes 24/24 comparisons, cycles 16/24 and
branch misses 12/24. All captures match the original query observations.

| Execution case | Batch | Instruction change | Cycle change |
| --- | ---: | ---: | ---: |
| ANSI INT array addition | 8,192 | -43.551% | -43.578%, calibration failed |
| ANSI INT array addition | 256 | -11.408% | -11.916% |
| ANSI BIGINT NULL addition | 8,192 | +0.028% | -2.556% |
| ANSI BIGINT NULL addition | 256 | +0.289% | +1.521% |

The unchanged wrapping INT array/scalar and multiplication execution controls
all change instructions by less than 0.1%. Their cycle controls do not all pass.
This does not reproduce or explain the original large full-binary wrapping
timing changes. In the original query binaries, the wrapping INT scalar
vector-loop bytes are identical, while the loop starts move from 0 to 16 modulo
32. That observation alone does not establish the cause of its elapsed change.

The two flagged small-batch multiplication planning cases change instructions
by +0.012% and +0.113%. The INT cycle control fails; BIGINT cycles fall 6.669%
with passed calibration. Neither result clears the older full-query flags.
Small-batch ANSI INT array-addition planning separately adds 1.076% instructions
and 1.696% branches, with passed controls, while cycles fall 9.979%. Its extra
planning work remains unattributed.

The BIGINT NULL execution difference is about 33 extra instructions per batch
at both sizes. This query has a plain column on the right, so the existing UDF
path skips NULL selection and passes a nullable array to Arrow. It cannot enter
the new non-null vector helper, but it does run the new eligibility checks.
Checking NULLs earlier is a bounded next candidate; this correlation alone
does not attribute all 33 instructions or the earlier 14% elapsed-time flag.

These measurements locate a reduction in target execution work and bound the
observed work changes in the controls. They do not establish identical machine
behavior, zero regression, wrapping parity or accepted remaining costs.

## Evidence

[Results](sse2-query-counters-results.json) retain all 24 phase comparisons.
[The archive](sse2-query-counters-runs.json.gz) contains every original counter
capture, source/library/binary identity, frozen protocol and assembly record.
It references the two immutable baseline archives instead of duplicating them.

```sh
python3 - <<'PYTHON'
import gzip, json
from pathlib import Path
p = Path('experiments/spark-sql/sse2-query-counters-runs.json.gz')
exec(json.loads(gzip.decompress(p.read_bytes()))['files']['check-archive.py'])
PYTHON
```

The checker uses repository artifacts only and recomputes all calibrations and
ratios. No unchanged timing run was repeated and no acceptance threshold changed.
