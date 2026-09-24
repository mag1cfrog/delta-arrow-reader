# Separate integer query planning and execution counters

This is a measurement-only follow-up in
[issue 195](https://github.com/mag1cfrog/delta-arrow-reader/issues/195). It compares
optional runtimes `5232f91` and `d2c5755`, the two sides of the
[scatter bitmap change](SCATTER_NULL_COUNT.md). No execution implementation
changes here. Region counts locate the observed instruction reduction in
nested NULL execution; full-query elapsed-time acceptance remains open.

## Method

[integer-phase-counter.patch](integer-phase-counter.patch) derives a diagnostic
from the unchanged `integer_overflow_bench.rs`. It reuses the exact perf FIFO
handshake already in `examples/decimal_bench.rs`, selects one existing query
and ANSI mode, and enables counters only around one measured phase after its
warmups. Queries, inputs, every-row output validation, planning/execution loops
and sample counts retain their original implementation.

The before/after diagnostics link the recorded original runtime libraries.
Their hashes are checked before linking; no runtime sources are changed or
rebuilt. Every captured SQL expression, type, physical plan and output-buffer
size matches the original full-query capture for that variant and batch size.
All values are validated before enabling counters.

The frozen protocol has four query/modes, two batch sizes, two phases and
16 balanced processes per combination: 256 processes total. Each process
uses 1,048,576 rows, four warmups and 15 counted samples on CPU 2. Counting
includes the selected measurement loop and its existing control handshake;
setup, value validation, warmups, the other phase and serialization are disabled.
No estimated overhead is subtracted.

Each event must run for at least 99% of enabled time. All before/before and
after/after controls must be within +/-1% for instructions and branches, and
+/-5% for cycles and branch misses. Every original process is retained; none
is repeated to obtain a favorable result. Instrumented elapsed samples are
archived but are not used for latency claims.

## Results

All events run for 100% of enabled time. Instruction and branch controls pass
16/16 comparisons, cycles pass 15/16, and branch misses pass 10/16. The failed
cycle control is small-batch INT nested NULL planning; every failed control
remains in the results and archive.

| ANSI nested NULL query | Batch | Instruction change | Cycle change | Instruction/cycle controls |
| --- | ---: | ---: | ---: | --- |
| INT addition | 8,192 | -0.182% | +0.376% | Pass |
| BIGINT addition | 8,192 | -0.184% | -0.124% | Pass |
| INT addition | 256 | -0.408% | -1.458% | Pass |
| BIGINT addition | 256 | -0.386% | -0.057% | Pass |

These median paired counts place the observed reduction in the affected
execution region. The ordinary INT and ANSI-off Decimal execution controls
change by less than 0.001% in instructions. Planning instructions range from
-0.012% to +0.016%. The small counter changes remain within the declared
control tolerances; they do not establish elapsed-time improvements, complete
performance parity, or resolution of the earlier full-query calibration failures.

This comparison also cannot clear older planning flags whose baseline differs
from `5232f91`. It narrows the latest change's attribution while leaving those
historical questions with their existing evidence in issue 195.

## Evidence

[integer-phase-counters-results.json](integer-phase-counters-results.json)
contains all comparisons and limits. [The archive](integer-phase-counters-runs.json.gz)
contains source derivation, exact library/binary identities, the frozen protocol,
all 256 query captures, raw counter CSV files, commands and failed controls.
It refers to the original [scatter archive](scatter-null-count-runs.json.gz).

```sh
python3 - <<'PYTHON'
import gzip, json
from pathlib import Path
p = Path('experiments/spark-sql/integer-phase-counters-runs.json.gz')
exec(json.loads(gzip.decompress(p.read_bytes()))['files']['check-archive.py'])
PYTHON
```

The verifier needs no private cache or rebuild. Remaining ordinary UDF vectors,
checked-kernel work, required selection/scatter and complete query/planning
acceptance remain in issue 195. Default-build integration is separate.
