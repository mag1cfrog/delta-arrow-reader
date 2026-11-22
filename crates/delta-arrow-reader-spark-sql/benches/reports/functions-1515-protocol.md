# Final-round slowdown attribution protocol (#1515)

The [original sample archive](functions-1515-original-1414-samples.jsonl.gz)
contains all 3,024 retained observations from the final #1414 session.
The [selection](../final_round_slowdowns_1515/original-slowdowns.json)
records the 41 mode comparisons that were slower under that session's
paired rule. They span 29 statement forms.

## Repeat and source isolation

Repeat the 39 modes whose statements do not execute code changed in
#1414. Use the exact historical binaries at `e1580ef0` and `51b0c8ea`
and the same SQL and projection controls from the final #1414 manifest.
The coordinator verifies their recorded executable hashes.

The other two modes execute TIMESTAMPDIFF in the implementation shared
with the literal-end TIMEDIFF change from `e8614487`. Compare the historical
`51b0c8ea` binary with scratch build `b442c02c`. The scratch commit is based
on `51b0c8ea` and restores only
`sail-function/src/scalar/datetime/spark_timestamp_diff.rs` from
`e1580ef0`. Build with Rust 1.96.0 in release mode, debug level 1, and
the same two-file row-observation overlay as #1414. The overlay affects
preflight evidence and leaves the timed executor untouched. Neither the
scratch commit nor its changes enter the report PR. The isolation session
times both original mode comparisons and the TIMEDIFF statement that
benefited from the retained change.

## Execution and decision

Preflight every selected statement in both ANSI modes on both artifacts
using the shared full-row comparison. Compare all rows, executed types
and source row counts. Exclude a statement with any discrepancy and
record the reason. A preflight or timing failure leaves that comparison
incomplete.

Use three fresh process pairs per selected mode with the original
#1414 CPU, memory and million-row input settings. Each process keeps
three samples after one warmup. Rotate the statement order by one
position per replica. Reverse the ANSI mode and arm order in replica 2.
Cap repeat at 30 minutes and isolation at 15 minutes including preflight.
Run the two sessions serially on the local Linux host with no competing
builds or timing jobs.

Subtract each sample's projection time and use each process's median ns
per input row. The floor is the larger half-range across the two sets
of three medians. A slowdown requires at least two positive paired
differences and a median paired increase above that floor. The reverse
rule identifies a speedup. Any missing process, type drift or incomplete
sample set yields an incomplete comparison. Run each schedule once.
A repeat without changed source is recorded for the maintainer. A
slowdown in isolation is eligible for one focused correction.

Archive every retained sample by process before continuing to the next
process. Keep the samples and report in the same staging PR.

```sh
python3 benches/final_round_slowdowns_1515/run.py repeat BEFORE_BUILD FINAL_BUILD REPEAT_OUTPUT
python3 benches/final_round_slowdowns_1515/run.py isolate WITHOUT_TIMEDIFF FINAL_BUILD ISOLATION_OUTPUT
```
