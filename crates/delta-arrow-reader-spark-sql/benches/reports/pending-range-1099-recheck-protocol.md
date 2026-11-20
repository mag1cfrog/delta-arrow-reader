# Pending RANGE search candidate recheck

[#1099](https://github.com/mag1cfrog/delta-arrow-reader/issues/1099) compares
staging `6a520463` and candidate `063b52e2`. The candidate selects the Spark
integer RANGE specialization before the per-row loop. Other frames use the
loop without pending-search state accesses. The first-round probe and reference
capture stay unchanged.

## Eligibility

Build two uninstrumented release executables on Linux with Rust 1.96.0, the
same release settings and code-generated driver, and their respective locked
dependencies. Archive each exact commit before adding this protocol. Keep
build logs, executable digests, generated source digests and assembly records.
Check that both builds agree on all 63 trace contexts, including delivered
batches, results, executed fields, errors and streaming output boundaries. Match
the Spark arms against the independently captured Spark 4.2.0 reference at
revision `32f7299601108917fb01920a54e084595b7b3bf8`. The capture digest is fixed
in `pending_range_1099_recheck/run.py`. The validator checks the original
manifest digest and substitutes the current checkout path in a temporary copy
of the capture header so the frozen validator can run outside the original
worktree.

Time all 27 constant-key contexts: three input sizes (1,024, 2,048 and 4,096
rows), three source batch sizes (1, 32 and 1,024) and three configurations
(native and Spark in each ANSI mode). The Spark results and executed types must
agree with native in all timed cells. Retain the progressive checks and the
wrapped-key results and failures as untimed controls.

## Schedule and pass rule

Run 12 blocks on CPU 2. Each block starts two new processes per build in
staging/candidate/candidate/staging order, reversed on alternate blocks. Each
process runs the same 27 cells in the configuration order rotated by the block
number, with two warmups and seven recorded executions per cell. The driver
includes source coordination and output collection in execution time. It
plans and constructs inputs before the timer and checks the entire result
afterward. No other local builds or performance runs overlap the session.
The session has a 65-minute wall limit and at most 48 timed processes.

Use the median of seven samples per process, then the candidate median minus
the staging median in each block. Estimate repeat noise per cell as the larger
of the two medians of absolute within-build replicate differences. A native
regression requires a positive median block difference exceeding repeat noise
and a one-sided paired sign-test rejection with Holm correction across nine
native cells at 5%. Stop without merging if any native cell meets that rule.
Report all nine effect sizes and repeat-noise estimates. The Spark gain passes
if both 4,096-row, one-row-batch cells are at least 10 times faster and show a
paired improvement beyond repeat noise, with the same sign test across those
two cells. Report all 18 Spark cells to show how the gain changes with row
count and batch size. Compare elapsed times within this session only.

## Run

From the Spark workspace, assemble the two clean checkouts using the
first-round `pending_range_1099/run.py assemble canonical` command. Build the
staging archive with that runner's `build` command. Build the candidate archive
with the command below. It uses the same release settings and refreshes the
patched sources without relying on the first-round instrumentation anchors.
Keep all output outside the checkout.

```sh
python3 benches/pending_range_1099_recheck/run.py check
python3 benches/pending_range_1099_recheck/run.py build SOURCE TARGET CANDIDATE_BUILD
timeout 65m python3 benches/pending_range_1099_recheck/run.py run \
  STAGING_BUILD/receipt.json CANDIDATE_BUILD/receipt.json SPARK_CAPTURE OUTPUT
```
