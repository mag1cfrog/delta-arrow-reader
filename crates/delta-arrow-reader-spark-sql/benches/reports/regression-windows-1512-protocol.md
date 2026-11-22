# Regression window timing protocol (#1512)

Time the nine original regression windows from the #1391 manifest on
the local Linux host. Use the unmodified `functions` harness, its seeded
million-row input, its projection statements and its fixed context settings.
Use the nine original IDs in
[cases.json](../regression_windows_1512/cases.json), in manifest order. ANSI off and on are separate configurations.

## Build and correctness

- Build `cargo +1.96.0 build --offline --locked --release --bench functions`
  on the staging runtime at `b78d82bf` and on the reviewed candidate, with two
  build jobs and `profile.release.debug=1` on both. The attribution merge added
  only reports and fixtures, so the staging binary has the same runtime code
  and harness as the product PR's parent. Record the source commits, binary hashes and manifest
  hash. Use independent copies of the executables.
- Freeze the protocol and selected IDs before the first clock. One Linux
  session runs both binaries. Validity requires the same executed Arrow fields
  and row count for each matched timing cell, as checked against the committed
  reduced-input fixtures. The seven corrected windows must pass exact Spark
  value and type comparisons on the recorded frames. The aggregate and
  unchanged window controls must match the recorded DAR baseline. The old
  seven-window value differences are the purpose of the issue. The unchanged
  REGR_COUNT control uses its DAR baseline type.
- If a candidate window differs from its specified reference, leave that
  window out of both timed arms and list every mismatch by statement, ANSI
  mode, row and magnitude. Leave #1512 open. A failure of an unrelated test
  blocks the product merge.

## Schedule and decision

Use four blocks, placing each binary first twice for each ANSI mode. A new
process runs all eligible cases per arm. Start with ANSI off in blocks 1 and 3
and ANSI on in blocks 2 and 4. Staging goes first for blocks 1 and 4 and the
candidate goes first in blocks 2 and 3. Each process inherits only the fixed
benchmark runtime settings, runs on CPUs 2 through 5 and is bounded to 12 GiB
virtual address space. Cap a process at 240 seconds and the session at 45
minutes. Run no other local builds or timings during the session.

Each case warms one execution/projection pair and retains the next three
pairs in the harness's alternating query/projection order. Collect all
streamed events including partial runs. Require three samples and matching
completion per case and process. Each sample records 1,000,000 input and
output rows and the signed execution minus projection measurement. If a
process or any cell fails, retain its samples, report the missing cells and
leave #1512 open. The schedule runs once.

Report the median execution ns per input row for every valid case, its
projection and signed increment on both arms, and the change in execution
cost. Aggregate 12 retained samples per arm and mode. Completion and exact
results govern acceptance. A measured slowdown is recorded and does not
disqualify a correctness fix. Commit the raw sample archive with the report.

From the Spark workspace, invoke the committed coordinator once with the two
receipts and a new archive path.

```sh
python3 benches/regression_windows_1512/run.py \
  BASELINE/receipt.json CANDIDATE/receipt.json \
  benches/regression_windows_1512/cases.json SAMPLES.jsonl.gz
```
