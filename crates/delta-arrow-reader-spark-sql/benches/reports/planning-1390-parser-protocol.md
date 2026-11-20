# Statement parser comparison (#1390, PR2)

Measure the effect of collecting full Chumsky errors only when the inexpensive
statement parse fails. The attribution report measured the parser body at about
9.22 ms for the 1,000-literal IN and 6.70 ms for the 200-branch CASE, compared with
about 0.02 ms to construct the parser. This comparison tests one change to error
tracking. Parser construction still occurs on each statement.
[Attribution](planning-1390.md)

## Before and after

The product baseline is attribution merge
`42cbac183328fceb13ad175d02477fd8758d85c7`, with timings from the canonical
binary in the attribution report. The candidate is the exact clean commit
containing the parser change and this protocol. The replay runner records that
commit, the private dependency and fixture hashes, the sealed executable and
the generated benchmark source. It reuses the canonical driver and native
controls. It uses the same fixture bytes, compiler flags, local Linux CPU 2,
one target partition, two warmups and nine samples per process.

The attribution schedule's 30 canonical processes run once on the new binary:
5 fresh processes per configuration and 5 native-configured A/B pairs. All 25
paired statements and 5 stress statements are measured in native-plain,
native-configured, Spark ANSI off and Spark ANSI on. Each process has a 300
second timeout. The existing before measurements supply the comparison.

## Evidence required before timing

Run the full Spark workspace, root-crate Clippy and formatting check. The
parser differential tests compare the original Rich path with the candidate
on committed valid and invalid SQL. The failure checks retain the error type,
text and source. An isolated release parser probe captures the original and
candidate ASTs for all 309 frozen SQL statements. A separate fixed probe
samples Linux virtual and resident memory after 0, 32, 128, 256 and 512
calls with valid `SELECT 1 + 2` and parser-invalid `SELECT 1 +`. Inspect both
curves before timing to check that the lightweight attempt drops its graph
and the original Rich fallback still has its existing retention behavior.

Make four untimed planning traces against the 678 corresponding canonical
attribution snapshots. Include each paired and stress configuration and all
558 ordinary Spark cases. Reuse the exact plan and Arrow schema comparator. A
difference or a previously eligible planning failure stops the measurement.
A captured plan must show zero started Delta scans, zero file tasks and zero
Parquet data bytes.

## Timing record

Take the median of each process's nine samples and then the median of the five
process medians. Report before and after logical, physical and total medians
for each shape, the Spark gap to native-configured on the same binary, and both
rounds' native A/B difference and process dispersion. The planning wall covers
the parser and the unchanged subsequent stages. The observed per-stage baseline
stays separate. This schedule does not measure a new stage split or execute
query rows. Any measured residual beyond the bounds of this change stays on
#1390.

Run from the Spark workspace. Each output destination must be new. Use the
existing local #1379 Delta root and the owned Rust target directory.

```sh
manifest=benches/planning_attribution/statement-parser.json
runner=benches/planning_attribution/replay.py
python3 "$runner" build "$manifest" "$EVIDENCE/source" "$OWNED_TARGET" "$EVIDENCE/build"
python3 "$runner" trace "$manifest" "$EVIDENCE/build/receipt.json" "$DAR_DELTA_ROOT" "$EVIDENCE/plans"
python3 "$runner" measure "$manifest" "$EVIDENCE/plans" "$DAR_DELTA_ROOT" "$EVIDENCE/run"
python3 "$runner" summarize "$manifest" "$EVIDENCE/run" "$EVIDENCE/walls.csv"
```
