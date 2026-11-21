# SF1 string functions, second round (#1388)

Recheck isolated SUBSTRING, CONCAT and UPPER in one session. The
[queries](queries.json) are unchanged from the first attribution.
Run the pinned staging product in [protocol.json](protocol.json).
Reuse the #1380 SF1 snapshot and the frozen execution harness,
including the direct Parquet reader, view, CPUs 2-5, two workers,
two output partitions, and batch size 8,192. Check the six Parquet
files, both Delta logs and the source manifest against the fixture
receipt. Use the Linux release profile with debug level 1 and the
same flags as the original execution benchmark.

First export all results for native and both ANSI modes. Compare
decoded rows, NULLs and multiplicity, both SQL and executed Arrow
types, source file identities and exact byte counts. Exclude a
shape from all clocks if any mode differs. Unexpected harness or
comparison failures stop the run. Every retained timing sample
must match its preflight row count, partition count and source I/O.

Use six fresh processes per path and six native A/B pairs per
eligible shape. Interleave shapes, rotate through all six path
orders, and alternate native control order. Each process runs one
warmup and three retained iterations. Compute medians of process
medians. A shape needs attribution only if an ANSI mode exceeds
native by more than the absolute difference between the A and B
native medians. Report all six within-replica ANSI-on minus
ANSI-off deltas, with their signs, to show whether the earlier
SUBSTRING mode gap repeats.

The schedule has at most 90 processes, 270 retained iterations and
30 minutes for all clocks. Preflight retains the legacy per-process
and 30-minute bounds. Profile the slower ANSI path once for each
shape that exceeds its native floor. Keep raw files in a new local
directory.

From the repository root, run the [controller](run.py) after this
protocol is committed. Provide the release executable from the
pinned staging source and the existing snapshot root.

```sh
python3 crates/delta-arrow-reader-spark-sql/benches/strings_round2_1388/run.py \
  BINARY SNAPSHOT NEW_EVIDENCE_DIRECTORY
```
