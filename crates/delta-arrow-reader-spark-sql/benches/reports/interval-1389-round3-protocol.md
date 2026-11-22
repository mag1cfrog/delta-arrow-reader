# STRING to INTERVAL third-round protocol (#1389)

Measure the three unchanged SF1 interval statements and add one
[quoted year-month case](../interval_1389_round3/quoted-query.json).
It wraps the same input string in single quotes before the Spark
cast. The native reference uses the equivalent unquoted string.
The quoted case includes the cost of forming the quoted string.
Keep the day-time input projection as the same-source I/O reference.

## Baseline and build

Use the #1389 SF1 snapshot, 8,184 rows, CPUs 2-5 and Rust 1.96.0 on
Linux. Build at optimization level 3 and debug level 1 using the
workspace's release settings. The builder adds the quoted case to
the benchmark's embedded manifest in a scratch checkout and restores
the original harness afterward. Each receipt pins the workspace
source, executable, harness and statements. The earlier work files
are unchanged.

For the baseline, use the executable built from `310ffad9`, on top of
staging `219ba018`, in both arms. Subsequent driver edits check the
comparison and record its provenance. The baseline PR
compares the four casts and reports the two sets of medians. It
completes when the frozen schedule and all result checks complete.

## Correctness and schedule

Execute preflight in both ANSI modes, using the same snapshot in both
builds. Compare all rows with multiplicity, executed Arrow and SQL
types and source I/O. Compare the paired Spark cases against native
and the day-time result across the ANSI modes. Require each
day-time cast to open the same files and bytes as its projection.
Exclude a mismatching shape, report it and leave its acceptance
incomplete.

Use the earlier [phase schedule](../interval_1389/comparison.md) for
each included case and its applicable controls. Schedule three
replicas, each with one warmup and three retained samples. Switch
build order across replicas for each configuration and noise arm.
The full schedule has 162 processes and 486 retained samples.
Use 300 and 180 seconds per preflight and timing process respectively,
with separate 30-minute totals. Run without other local builds or
clocks. Retain every sample with the report. `source_commit` in the
samples identifies the original workload. `binary_source_commit`
identifies the exact source of that arm's release executable.

## Comparing a correction

Run the same schedule once for the correction and its staging
baseline, using distinct binaries. Pin the baseline source commit in
the correction's protocol and pass it as `STAGING_SOURCE`. Within each cell, pair the
candidate and staging process medians by replica. The gate uses
the median paired change and the range of the three staging process
medians. Require every Spark cast cell to improve by more than that
range. A regression beyond the staging range on a native or
projection control goes through the instruction and state layout
check. The driver flags such controls before that review. Record their
dispositions in the report. An unresolved or changed-code regression
fails the gate. Any missing process, sample, required correctness
comparison or cast gain leaves the correction unmerged.

Report the cost above the native reference, or the same-mode
projection for day-time, for each cast. Use each build's native
A/B repeat difference and the day-time cast-minus-projection repeat
difference to show the scale of remaining costs. Improvement and
remaining cost are both reported.

```sh
python3 -B benches/interval_1389_round3/build.py WORKSPACE BUILD TARGET_DIR
python3 -B benches/interval_1389_round3/run.py STAGING_BUILD CANDIDATE_BUILD SNAPSHOT OUTPUT STAGING_SOURCE
```
