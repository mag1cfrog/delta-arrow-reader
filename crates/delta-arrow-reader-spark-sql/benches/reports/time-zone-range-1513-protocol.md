# Time zone range comparison protocol (#1513)

Compare staging `689be9bc1117c55f0bdb85d93051fa81afe3f40d` with the
reviewed candidate. Build both on Linux with Rust 1.96.0, release
optimization and debug level 1. Use the same pair of observation-only
overlays from `window_aggregates_1408/instrument.py::changes` with phase
clocks disabled. These overlays record exact preflight rows and leave
the timed execution path intact. Run the [builder](../time_zone_range_1513/build.py)
from the candidate workspace, with each pinned Spark crate worktree and a
new output directory. The builder restores both scratch inputs before writing
the executable and receipt. Reuse a single target directory for the builds.

```sh
python3 benches/time_zone_range_1513/build.py STAGING_WORKSPACE STAGING_BUILD TARGET_DIR
python3 benches/time_zone_range_1513/build.py CANDIDATE_WORKSPACE CANDIDATE_BUILD TARGET_DIR
```

## Preflight

Use all 16 statements in the frozen
[#1409 manifest](../time_conversions_1409/manifest.json). They include
the three conversion functions with UTC, Los Angeles and a fixed
offset, plus the earlier time conversion controls. Cover ANSI off
and on and each supplied native statement. Require equal rows by ID
and equal executed Arrow types across both binaries and applicable
configurations. Require the source guard to consume all one million
seeded input rows. Exclude mismatches from all timing arms, report
each exclusion, and leave #1513 open if any case is excluded.

## One paired session

For each configuration, run four blocks of staging A, candidate and
staging B. Alternate A and B order and reverse configuration order
in odd blocks. A and B use the same staging executable. Each
fresh process runs every eligible statement in manifest order.
Pin their CPUs and process memory to #1391's settings. Use one
warmup and three retained query/projection pairs in each cell.
Cap each process at five minutes and the preflight plus timing session
at 45 minutes. Run without other local builds or timing jobs.

Each block compares the candidate process median above projection to
the mean of the two staging process medians for the same statement
and configuration. The acceptance limit is the largest absolute
staging A/B difference for that statement and configuration across
those four blocks. A cell passes if its median paired increase does
not exceed that limit. All cells must pass. A missing block, failed
process, or result type or row-count drift fails the affected cell.
Keep partial samples, report the failure, and leave the issue open.
The schedule runs once.

Keep the report and compressed raw samples together in the same PR.
The archive records the preflight selection, binary identities and
every retained sample. Run the committed coordinator from the Spark
workspace with the two release artifact directories and a new output
directory.

```sh
python3 benches/time_zone_range_1513/run.py STAGING_BUILD CANDIDATE_BUILD OUTPUT
```
