---
title: Large-workload pilot capacity check
description: The recorded capacity rejection before the selective-read scale pilot, and the requirements still unmet.
---

# Large-workload pilot capacity check

The 2026-09-28 capacity check did not find a feasible candidate under the
192 GiB preparation allowance. No large data was generated, no native pilot
query ran, and no publication scale or workload manifest was frozen.
[#345](https://github.com/mag1cfrog/delta-arrow-reader/issues/345) remains incomplete.

The [machine-readable record](selective-read-pilot-capacity.json) contains the
predeclared limits, reader/build pins, environment, exact commands, full
preflight plans, exit statuses and hashes of the retained raw evidence.
The committed generator implementation is from
[#349](https://github.com/mag1cfrog/delta-arrow-reader/pull/349).
This is capacity evidence, not a timing result.

## Recorded limits

The host had about 248 GiB free on the local XFS filesystem. Preparation kept
its 16 GiB address-space limit, 4 GiB sort pool, 192 GiB data allowance and
1,800-second invocation deadline. The declaration set a 300-second ceiling
for this capacity screen and a 24-hour ceiling for a later pilot attempt.
No additional build storage was allocated; the existing five pinned reader
builds were identified and their executable hashes checked.

Measured-reader limits remain eight logical CPUs and 8 GiB, with MinIO at two
separate CPU cores and 4 GiB. The normal cache policy remains fresh clients and
reused server/OS caches. This check did not start MinIO or change those limits.

## Preflight results

The first check retains the existing SF1 source as a baseline. The candidate
checks follow SF10, SF30, SF100 and SF300 in order. An additional SF10 check
uses the existing exact source inventory rather than a worst-case row count.
Later-rung capacity checks are not visited timing trials.

| Source input | Rows used for planning | Estimated peak, GiB | Fits 192 GiB budget | Result |
| --- | ---: | ---: | --- | --- |
| Saved SF1 baseline | 6,001,215 | 59.05 | Yes | Preflight passed |
| Fresh SF10 | 105,000,000 maximum | 1,052.05 | No | Rejected |
| Fresh SF30 | 315,000,000 maximum | 3,156.15 | No | Rejected |
| Fresh SF100 | 1,050,000,000 maximum | 10,520.51 | No | Rejected |
| Fresh SF300 | 3,150,000,000 maximum | 31,561.53 | No | Rejected |
| Saved SF10 source | 59,986,052 | 590.60 | No | Rejected |

All rejected candidates also exceed current free disk. The generator returned
nonzero before creating an output directory. Every native reader is recorded as
`not_attempted_capacity`; this is not an unsupported-feature result or a reader
failure.

These are conservative allowances, not measured storage requirements. For
saved SF10, the plan reserves about 229 GiB for each of the reference and
comparison SQLite databases, plus source, table, MinIO and validation-export
space. Those database allowances cover every source row at 4,096 bytes per row.
They are not a prediction of the date30 anchor's output size. The full workload
also requires an unfiltered wide scan, so an anchor-only estimate cannot establish
capacity for the entire inventory. A tighter capacity model would need a reviewed
change and validation before a retry.

## Independent file-pair lower bound

The file-organization requirement cannot fit the present data allowance even
if the conservative SQLite estimates are reduced.

For 4,096 files with median size at least 64 MiB, at least the upper 2,048 files
have size at least 64 MiB. One repacked snapshot therefore contains at least
128 GiB of Parquet data. Its DV snapshot preserves and copies those exact
Parquet bytes, so the two snapshots require at least 256 GiB together.

That lower bound already exceeds the 192 GiB allowance and the approximately
248 GiB free at this check. It excludes the normal layout and its DV copy,
original source, logs, DVs, MinIO, references and validation exports. It does not
assume every file is exactly 64 MiB, and it does not select a TPC-H scale.
The current generator copies the objects into separate files and budgets the
bytes written; this calculation does not rely on filesystem deduplication.

## Reproduce the checks

Use the pinned executable and a new output path, as described in
[Generate public selective-read fixtures](selective-read-fixtures.md).
For example, when the recorded report-profile source exists:

```sh
target/selective-read/release/selective-read-fixtures \
  --profile large --scale-factor 10 --fixture wide.shuffled \
  --source-from ../selective-read-report-314 \
  --disk-limit-mib 196608 --elapsed-limit-seconds 1800 \
  --preflight --output ../pilot-capacity-sf10
```

For a fresh-source check, omit `--source-from` and use each declared scale in
order. The JSON record retains all six exact invocations and the saved-source
manifest hashes. Only preflight reads were performed; the source row inventory
is verified against actual rows and the pinned generator when reuse proceeds.

## Requirements before retrying

Provide a storage location and reviewed allocation that can hold the selected
workload's complete staged preparation and validation. The 256 GiB lower bound
is not a sufficient allocation, and the SF10 estimate does not establish that
SF10 meets either the minute-scale or large-file requirement. Retain the failed
capacity record and declare a new environment/budget identity before a retry.

Then run the native scale pilot and independent file-geometry selection,
validate the complete inventory, estimate formal duration, and freeze the
concrete 340 case/reader and 35 session/reader entries. The
[large-workload contract](selective-read-large-workloads.md) still governs those
steps. The current record has `publication_ready: false`; it cannot populate
formal measurement slots.
