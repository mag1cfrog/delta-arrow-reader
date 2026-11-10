# Stopped Spark SQL work

Use [HANDOFF.md](HANDOFF.md) to select an implementation continuation. It contains exactly one selected source per resumable issue. [HISTORY.md](HISTORY.md) classifies all other branch names; they are reference and recovery material.

Stopped at the user's request on 2026-11-09. The goal was cleared. Further implementation, repair, integration and merging require a new request.

The unfinished #893 implementation is preserved at `63da7acb50c237fb878667e3846f35bdd8d2e532` on `fix/spark-udf-folding-opt-outs`. It has not received final acceptance. The remaining review findings are an eager-argument error-order issue that needs reproduction and a new `clippy::unnecessary_lazy_evaluations` diagnostic. Integration with later staging changes is also unfinished. `STOPPED.json` records the checks that completed and the remaining scope.

The archive contains all local Git references, recoverable snapshots of 26 worktrees, five stashes, and the collected source, observations, logs and review reports. Three worktrees had uncommitted changes; their snapshots and separate staged/worktree patches are included. Original working files, indexes and stashes were preserved. Rebuildable executables and cache directories are excluded from the evidence archive.

All 1,563 descendant sessions were confirmed archived, including 425 archived during shutdown. No project build or test process was running during the shutdown inspection.

## Recover the work

From a checkout of this archive branch, download the two assets from the unpublished archive draft:

```sh
gh release download archive/spark-work-2026-11-09 --repo mag1cfrog/delta-arrow-reader --pattern source.bundle --pattern evidence.tar.gz
sha256sum -c SHA256SUMS
git bundle verify source.bundle
git clone source.bundle recovered
git -C recovered fetch ../source.bundle 'refs/archive/*:refs/archive/*'
git -C recovered switch --create fix/resumed-spark-udf-folding-opt-outs origin/fix/spark-udf-folding-opt-outs
mkdir restored-evidence
tar -xzf evidence.tar.gz -C restored-evidence
```

`archives.json` contains the two complete-file checksums. `evidence-files.json` contains a checksum for each archived evidence file. `worktrees.json` maps every original checkout to its snapshot reference; fetch those references before choosing a different worktree snapshot. `stashes.json` maps the five saved stashes to their recovery references. The per-worktree patches retain the original staging boundary.

The recovery commands restore files only. They do not restart the cleared goal or resume agents.
