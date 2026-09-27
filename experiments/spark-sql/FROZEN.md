# Frozen Spark SQL experiment

The maintainer stopped this initiative on 2026-09-27 UTC. It is an unfinished
engineering experiment and is not a supported Spark-compatible frontend.
Python Reader bindings are independent of Spark compatibility.

The [decision, architecture, tested versions, results, costs and reproduction guide](https://github.com/mag1cfrog/delta-arrow-reader/blob/main/docs/content/spark-sql-experiment.md)
and [core Reader / Python direction](https://github.com/mag1cfrog/delta-arrow-reader/blob/main/docs/content/project-direction.md)
are maintained with the main project documentation.

The pre-closeout tag `archive/spark-sql-2026-09-27` preserves
`bde063e527547f20e687548cd8cda098c1b0fb88`, the normal integration merge of PR #292.
This documentation commit does not alter that runtime, its oracle, tests or captures.
The `feat/spark-sql-extraction` branch and its history remain available.

## Unfinished work

[#293](https://github.com/mag1cfrog/delta-arrow-reader/issues/293) was stopped
before its private candidate became a repository implementation. The
[frozen patch](frozen-wip/issue-293.patch) records the exact before/after source
difference; [identity.json](frozen-wip/identity.json) pins both private sources
and result files. It is an archival attachment, not an applied runtime change.
The before source is a selected patched runtime, not the default vendor file.
Do not apply the patch to that default file or treat focused executor checks as
independent approval. Raw-change review and cost validation are unfinished.

The [executor stop record](frozen-wip/STOP-executor.md) and
[reviewer stop record](frozen-wip/STOP-reviewer.md) preserve their original reports.
Their private files and binaries remain in place. A verified backup containing the
candidate directory and reviewer evidence is at
`/home/hanbo/.local/share/delta-arrow-reader-review/freeze-2026-09-27/private-evidence.tar.gz`;
SHA-256 `a823602c3fd8b71b2e95cb4cd1e7c143d475603ad68a8779d8a5cfa5344236d0`.
The same directory holds the full Git bundle, manifests and issue snapshots.

## Stop controls

Windows native automation `delta-arrow-reader-linux` is paused. The existing
Linux coordination helper refuses delivery while its `FROZEN` marker exists.
The historical workflow and prompts are inactive and their originals are backed
up. Both tasks ended without a forced repository-write interruption.

The original acceptance requirements and failures remain evidence. Closing
roadmap trackers as not planned does not resolve their open bugs. No further
compatibility cycle, benchmark loop, next-area selection or bulk Sail upstreaming
is authorized. A future restart requires an explicit maintainer decision.
