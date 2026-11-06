# Archival Mac runner

`run.py` is a byte-identical copy of the source-reviewed R2 runner, SHA256
`b9abbb645b5ca9e1229b68f0113897777aaa43346165dede5a764f1b6af5d939`.
It is reference code for orchestration review. It refuses relocation and accepts
no arguments. Running the copy in this directory fails its fixed-path check.

The runner requires macOS, unoptimized Python with `tomllib`, the complete
original sealed packet, its fixed registry sources, and the approved outer
controller's inherited lease. Its original fixed paths are:

| Purpose | Original path |
| --- | --- |
| Packet | `/Users/hanbowang/Documents/codex-evidence/c06-2026-11-02/771-large-helper-immutable-r2` |
| Actual output | `/Users/hanbowang/Documents/codex-evidence/c06-2026-11-02/771-large-helper-immutable-r2-actual` |
| Validator | `/Users/hanbowang/Documents/repos/delta-arrow-reader-mac-validator` |
| Controller/modules | `/Users/hanbowang/Documents/codex-evidence/c06-2026-11-02/mac-shared-engine-adapter` |
| Coordination | `/Users/hanbowang/.local/share/delta-arrow-reader-mac/coordination.json` |
| Cargo home | `/Users/hanbowang/.cargo` |
| Rustup home | `/Users/hanbowang/.rustup` |
| Existing target | `/Users/hanbowang/.cache/dar-c06-target` |

The controller is `run_locked.py`, SHA256
`f0a6a980b163be6b126327fa6a4b44162bb72235be74db777a12eb74f7e25a49`.
The imported `inherited_lock.py` has SHA256
`4a120f00997626dcb64560eef12855ec1736e9a82784f25a4912d43c9fd83783`.
The fixed coordination SHA256 is
`8c91f6b1c7eb1d40a799e0c051dd97812d3a114f7dbe1f2b7cd8f775074fc4a6`.
These local modules, controller/coordination state, and complete original packet
are not included in this handoff. Their hashes record provenance and do not make
the copy independently executable.

The runner expects the controller to stage exact observer commit
`5fa08928262a0dc149333685cafd185a5ebc8b40`, start it in the Spark SQL crate,
provide six build jobs and the pinned target, and hold the authoritative lease
until the entire child process group drains. It uses macOS `vm_stat`, `sysctl`,
`ps`, and macOS byte units for `wait4` maximum RSS. Its source check deliberately
expects the original observer tree, so it also rejects the additional handoff
directory if used unchanged against this branch tip.

A receiving agent must review and rebind the environment, source/configuration
pins, controller lease, resource measurements, RSS units, fixed destinations,
and closeout for its own host before execution. A Linux adaptation is separate
work. Use the existing branch probes and unchanged input/resource policy as the
test contract. The [handoff instructions](../README.md) state the required
commands, environment gates, evidence, and limited proof scope.
