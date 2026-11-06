# Large LIKE boundary handoff for #771

This branch preserves three existing ignored Rust probes for a host with enough
available RAM. All three are `UNEXECUTED`. Rust compilation, native execution,
measured peak RSS, rustfmt, and Clippy remain unvalidated for this handoff.

The branch starts at observer commit
`5fa08928262a0dc149333685cafd185a5ebc8b40` (tree
`1827f4bfeb170c750d24f8ce36f62f48779e29e5`), based on frozen product commit
`028c10625608053b34b76421379b6dc2df1c5756` (tree
`da82213cc6b7ab458d7f2ded0bef70e09c4551b3`). The observer appends the exact
existing test module to `spark_like.rs`. This publication adds only this handoff
directory and retains that historical source base.

The production `spark_like.rs` bytes also match commit
`5a29cd2fb539aafa137053276ba27c72bbd9d344`, with SHA256
`679c3f3ce0d02ff0e2e9ccc4f1f44e62130bedb5d8361d70a462aed94f2c6e33`.
That comparison covers this component only. It provides no whole-tree validation
or execution evidence for current staging.

## Files and fixtures

The runnable probes are already in
[`spark_like.rs`](../../crates/delta-arrow-reader-spark-sql/vendor/sail/crates/sail-function/src/scalar/predicate/spark_like.rs),
inside `c06_large_boundary_observer`. Keep their inputs and assertions unchanged.

- `unit_append.rs` is the byte-identical module snapshot, including the procedural
  fixtures and bounded oracle. Do not append it again.
- `input-and-resource-policy.json` preserves the original policy verbatim.
  Its `source_revision` and `production_pins` describe the historical policy
  packet. `source-pins.json` binds all 2888 files in the frozen observer tree;
  it excludes this handoff directory. `origins.json` embeds the original diff
  text recording the parser re-exports between the policy and frozen cast source.
- `CAPTURE-INVENTORY.json` defines the expected observations and unavailable
  fields. It contains no actual large result.
- `origins.json` records hashes, historical attribution, source identity, status,
  and the latest Mac admission.
- `mac-reference/run.py` is the unchanged, source-reviewed Mac runner. Read its
  [environment requirements](mac-reference/README.md) before adapting it.

The fixtures generate their bytes in Rust. There are no large input files to
download. The independent oracle checks every actual byte against constant
4096-byte chunks, every row and offset or view, and Arrow full validity. It never
allocates a giant expected value or fabricates lengths, offsets, or backing bytes.

## Cases and resource policy

Each case uses escape `!`, non-NULL inputs, and source scalar `z`.
One GiB is 1073741824 bytes. Admission requires the process budget plus a separate
2 GiB system reserve, using a fresh measurement under the actual host guard.

| Case | Actual input and expected helper output | Process budget | Admission threshold | Status |
| --- | --- | --- | --- | --- |
| `aggregate_large_utf8` | 2048 rows of 1048576 ASCII `a` bytes, materialized as 2 GiB `LargeUtf8`; unchanged 2 GiB `LargeUtf8` output | 8 GiB | 10 GiB | UNEXECUTED |
| `dynamic_view_expansion` | 262144 real views of 4096 backslashes, 1 GiB logical input; 8192 backslashes per output row, 2 GiB `LargeUtf8` | 7 GiB | 9 GiB | UNEXECUTED |
| `scalar_view_expansion` | One 1 GiB `Utf8View` scalar of backslashes; one 2 GiB `Utf8View` scalar, then a checked real view over the returned owned bytes | 11 GiB | 13 GiB | UNEXECUTED |

The budgets are policy limits, with source-derived allocation estimates. They
are not measured peaks or hard bounds. On the original Mac, only free plus
speculative pages receive availability credit. Inactive, purgeable, compressed,
or total physical memory does not replace a fresh admission measurement. Another
OS needs a reviewed equivalent that accounts for the process/container memory
limit and the host reserve.

The latest Mac receipt measured 901595136 available bytes
(0.8396759033203125 GiB) at `2026-11-06T19:21:40.390750+00:00` on an 18 GiB
host. It admitted none of the three cases and ran no engine or large allocation.

## Build and run on a receiving host

Review the receiving host's serialization, resource admission, watchdog, process
cleanup, and evidence capture before executing any build or probe. Run every
engine command as a child of that actual guard and lock. The Rust environment
assertions below are identity checks; they do not enforce memory limits.
This packet supplies no portable Linux controller. Adapt and review orchestration
separately while retaining the existing probes and policy.

Use an installed Rust 1.96.0 toolchain. From
`crates/delta-arrow-reader-spark-sql` in this branch, the existing build argv is:

```sh
cargo +1.96.0 test --locked --workspace --lib --no-run --message-format=json
```

Capture the complete build output. Select exactly the `sail-function` library
test executable from Cargo's artifact JSON, retain an exclusive copy, and record
its SHA256, mode, and original artifact record. Run that same retained executable
in a fresh process for each case, serialized under the guard. Check its hash
before every case and at closeout.

For each child, the controller must set these exact environment gates:

| Variable | Value |
| --- | --- |
| `C06_ENABLE_REAL_LARGE_BOUNDARY` | `1` |
| `C06_LARGE_BOUNDARY_CASE` | The exact case name from the table above |
| `C06_LARGE_BOUNDARY_RUN_TOKEN` | A fresh nonempty nonce, unique to this child |

The following are child command templates for that controller. They assume the
controller has already admitted the case, installed its watchdog, set the case's
gates, and bound `RETAINED_SAIL_FUNCTION_TEST_EXECUTABLE` to the hashed copy.
Do not execute them directly from an unguarded shell.

```sh
"$RETAINED_SAIL_FUNCTION_TEST_EXECUTABLE" \
  scalar::predicate::spark_like::c06_large_boundary_observer::aggregate_large_utf8 \
  --exact --ignored --nocapture --test-threads=1

"$RETAINED_SAIL_FUNCTION_TEST_EXECUTABLE" \
  scalar::predicate::spark_like::c06_large_boundary_observer::dynamic_view_expansion \
  --exact --ignored --nocapture --test-threads=1

"$RETAINED_SAIL_FUNCTION_TEST_EXECUTABLE" \
  scalar::predicate::spark_like::c06_large_boundary_observer::scalar_view_expansion \
  --exact --ignored --nocapture --test-threads=1
```

Retain all three case records, including every unavailable or unexecuted case.
Before tools/build and before each case, measure admission under the same lock.
The existing runner uses an 1800-second case timeout, a 3600-second build timeout,
process-group RSS samples, and reserve checks. A receiving controller must enforce
the budgets and reserve, terminate and drain children on failure, and audit its
own closeout. Sampling and termination are best effort; an OS kill never passes.

For every attempt, retain raw command vectors and stdout/stderr, toolchain and
test-executable hashes, source commit/tree/index identity, effective Cargo
configuration and relevant build environment, all resource samples and RSS units,
admission decisions, case nonces, exits/signals, elapsed time, and lock identity
before/after complete process-group drainage. Retain both native begin/complete
JSON records and raw logs. Validate each token, type, row count, validity,
offset/view, complete-byte count, and proof scope against the capture inventory.
An exit code or summary alone is insufficient.

Preflight rejection, budget/reserve excess, timeout, OOM/signal, or incomplete
execution stays unavailable. Build, sampler, assertion, observation, identity, or
closeout failure receives no acceptance credit. Preserve all outcomes and review
the complete evidence before accepting a helper-only pass.

## Scope of a successful observation

These probes call `SparkLike::native_like_args` and stop before `apply_cmp` or
regex matching. They test the fixed rewrite and representation boundaries only.
They provide no Apache Spark observation, final Boolean match, full LIKE
compatibility, ANSI-mode coverage, sliced-layout matrix, or SQL planning evidence.
Analyzed, optimized, and physical SQL `Field` names, declared nullability, and
metadata are unavailable. Keep them unavailable; expected fields and empty
metadata must not be substituted for native observations.
