---
title: Project direction and Python plan
description: Core Reader priorities, an independent Python binding plan, and an optional DataFusion Python integration investigation.
---

# Project direction and Python plan

The project focuses on reading Delta tables into Arrow batches. On September 27,
2026 (UTC), the maintainer stopped investment in the Spark SQL compatibility
initiative. The [Spark/Sail experiment](spark-sql-experiment.md) is preserved as
an unfinished snapshot. Completing it is not a prerequisite for Python bindings,
publishing, or core Reader work.

## Core Reader work

The existing Rust reader and optional DataFusion provider remain the foundation:

- Delta snapshots, metadata replay and refresh, with stable snapshot ownership.
- Asynchronous Parquet reads, predicate-driven pruning and decoding, selective
  I/O, and page/offset-index optimizations where applicable.
- Correct deletion vectors, projection, NULLs and row counts.
- Incremental streaming, bounded scheduling, cancellation and resource control.
- DataFusion integration through the same planning and scan execution path.

This decision introduces no new runtime implementation. It does not approve a
dependency upgrade or relax an existing correctness requirement.

## Python can proceed independently

The first useful Python release is a binding to the core Reader. The existing
[Python umbrella #101](https://github.com/mag1cfrog/delta-arrow-reader/issues/101)
and its focused leaves already define most of the desired API. Retain their
error, ownership and validation contracts rather than redesigning them.

| Work | Existing owner | Boundary |
| --- | --- | --- |
| Install/import, load a latest or versioned snapshot, schema/version, reader errors | [#102](https://github.com/mag1cfrog/delta-arrow-reader/issues/102) | Independently actionable; no Spark or SQL frontend prerequisite |
| Shared interruptible waits and runtime shutdown | [#417](https://github.com/mag1cfrog/delta-arrow-reader/issues/417) | Verify through table loading before adding streaming |
| Projection/limits, Arrow C Stream and `pyarrow.RecordBatchReader` | [#105](https://github.com/mag1cfrog/delta-arrow-reader/issues/105) | Reuse the native scan; no eager collection or second producer queue |
| Refresh and existing scan/resource controls | [#106](https://github.com/mag1cfrog/delta-arrow-reader/issues/106) | New snapshots do not retarget existing readers |
| Supported predicates and exact schema-aware scalar conversion | [#107](https://github.com/mag1cfrog/delta-arrow-reader/issues/107) | Core predicate semantics; no Spark parsing or new expression language |
| Platform wheels and installed-package checks | [#418](https://github.com/mag1cfrog/delta-arrow-reader/issues/418) | Validate the completed Reader API on all four wheel targets |
| Release-plz and Trusted Publishing integration | [#109](https://github.com/mag1cfrog/delta-arrow-reader/issues/109) | Reuse the validated wheel builder; first publication remains on #101's release checklist |

The implementation order is #102, #417, #105, then both #106 and #107, followed
by #418 and #109. The native `#113 blocks #102`
relationship and the `#112 blocks #109` SQL-release gate have been removed.
The Spark experiment and deferred SQL investigation were detached from the first
Python release umbrella. Publishing depends on the validated wheel builder
(#418), controls (#106), and filters (#107).

The binding must preserve single-transfer Arrow ownership, retained batches and
readers after Python table deletion, terminal errors rather than successful EOF,
early close, GIL release during blocking work, interruption and safe final runtime
shutdown. The binding owns its runtime; import must not start it, and the Rust
core must not acquire a hidden runtime. Keep existing redacted error contracts.

The Python package now builds from source with type stubs and the Rust workspace's
shared version. It supports snapshot loading, schema/version inspection, and
full-table PyArrow readers and single-use stream exports. Projection, limits,
platform wheels and publishing remain planned work. Python
asyncio, writes, a dataframe/query product and JavaScript bindings remain outside
this first release.

## Optional SQL access through DataFusion Python

This is a design investigation, not an implemented or tested Python adapter.
The earlier lazy `Session`/`Table` proposal in
[#112](https://github.com/mag1cfrog/delta-arrow-reader/issues/112) is deferred and
retained as historical design. It must not hold up Reader bindings or publishing.

The current Rust API already exposes `DeltaTableProvider::try_new` and `ScanOptions`
in `src/reader/datafusion.rs`. The provider owns a loaded snapshot, plans projection
and filters through the existing Reader planner, and creates `DeltaScanExec`.
Its execution path uses the Reader scheduler. The `datafusion` feature remains
optional for Rust users. There is no need to introduce Sail to expose this provider.

DataFusion Python documents a custom-provider protocol: a Python object exposes
`__datafusion_table_provider__`, returning a capsule named
`datafusion_table_provider` containing `FFI_TableProvider`. A consumer can register
it in its own `SessionContext`. This supports investigating a small wrapper over
the existing provider while leaving SQL parsing, planning and query APIs with
DataFusion Python. See the
[official custom-provider guide](https://datafusion.apache.org/python/user-guide/io/table_provider.html).

An ordinary Rust trait object must not be passed between independently compiled
Python extensions. The
[official FFI explanation](https://datafusion.apache.org/python/contributor-guide/ffi.html)
describes the ABI boundary and why directly depending on the `datafusion-python`
Rust crate is not a reliable substitute for that protocol.

Version selection is unresolved. The inspected Reader baseline uses DataFusion
54.1.0; do not infer compatibility with every DataFusion Python wheel from the
guide's minimum version. The guide's simple constructor and the
[upstream example](https://github.com/apache/datafusion-python/blob/main/examples/datafusion-ffi-example/src/table_provider.rs)
already differ: the inspected example accepts a session capsule and uses a
logical codec. Pin matching versions and follow their exact protocol when this
investigation is resumed. No compatibility matrix or wheel build was run here.

A bounded future check should establish:

1. Independently built wheels can register the existing provider using the pinned
   FFI protocol, with a clear error for an unsupported version/capsule.
2. Planning/registration stays lazy, projection and eligible filters reach the
   Reader, and Delta/deletion-vector results agree with direct scans. Unsupported
   filters still receive the correct residual evaluation.
3. Snapshot/provider/runtime owners survive variable deletion; early stream drop,
   failure and interruption release resources and stop further work admission.
4. Schema metadata, view/dictionary types and Arrow buffers survive the boundary.
   SQL and its resource policy belong to the consuming DataFusion context;
   streamed output alone does not bound joins, sorts or aggregations.

The existing Arrow C Stream plan also permits generic Arrow consumers. It does
not by itself prove provider-level predicate pushdown. Decide whether to ship an
optional FFI wrapper only after the bounded check, independently of the first
Reader release. Do not build a second SQL engine or restore Spark compatibility
to answer that question.

## Preserved systems investigations

These remain candidates, with their evidence and unresolved status intact:

- [#44](https://github.com/mag1cfrog/delta-arrow-reader/issues/44): typed-statistics
  reuse for cached Delta scan metadata.
- [#45](https://github.com/mag1cfrog/delta-arrow-reader/issues/45): structured-only
  checkpoint statistics and the Delta Kernel JSON-synthesis contract.
- [#129](https://github.com/mag1cfrog/delta-arrow-reader/issues/129) and
  [#200](https://github.com/mag1cfrog/delta-arrow-reader/issues/200): recorded Kernel
  predicate/projection limitations.

The Parquet predicate-cache finding
[#223](https://github.com/mag1cfrog/delta-arrow-reader/issues/223) is closed after
its unchanged original reproducer passed on Arrow/Parquet 59.0.0 and 60.0.0.
The same test still panics on 58.4.0; applying only the runtime guard from
[upstream PR #9983](https://github.com/apache/arrow-rs/pull/9983) to a private 58.4.0
source copy makes it pass. This confirms the upstream fix for the reported case.
The Reader remains pinned to 58.4.0 and affected; no upgrade or workaround was
adopted. The original report, reproduction and verification remain in the closed
issue. Any future dependency upgrade still needs normal correctness and
performance validation.

No Reader implementation or upstream submission was started for these candidates
during closeout. Isolated Sail findings and rejected prototypes remain in the experiment;
there is no new project to upstream the entire frontend.
