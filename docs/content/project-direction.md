---
title: Project direction and language support
description: Core Reader priorities, Python bindings with SQL, and CLI distribution.
---

# Project direction and language support

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

## Python bindings

Python support shipped in [v0.7.0](https://github.com/mag1cfrog/delta-arrow-reader/releases/tag/v0.7.0)
as a binding to the core Reader, with read-only SQL and a bundled CLI. The
[Python umbrella #101](https://github.com/mag1cfrog/delta-arrow-reader/issues/101)
and its implementation issues record the API, error, ownership, and validation
contracts. Keep those contracts when extending the binding.

| Work | Implementation issue | Boundary |
| --- | --- | --- |
| Install/import, load a latest or versioned snapshot, schema/version, reader errors | [#102](https://github.com/mag1cfrog/delta-arrow-reader/issues/102) | Load snapshots without a Spark frontend |
| Shared interruptible waits and runtime shutdown | [#417](https://github.com/mag1cfrog/delta-arrow-reader/issues/417) | Release the GIL during waits; allow interruption and safe shutdown |
| Projection/limits, Arrow C Stream and `pyarrow.RecordBatchReader` | [#105](https://github.com/mag1cfrog/delta-arrow-reader/issues/105) | Reuse the native scan; no eager collection or second producer queue |
| Refresh and existing scan/resource controls | [#106](https://github.com/mag1cfrog/delta-arrow-reader/issues/106) | New snapshots do not retarget existing readers |
| Supported predicates and exact schema-aware scalar conversion | [#107](https://github.com/mag1cfrog/delta-arrow-reader/issues/107) | Core predicate semantics; no Spark parsing or new expression language |
| Platform wheels and installed-package checks | [#418](https://github.com/mag1cfrog/delta-arrow-reader/issues/418) | Validate installed reader, SQL, and CLI APIs on supported wheel targets |
| Release-plz and Trusted Publishing integration | [#109](https://github.com/mag1cfrog/delta-arrow-reader/issues/109) | Publish the exact wheels validated on the release PR |
| Read-only SQL through Rust DataFusion | [#112](https://github.com/mag1cfrog/delta-arrow-reader/issues/112) | Register loaded snapshots and stream query results through the existing binding |

The core binding and SQL session API are released. Their installed tests run
with the shared wheel checks before publication. The Spark experiment remains
outside the release.

The binding must preserve single-transfer Arrow ownership, retained batches and
readers after Python table deletion, terminal errors rather than successful EOF,
early close, GIL release during blocking work, interruption and safe final runtime
shutdown. The binding owns its runtime; import must not start it, and the Rust
core must not acquire a hidden runtime. Keep existing redacted error contracts.

The [Python package](https://pypi.org/project/delta-arrow-reader/) is available on
PyPI with the Rust workspace's shared version. It supports snapshot loading,
schema/version inspection, column projection, native filters, row limits,
PyArrow readers, single-use stream exports, refresh, and read-only SQL through
registered Delta snapshots. See the
[installation guide](../../crates/delta-arrow-reader-core/docs/public/installation.md#python)
for wheels and source builds. Python asyncio, writes, a dataframe expression API,
and JavaScript bindings remain outside this release.

## CLI access from other languages

The [CLI workstream](https://github.com/mag1cfrog/delta-arrow-reader/issues/428)
exposes the existing reader through a subprocess: `dar inspect` returns JSON,
and `dar scan` streams Arrow IPC. The [Go example](../../examples/go-cli)
demonstrates process cleanup, cancellation, and streaming consumption without
cgo. This boundary does not require a hosted service or another scan engine.

Linux amd64 and arm64 releases use the workspace version. Heavy binary,
distribution, image, and Go checks run on the release PR after approval.
Publication reuses those exact archives and OCI images in GitHub Releases and
GHCR. Version 0.7.0 is the first public standalone CLI release. See the
[release runbook](cli-release.md) for recovery and publication
evidence, and [installation](../../crates/delta-arrow-reader-core/docs/public/installation.md#cli-binaries-and-container-images)
for supported runtimes and version/digest pins. The parent issue tracks the
first-release verification record.

## SQL in the Python package

[#112](https://github.com/mag1cfrog/delta-arrow-reader/issues/112) added a `Session`
that owns a Rust DataFusion context inside our PyO3 extension. Users register
loaded `DeltaTable` snapshots and call `sql()` to get a PyArrow reader. The
existing Rust provider handles Delta scans and eligible projection/filter
pushdown; DataFusion handles SQL planning and computation.

The initial API consists of session construction, table registration and read-only
SQL execution. It reuses the binding's interruptible runtime waits and Arrow
stream exporter. Existing readers retain their snapshots and owners when Python
variables are deleted or the source table is refreshed. Joins, sorts and
aggregations can retain memory despite streamed output.

The Python extension enables the existing Rust `datafusion` feature. DataFusion
remains optional for Rust-only users. There is no dependency on the separate
`datafusion` Python package or on `datafusion-ffi`.

Registering our provider in an independently installed DataFusion Python session
is a different integration. It would need the documented provider FFI boundary
and version compatibility checks. It is outside this release, along with a
Python dataframe expression API, writes, Spark compatibility and asyncio.

Every release requires installed SQL acceptance on the supported wheel platforms,
along with the reader and CLI checks. #101 and #428 retain the first-release
checklists and verification evidence.

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
