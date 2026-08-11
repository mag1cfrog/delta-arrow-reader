# Architecture

## Purpose

This crate owns the independently extracted, read-only Delta Lake to Arrow
implementation. It provides direct Arrow streaming and an optional DataFusion
adapter without depending on Delta Funnel orchestration.

## Ownership

[Issue #486](https://github.com/mag1cfrog/delta-funnel/issues/486) exported the
validated staging crate and transferred canonical ownership to this
repository. Delta Funnel retains its existing internal reader until it adopts
the independently released package. That adoption is a separate migration
step.

## Current boundary

This crate owns reader configuration, errors, metrics, immutable
snapshot/protocol/schema loading, deletion-vector handling, exact logical
predicates, scan metadata and transforms, deterministic partition planning,
backend-neutral bounded scheduling, and the private NativeAsync and
OfficialKernel file executors. NativeAsync reuses the snapshot engine's object
store, performs projected async range reads or threshold-controlled per-file
buffering, and hands ordered logical batches to the scheduler without creating
a runtime, store, limiter, queue, or public stream. OfficialKernel reuses the
same engine, plan, transforms, DVs, limiter, scheduler, and metrics through one
bounded blocking handoff. It disables physical predicates for DV files that
lack original row indexes and leaves exact residual evaluation to the later
direct and DataFusion surfaces. Dropping a scan stops future scheduling and
closes the handoff deterministically, but already-running synchronous Kernel
dependency work can finish only at its next safe handoff boundary.

The public direct API composes those services into an immutable table handle,
a single-use scan plan, and a pull-driven Arrow batch stream. It applies exact
residual predicates, removes hidden predicate columns, enforces a global output
limit, merges partitions deterministically, and keeps scan metrics accessible
after stream drop.

The optional public DataFusion surface preserves the frozen provider policy:
it validates projections, classifies static filters, retains complete residuals,
accepts partition-only dynamic filters, and executes the same bounded scan
services. Its physical plan preserves cancellation, reader error sources,
per-scan metrics, unknown optimizer statistics, and DataFusion's advisory scan
limit behavior. Registration is intentionally one table at a time; Delta
Funnel retains atomic multi-source workflow registration.

The crate does not yet contain production routing or a compatibility facade.
Later
[#447-family issues](https://github.com/mag1cfrog/delta-funnel/issues/447) own
those remaining boundaries.
