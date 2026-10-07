# Collect I/O and plans during exact validation

For a new revision 6 campaign, `--gate-diagnostics` records storage requests and
native plans during the exact-value gates. It implies `--gate-warmup` and
`--combined-diagnostics`, and requires one snapshot with both open and reuse
profiles. The same native query execution exports every projected value, provides
the storage warmup and collects diagnostic evidence. The later I/O invocations
are removed.

DuckDB retains its separate EXPLAIN invocation after timing. EXPLAIN can read
extra Parquet data, so it must not enter the gate's I/O totals. The other four
readers export their existing native plans during the gate.

DuckDB's merged gate exports the Arrow result schema and every projected value,
without repeating `DESCRIBE bench` after the query. The separate EXPLAIN invocation
still retains the full provider schema. This avoids validation-only Delta-log
reads in the gate's I/O totals.

## Start a new campaign

Add `--gate-diagnostics` to the existing
[campaign command](selective-read-campaign.md). Prepare updated native adapters
and matching workload/reference provenance first. Older adapters do not support
the diagnostic-validation request. Dependency versions, native query definitions,
resource budgets and transport remain unchanged.

The controller freezes `gate_diagnostics: true` and this document's SHA-256 as
`gate_diagnostics_amendment_sha256` before the first gate. Every inventory entry
declares the method before timing. A successful gate must retain the full
exact-value proof, reconciled request capture, stream boundaries, native plans
where required, and completed process/server cleanup. Missing evidence makes the
gate fail and prevents timing for that reader/profile.

With five supported readers and both profiles, a case has ten gates followed by
50 independent timing invocations and two DuckDB plan invocations. This removes
ten I/O invocations from the previous combined-diagnostics/gate-warmup schedule.
The 50 timing slots retain their order, five samples per reader/profile, fresh
processes and clients, and two newly planned queries per reuse invocation. Gate
and diagnostic clocks never enter timing distributions.

## Interpret the evidence

The report identifies the exact gate as the source of diagnostic I/O and retains
plan hashes and the amendment hash. I/O totals cover the full gate invocation,
including native plan export. Local result-export work can affect the diagnostic
duration; that duration is excluded from benchmark timing.

Tracing and plan capture now precede timing and can change preparation costs or
cache residency. This is a distinct preparation method. Keep reader comparisons
within the same declared method; the overview rejects a mixture of campaigns
with and without gate diagnostics. Do not rewrite or switch a running/completed
campaign, pool samples across methods, or replace partial results. Without the
flag, existing schedules and their audit rules continue to apply.
