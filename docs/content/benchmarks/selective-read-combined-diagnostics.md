# Combine plan and I/O diagnostics

This campaign amendment applies to comparison revision 6 when
`--combined-diagnostics` is selected before the campaign starts. It replaces the
separate plan and I/O invocations for delta-arrow-reader, delta-rs, Polars and
Spark with one traced native diagnostic invocation per reader/profile. The
existing native adapters export their plans and consume every query result.

DuckDB keeps its separate plan invocation and its traced I/O invocation. Its
EXPLAIN can read additional Parquet data, which would otherwise enter the I/O
totals. The completed Q2 scattered no-DV campaign recorded about 14 MB more
traffic during DuckDB's open plan invocation than its I/O invocation. The other
four readers recorded equal request counts and response bytes for those stages
in both open and reuse profiles.

## Execute a new campaign

Add `--combined-diagnostics` to the existing
[campaign command](selective-read-campaign.md) with a revision 6 workload.
The controller freezes `combined_diagnostics: true` and this document's SHA-256
as `diagnostics_amendment_sha256` in `campaign.json`, before timing. The frozen
schedule retains one warmup and five independent timed invocations per runnable
reader/profile. All plan capture and detailed tracing follow all timing slots.
Exact-value validation, two newly planned reuse queries, native reader builds,
resource limits, query deadlines, transport and cache policy remain unchanged.
No reader rebuild or new reference is needed for this controller amendment.

With all five readers and both profiles, a case needs 72 post-validation
invocations instead of 80. The eight omitted invocations were plan-only sessions
for the four combined readers. DuckDB still has two separate diagnostics.

## Audit and report

The audit regenerates the schedule from the declared mode, verifies the amendment
hash and requires every successful combined diagnostic to retain reconciled I/O
and a nonempty native plan for every query. Plan hashes bind the exported files
to their observations. Missing plans, changed plan bytes, altered slots and
missing tracing evidence fail the audit.

I/O totals cover the diagnostic invocation, including any work performed by
native plan export. Raw request logs and stream-completion boundaries remain
available; diagnostic latency never enters headline timing distributions.
The overview retains each campaign's diagnostic mode and amendment hash. Formal
timing from separately declared diagnostic modes can share an overview because
the diagnostic change occurs after timing and uses the same reader builds and
execution conditions. Keep the mode visible when comparing I/O evidence.

Without the flag, the original schedule applies. Campaigns without the new field
also retain that schedule. Do not edit a running or completed campaign to enable
the amendment, replace partial samples or relabel historical artifacts.
