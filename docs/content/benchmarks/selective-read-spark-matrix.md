# Q2/Q4 comparison with single-machine Spark

Comparison revision 6 replaces Daft with Apache Spark and the native Delta Lake
connector in the eight-case production comparison. The ordered reader roster is
`delta-arrow-reader, delta-rs, duckdb, polars, spark`. Daft's historical records
remain under their original comparison revisions and archived harness sources.
The owner cancelled the preceding five-reader formal attempt before it produced
timing samples; it must not be resumed or pooled into revision 6.

## Fixed inputs and readers

Use the existing SF10-derived Q2/Q4 production fixtures and query definitions:
Q2/Q4, localized/scattered, and no-DV/DV. File, row-group and page layouts, stored
columns, projected values, predicates and paired deletion semantics are unchanged.
Use corrected DV logs that retain `invariants` and `appendOnly` when upgrading
writer protocol 2 to 7. The earlier logs omitted those legacy writer features;
keep their records under their original identities. This metadata correction
changes fixture hashes and requires fresh references/certificates, while Parquet
and deletion-vector content remain unchanged.
The [production execution contract](selective-read-production-workloads.md) and
[sampling amendment](selective-read-sampling.md) apply except for the reader roster
and Spark's result-delivery API specified here.

Keep delta-arrow-reader 0.6.1, delta-rs 1.6.6, DuckDB 1.5.5 and Polars 1.44.2.
Spark uses 4.1.1, Delta Lake 4.3.1, Hadoop 3.4.2, AWS SDK bundle 2.29.52 and
Temurin `21.0.8+9-LTS`. Python dependencies and all native artifacts are pinned in
the [Spark preparation lock](https://github.com/mag1cfrog/delta-arrow-reader/blob/main/benches/selective_read/runners/spark/lock.json).
New builds must bind this document and the updated request/oracle helpers. A
matching engine version does not make an old executable or certificate current.

Each reader receives the same eight assigned logical CPUs, an 8 GiB limit for its
entire process tree and zero swap. Spark uses `local[8]` with a 4 GiB JVM heap.
The shared progressive network profile remains 200 ms +/- 20 ms and 150 Mbps,
with seed `q2-full-network-v1`. Keep MinIO, proxy and observer CPU placement,
budgets and cache policy unchanged. Performance runs remain manual.

## Spark clocks and full-result consumption

Start a fresh Spark session/JVM for each independent invocation. Record startup
separately from the open/query interval. Opening the selected Delta snapshot and
consuming the complete projected result remain inside the open/query clock.
Reuse initialization opens one versioned native source; plan each of the two
identical queries separately against it. Do not cache or persist query results.

Spark consumes the complete result through `DataFrame.toArrow()`. This is a
collected Arrow result, so `first_batch_ns` is unavailable with an explicit
collected-API reason. Do not invent a streaming timestamp. The API's driver result
limit and the total process-tree cap still apply. The current selective outputs
are small; a different broad-output workload would need a new contract.

Untimed validation exports every projected value after the query clock and uses
the existing independent full-input oracle. Missing versions, corrupt/missing
data and unsupported reader features retain their actual failure category.
Native Spark plans and I/O evidence are separate invocations after timing.

## Freeze and report

Revision 6 requests, references, workloads, schedules and observations use this
document's SHA-256 as `protocol_sha256`, retain the base and sampling hashes, and
bind the new workload manifest. Its reader order, inventory and translation locks
must match the fixed roster. Only Polars needs a native-expression translation;
the other readers parse the canonical SQL directly.

Before timing, prepare fresh builds and exact-value certificates under revision
6. Retain five independent samples, one warmup, isolated open and two-query reuse,
the 1800-second native phase deadline and 60-second cleanup deadline. Fast readers
are not padded or repeated more often to match another engine's duration.

Use a new campaign ID and schedule for every staged snapshot. Keep all eight cases
and both profiles in the overview, including pending, failed or unsupported
entries. Reports cannot mix revisions, build identities or execution conditions,
and partial or historical samples cannot fill a new schedule. Historical runs
require their recorded frozen harness; preserve those bytes and identities.
`publication_ready` remains false until complete reproduction and report review.
