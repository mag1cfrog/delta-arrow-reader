# Q2/Q4 workload execution contract

Comparison revision 5 runs the eight cases in the
[production shape definition](selective-read-production-shapes.md): Q2 and Q4,
localized and scattered rows, each with and without real deletion vectors.
The workload records every case, including inputs that are not yet prepared.
All five readers remain in each campaign inventory.

## Identity and native execution

The workload freezes the shape definition, completed fixture manifest, source
identity, expanded SQL, projection, literals, native expressions, writer settings,
file sizes and row groups. Probe fixtures carry their actual reduced file and row
counts and cannot be reported as full SF10 tables. A full fixture contains every
original source row and the file geometry declared by its completed manifest.
The main 512 MiB file-target recipes have these dimensions:

| Shape | Files per layout | Stored columns | Output columns |
| --- | ---: | ---: | ---: |
| Q2 | 130 | 416 | 69 |
| Q4 | 60 | 90 | 71 |

Use 131,072 rows per group, a 20,000-row page limit, 1,024-row writer batches,
a 1 MiB page-byte target, plain encoding without dictionaries and Zstd level 3.
Page limits are checked at writer batch boundaries; record actual page lengths
and compressed file sizes separately. The 256 MiB file target and 2,048-row
page limit remain sensitivity controls outside the eight main cases. The
generation-shape contract retains its historical identity; these main settings
follow the accepted physical calibration.

Revision 5 uses this document as `protocol_sha256`, retains the base protocol
and sampling amendment hashes, and adds `sampling_stage` to the identity.
The workload hash binds all remaining input and harness identities. Revisions
2, 3 and 4 retain their original meanings and replay rules.

Readers open a Delta table at a fixed snapshot through their native APIs. Each
reader decides which files and row groups to read. The harness supplies no
selected file list. Every result batch is consumed. Resource limits, pinned
engine versions and clock boundaries follow the existing protocol.

## Validation and sampling

The independent oracle scans the original source with PyArrow, evaluates the
three predicates, and derives payload values in Python. It checks the exact
output values, types, nulls and multiplicity. It scans fixture batches without
Parquet filters, checks stored metric columns independently, and compares every
qualifying fixture row against the source reference. DuckDB only sorts actual
results for exact comparison. Preparation counts cannot certify correctness.

DV pairs retain identical Parquet bytes. Deletions use the existing logical-key
hash rule plus a shared union of nonmatching file minima and the minimum
matching key across the paired input inventory. The oracle checks physical
ordinals against logical keys, deleted and surviving matches, and DV coverage
on candidate and excluded files. Probe unions and full-table unions have
separate fixture identities.

Each job has one untimed exact-result gate and one unreported warmup. A pilot
has two independent timed invocations per supported reader; formal sampling
has five. Each open invocation contains one query. Each reuse invocation
contains initialization and two freshly planned queries on the same native
source. The scheduler uses the existing balanced ordering and records its
prefix when fewer samples than ordering positions are scheduled.

Formal workloads may contain one or more complete full no-DV/DV pairs for the
same shape and layout. A batch can be prepared without keeping all eight
fixtures resident at once. The workload retains the complete eight-case,
five-reader inventory, marking inputs outside that batch as `not_prepared`.
A completed batch does not establish complete publication coverage. Publication
requires all eight cases and their native reuse profiles, fresh independent
exact-result gates and every predeclared formal slot. Pilot and historical
samples cannot fill those slots. Batch reports retain `publication_ready: false`.

Freeze the complete case definitions, source and generated-object hashes,
SQL/native expressions, reference provenance, builds, resources, transport and
schedule before formal timing. Completed generation manifests can define an
input whose bulk objects have been reclaimed; record physical residency
separately. Before uploading or validating an active case, restore and verify
every required object against its frozen identity. Missing objects leave that
case pending.

Plan capture and one traced I/O invocation run after all timing slots. They
do not contribute to query-time distributions. Save requests, response bytes,
GET/HEAD file sets, file-stat candidates and files containing live matches.
Geometry describes pruning opportunities; it does not prove decoded page or
row-group counts. Keep unsupported readers, failed samples, ties and losses.

Formal initialization and each query have uniform 1,800-second deadlines;
startup uses the same bound, with 60 seconds for between-query bookkeeping and
cleanup. Readers retain the existing eight-CPU, 8 GiB, no-swap budget. Data,
staging, source, scratch and references stay within 192 GiB, with builds budgeted
separately. Record the actual affinity and all server/proxy resource limits.

The main emulated transport uses 200 ms request latency, deterministic +/-20 ms
jitter and one shared, progressively paced 150 Mbps response-body budget.
Freeze the proxy build, jitter seed and configuration before timing and use the
same endpoint for every reader. Clients and processes are fresh per independent
sample; MinIO and OS caches remain in place without flushing. Record snapshot
initialization and both fresh queries in each reused native source separately.
Use the same warmup history and keep initialization out of query-only clocks.

This controlled transport is emulation, not a reproduction of private S3 data
or network conditions. Localhost controls and earlier 600-second pilot limits
remain separate from the formal campaign. The historical 68-case inventory
provides supporting mechanism and throughput evidence; it does not replace the
eight-case main comparison.

No CI jobs, large CI runs or performance thresholds are added by this contract.
