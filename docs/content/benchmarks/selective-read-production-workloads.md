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
counts and cannot be reported as full SF10 tables. A full fixture must contain
every source row and the declared 4096 or 18432 files.

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
prefix when the pilot has fewer samples than positions.

Plan capture and one traced I/O invocation run after all timing slots. They
do not contribute to query-time distributions. Save requests, response bytes,
GET/HEAD file sets, file-stat candidates and files containing live matches.
Geometry describes pruning opportunities; it does not prove decoded page or
row-group counts. Keep unsupported readers, failed samples, ties and losses.

The first full Q4 pilot uses the existing local MinIO storage control. It is
not a reproduction of the earlier S3 network environment. Pilot artifacts and
probe validations are not a publication result. The prior 68-case inventory
remains available as supporting mechanism and throughput evidence.

No CI jobs, large CI runs or performance thresholds are added by this contract.
