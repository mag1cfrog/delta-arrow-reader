---
title: Validate public selective-read results
description: Prepare independent reference results and check untimed reader output before accepting benchmark measurements.
---

# Validate public selective-read results

The oracle checks the 30 original/wide public query cases in the
[comparison protocol](selective-read-protocol.md). It reads every source row,
evaluates predicates with Rust integers, calendar dates and scaled Decimal
values, and derives wide payloads from the published hash rule. The Rust oracle
owns reference generation, Delta/DV checks, external sorting and exact value
comparison. A small PyArrow process supplies unfiltered Parquet batches, keeping
its C++ decoder independent of the measured Rust readers. It receives no
predicates, file-pruning decisions or deletion lists.


Each reference also checks the selected fixture's qualifying rows against the
saved source. This detects changed values, payloads, nulls, or duplicate rows
before an engine runs. All qualifying rows are checked even for a LIMIT case.
The same checker also accepts [file-organization controls](selective-read-files.md),
[within-file controls](selective-read-within-file.md), and
[paired DV snapshots](selective-read-deletion-vectors.md). DV references apply
independently checked deletion lists and retain both physical and live row counts.

## Install and prepare a reference

Run from the repository root on Linux x86-64, using CPython 3.14.6. The
requirements file pins PyArrow 25.0.1 and DuckDB 1.5.5 with their wheel hashes.
DuckDB is used by fixture layout preparation. Reference generation and result
comparison use Rust and PyArrow. Use an environment outside the checkout:

```sh
uv venv --python 3.14.6 ../selective-read-oracle-venv
uv pip sync --python ../selective-read-oracle-venv/bin/python --require-hashes \
  benches/selective_read/oracle-requirements.txt
```

[Generate public fixtures](selective-read-fixtures.md) first, then prepare one
reference per case in a new directory:

```sh
cargo +1.98.1 run --locked -p xtask -- selective-read prepare-reference \
  --python ../selective-read-oracle-venv/bin/python \
  --fixtures ../selective-read-smoke \
  --case wide.clustered.eq2-in20 \
  --output ../selective-read-reference-wide
```

Use the protocol's `li.clustered`, `li.shuffled`, `wide.clustered`, or
`wide.shuffled` case IDs. The source scale and frozen IN literals come from the
fixture manifest. Preparation independently verifies that those literals are
the first qualifying distinct partkeys and that the saved SQL matches the case.
For an untimed duplicate-IN check, add `--duplicate-in-literal`; this appends
the first selected literal and produces a distinct SQL statement with the same
expected rows.

Preparation checks the source/data/log object hashes, selected snapshot log,
and Delta Add statistics. It scans all fixture files to find actual matching
files and independently evaluates their conservative statistics to identify
candidate files. A matching file excluded by those statistics fails preparation.
These sets describe the inputs, not any engine's observed pruning decisions.

The new directory contains:

| File | Contents |
| --- | --- |
| `reference.parquet` | Exact qualifying rows in logical-key order, with typed columns and ZSTD compression |
| `reference.json` | Completion marker, source/build/protocol identities, SQL, literals, projection, expected counts, predicate-step selectivity, and candidate/matching file sets |

The completion marker is written last. A failed or interrupted preparation
leaves an incomplete directory for inspection. Choose a new destination when
retrying. Existing directories are never overwritten.

For a production no-DV/DV pair, prepare both references in one process:

```sh
cargo +1.98.1 run --locked -p xtask -- selective-read prepare-reference \
  --python ../selective-read-oracle-venv/bin/python \
  --fixtures ../production-q2-scattered-pairs \
  --workload ../production-q2-workload/workload.json \
  --case production.q2.scattered --case production.q2.scattered.dv \
  --output ../production-q2-references
```

With multiple `--case` arguments, each reference goes in `OUTPUT/CASE_ID`.
Each case retains its own oracle memory, disk and elapsed-time limits.
The process checksums each unchanged local inode once, including hard links,
and checks its device, inode, size, modification time and change time on reuse.
Writes, replacements and changes during hashing invalidate reuse. The cache
ends with the process; a standalone invocation starts fresh.

Production pairs share the original-source scan and full-column checks for
unchanged Parquet files. DV preparation still reads every physical key to
verify deletion ordinals and logical IDs, then checks every live projected
value against the independent reference. `reference.json` records fresh and
reused full-column scans in `physical_validation`. Untimed production scans use
8,192-row batches within the existing 16 GiB preparation limit; native reader
batch sizes are unchanged. Reader correctness gates
use the same checksum helper and still compare every exported value.

## Export and check a reader result

Each reader exports an **untimed Arrow IPC stream** with the case's ordered
projection and an identity JSON file. Empty results still include their schema.
Preserve the original logical types, values, nulls, and duplicates. Emit batches
of at most 8,192 rows, close both files, and keep them immutable during checking.
Dictionary, ordinary, large, and view strings are equivalent representations;
Decimal precision/scale and integer widths remain part of the contract.

The identity file has these fields:

| Fields | Required values |
| --- | --- |
| `comparison_revision`, `fixture_manifest_sha256`, `case_id`, `snapshot_version`, `canonical_sql` | The exact inputs used for this invocation; must match the prepared reference |
| `workload_contract` | For revision 3 and later, the explicit workload definition, including queries and translations |
| `reader_id` | The reader selected by the comparison revision; revision 6 uses Spark in place of Daft |
| `reader_artifact` | Executable, dependency lock, runtime and bundled runtime file identities |
| `reader_config` | The invocation's resolved reader configuration |
| `native_expression` | The executed native expression description for Polars; null for a reader executing canonical SQL directly |
| `reader_build_sha256` | Identifies the saved build record; compatibility compares its reader artifact and runtime fields |
| `result_sha256` | SHA-256 of the completed IPC stream file |

SQL, configuration and native expressions are compared directly. File checksums
use 64 lowercase hexadecimal characters. The
[reader adapters](selective-read-runners.md) produce these artifacts and preserve
the underlying build/config records for all five readers.
The checker verifies their binding to the reference and result file; the runner
is responsible for recording its actual build and invocation rather than
copying labels from a reference it did not execute.

```sh
cargo +1.98.1 run --locked -p xtask -- selective-read check-export \
  --python ../selective-read-oracle-venv/bin/python \
  --fixtures ../selective-read-smoke \
  --reference ../selective-read-reference-wide \
  --result ../reader-result.arrow \
  --identity ../reader-result.json > ../reader-validation.json
```

Success emits JSON with `status: passed` and exits zero. A validation error
emits `status: validation_failed`, an error reason, and exits nonzero. Argument
usage errors are reported by the CLI before validation starts. Store both
stdout and stderr when running the checker from a campaign.

The checker compares exact row contents and multiplicity, independently of
output order. It checks that sorted keys strictly increase, including across
batch boundaries, and rejects duplicated keys. An unlimited query needs
the complete expected set; unordered `LIMIT 100` needs exactly
`min(100, qualifying_rows)` distinct valid rows with complete matching values.
Two readers may return different valid subsets. Field nullability declarations
are recorded separately, while actual null values must agree.

Keep export, integrity checks and sorting outside query timing. Correctness
binds the explicit workload definition, query, reader artifact and settings.
File checksums detect changed data and exports. Values are compared directly, including nulls and duplicate keys.


## Resource use and checks

Reference rows arrive from the independently checked source in logical-key
order. Rust writes them in 131,072-row Parquet groups with ZSTD compression.
Integer, date and Decimal columns retain their types. The checker reads and
normalizes at most 8,192 rows per batch; equivalent string representations
become ordinary UTF-8 after their logical types are checked.

Rust normalizes and sorts actual Arrow rows by their logical keys, using a
512 MiB working-memory target and spill files beside the reference. Merge passes
open at most eight runs at once. The checker compares complete typed values and
checks key order, uniqueness, counts and LIMIT membership. It reads references
through PyArrow. Temporary runs are removed after success or a handled failure;
a process killed by its deadline can leave partial files for inspection.


Allow disk for the reference, actual result exports and sort spill while preparing
or checking. Wide full scans can require substantial space; point the reference
directory at the data filesystem. Disk exhaustion fails the operation and
cannot produce a successful completion marker. Reuse one reference for all
five readers, then retire it when that case's validation is complete.

Run the small boundary and corruption checks:

```sh
SELECTIVE_READ_ORACLE_PYTHON=../selective-read-oracle-venv/bin/python \
SELECTIVE_READ_FIXTURES=../selective-read-smoke \
  cargo +1.98.1 test --locked -p xtask --bin xtask selective_read::oracle \
  -- --include-ignored --test-threads=1
```

The tests include all 30 cases on boundary fixtures and public smoke fixtures.
CI runs them after generating the smoke dataset. The checks cover paired layouts,
exact Decimal/date boundaries, changed values at equal row counts, wrong types,
null changes, duplicated/missing rows, duplicate IN literals, wrong snapshots,
stale artifacts, valid alternative LIMIT subsets, and sort-spill quota rejection.
No reader speed threshold
is part of these checks.

## Large-workload references

Pass `--workload PATH/workload.json` to prepare a revision 3 reference. The
[large query guide](selective-read-matrix.md#large-workload-candidates) describes
the 18 case bindings and two native reuse profiles. The reference records the
explicit workload contract and the workload-manifest checksum. Large-profile
fixtures require this explicit workload; omitting it cannot create a revision 2
result. Existing revision 2 inputs keep their original cases.

The workload declares finite oracle disk and elapsed-time ceilings. Preparation
and checking enforce a 16 GiB virtual-address-space ceiling and a native process
alarm, and check free disk before scanning. The reference writer checks its byte
limit before each write. The Rust sorter limits the combined size of all live
spill files, including intermediate merge output. The process address-space
ceiling also covers in-memory buffers and the independent decoder.
Reader validation exports have a per-file
limit inherited by the reader process; ten exports share the allowance in a
reuse session. Exhaustion is a failed operation, not an unsupported reader.
After reserving 8 MiB for metadata, one quarter goes to the reference Parquet,
one quarter to temporary sort files, and half to all exports in the
current reader session. The checker counts sibling query exports together.

The reference scans full source and fixture rows. It independently derives the
scale's IN literals, compares exact values and multiplicity, and checks DV
logical keys against physical ordinals. Each large date30 DV case must remove
at least one qualifying row and retain a qualifying survivor. Reference metadata
includes physical/live/qualifying counts and projected logical bytes, defined
as non-null fixed-width values plus actual UTF-8 string bytes. This excludes
null bitmaps, offsets, IPC framing, and compressed storage overhead.

Prepare one case, validate the five readers and required reuse mode, retain its
certificates and manifests, then retire reproducible exports/references before the
next case. The limits cover one staged case; they do not authorize keeping all
18 references and every reader export at once. Hashing, export, and oracle work
remain outside performance query clocks.

## Reference storage amendment

References retain artifact format `selective-read-reference-v2`, with compressed
Parquet storage. New metadata records `oracle_implementation: rust-v1` and
`sort_backend: rust-external-merge-v1`. Existing PyArrow/DuckDB reference metadata
is still accepted when its workload contract and exact values pass validation.
The comparison revisions, frozen SQL and independent predicate/DV rules keep
their existing meanings.

The base protocol and large-workload design documents retain their original
bytes and hashes. Their SQLite storage references describe the previous
implementation. Generator preflights now reserve one full-output Parquet
reference and one sort-spill allowance, each using the existing table-byte
coefficient: 768 bytes per wide source row or 256 per original source row.
These are conservative planning coefficients, not measured compressed sizes.
They still assume unfiltered output; selective cases can use less.

Oracle source, pinned requirements, package versions and storage settings are
recorded in each new reference. Old SQLite references are unsupported; Parquet
references remain usable when their explicit input contracts and value checks
pass. Historical capacity records retain their original estimates and do
not establish the new checker's disk requirements.
