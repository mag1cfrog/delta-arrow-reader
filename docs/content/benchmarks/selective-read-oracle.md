---
title: Validate public selective-read results
description: Prepare independent reference results and check untimed reader output before accepting benchmark measurements.
---

# Validate public selective-read results

The oracle checks the 30 original/wide public query cases in the
[comparison protocol](selective-read-protocol.md). It reads every source row,
evaluates predicates with Python integers, dates, and exact Decimals, and derives
wide payload values from the published hash rule. PyArrow decodes Parquet and
Arrow IPC; none of the five benchmark engines computes the expected results.

Each reference also checks the selected fixture's qualifying rows against the
saved source. This detects changed values, payloads, nulls, or duplicate rows
before an engine runs. All qualifying rows are checked even for a LIMIT case.
The same checker also accepts [file-organization controls](selective-read-files.md),
[within-file controls](selective-read-within-file.md), and
[paired DV snapshots](selective-read-deletion-vectors.md). DV references apply
independently checked deletion lists and retain both physical and live row counts.

## Install and prepare a reference

Run from the repository root on Linux x86-64, using CPython 3.14.6. The
requirements file pins PyArrow 25.0.1 and its wheel hash. Use an environment
outside the checkout:

```sh
uv venv --python 3.14.6 ../selective-read-oracle-venv
uv pip sync --python ../selective-read-oracle-venv/bin/python --require-hashes \
  benches/selective_read/oracle-requirements.txt
```

[Generate public fixtures](selective-read-fixtures.md) first, then prepare one
reference per case in a new directory:

```sh
PYTHONDONTWRITEBYTECODE=1 ../selective-read-oracle-venv/bin/python \
  benches/selective_read/oracle.py prepare \
  --fixtures ../selective-read-smoke \
  --case wide.clustered.eq2-in20 \
  --output ../selective-read-reference-wide
```

Use the protocol's `li.clustered`, `li.shuffled`, `wide.clustered`, or
`wide.shuffled` case IDs. The source scale and frozen IN literals come from the
fixture manifest. Preparation independently verifies that those literals are
the first qualifying distinct partkeys and that the saved SQL matches the case.
For an untimed duplicate-IN check, add `--duplicate-in-literal`; this appends
the first selected literal and produces a distinct SQL hash with the same
expected rows.

Preparation checks the source/data/log object hashes, selected snapshot log,
and Delta Add statistics. It scans all fixture files to find actual matching
files and independently evaluates their conservative statistics to identify
candidate files. A matching file excluded by those statistics fails preparation.
These sets describe the inputs, not any engine's observed pruning decisions.

The new directory contains:

| File | Contents |
| --- | --- |
| `reference.sqlite` | Exact qualifying rows ordered on disk by their unique logical keys |
| `reference.json` | Completion marker, source/build/protocol identities, SQL, literals, projection, expected counts, predicate-step selectivity, and candidate/matching file sets |

The completion marker is written last. A failed or interrupted preparation
leaves an incomplete directory for inspection. Choose a new destination when
retrying. Existing directories are never overwritten.

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
| `comparison_revision`, `protocol_sha256`, `fixture_manifest_sha256`, `case_id`, `snapshot_version`, `canonical_sql_sha256` | The exact inputs used for this invocation; must match the prepared reference |
| `reader_id` | `delta-arrow-reader`, `delta-rs`, `duckdb`, `polars`, or `daft` |
| `reader_build_sha256` | Hash of the adapter's recorded build identity, including executable/package, dependency lock, and adapter source identities |
| `reader_config_sha256` | Hash of the invocation's resolved reader configuration |
| `native_expression_sha256` | Hash of the executed expression translation for Polars/Daft; null for a reader executing canonical SQL directly |
| `result_sha256` | SHA-256 of the completed IPC stream file |

Hash strings use 64 lowercase hexadecimal characters. The
[reader adapters](selective-read-runners.md) produce these artifacts and preserve
the underlying build/config records for all five readers.
The checker verifies their binding to the reference and result file; the runner
is responsible for recording its actual build and invocation rather than
copying labels from a reference it did not execute.

```sh
PYTHONDONTWRITEBYTECODE=1 ../selective-read-oracle-venv/bin/python \
  benches/selective_read/oracle.py check \
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
output order. Its disk index rejects duplicate keys. An unlimited query needs
the complete expected set; unordered `LIMIT 100` needs exactly
`min(100, qualifying_rows)` distinct valid rows with complete matching values.
Two readers may return different valid subsets. Field nullability declarations
are recorded separately, while actual null values must agree.

Keep all export, hashing, verification, and SQLite work outside query timing.
A changed fixture, protocol, oracle, query translation, reader build, or reader
configuration needs a fresh correctness gate. Generator protocol hashes remain
provenance; the reference separately binds the active comparison revision/hash.

## Resource use and checks

Rows are processed in batches and stored in SQLite's on-disk primary-key order.
Comparisons use full encoded values, not probabilistic result fingerprints.
Each SQLite connection has a 64 MiB page cache, and file contents are hashed in
streams. Temporary comparison databases are created beside the reference and
removed when checking finishes.

Allow disk for the reference and one complete qualifying result while preparing
or checking. Wide full scans can require substantial space; point the reference
directory at the data filesystem. Disk exhaustion fails the operation and
cannot produce a successful completion marker. Reuse one reference for all
five readers, then retire it when that case's validation is complete.

Run the small boundary and corruption checks:

```sh
PYTHONDONTWRITEBYTECODE=1 ../selective-read-oracle-venv/bin/python \
  -m unittest discover -s benches/selective_read -p test_oracle.py -v
```

To also validate all 30 cases against actual public smoke fixtures, set
`SELECTIVE_READ_FIXTURES` to their generated directory when running that command.
CI does this after its generator smoke check. The checks cover paired layouts,
exact Decimal/date boundaries, changed values at equal row counts, wrong types,
null changes, duplicated/missing rows, duplicate IN literals, wrong snapshots,
stale artifacts, and valid alternative LIMIT subsets. No reader speed threshold
is part of these checks.

## Large-workload references

Pass `--workload PATH/workload.json` to prepare a revision 3 reference. The
[large query guide](selective-read-matrix.md#large-workload-candidates) describes
the 18 case bindings and two native reuse profiles. Revision 3 binds the base
protocol, amendment, and concrete workload-manifest hashes. Large-profile
fixtures require this explicit workload; omitting it cannot create a revision 2
result. Existing revision 2 inputs keep their original cases.

The workload declares finite oracle disk and elapsed-time ceilings. Preparation
and checking enforce a 16 GiB virtual-address-space ceiling and a native process
alarm, and check free disk before scanning. SQLite page quotas bound the reference
and temporary comparison indexes. Reader validation exports have a per-file
limit inherited by the reader process; ten exports share the allowance in a
reuse session. Exhaustion is a failed operation, not an unsupported reader.
After reserving 8 MiB for metadata, one quarter goes to the reference index,
one quarter to its temporary comparison index, and half to all exports in the
current reader session. The checker counts sibling query exports together.

The reference scans full source and fixture rows. It independently derives the
scale's IN literals, compares exact values and multiplicity, and checks DV
logical keys against physical ordinals. Each large date30 DV case must remove
at least one qualifying row and retain a qualifying survivor. Reference metadata
includes physical/live/qualifying counts and projected logical bytes, defined
as non-null fixed-width values plus actual UTF-8 string bytes. This excludes
null bitmaps, offsets, IPC framing, and compressed storage overhead.

Prepare one case, validate the five readers and required reuse mode, retain its
certificates and manifests, then retire reproducible exports/indexes before the
next case. The limits cover one staged case; they do not authorize keeping all
18 references and every reader export at once. Hashing, export, and oracle work
remain outside performance query clocks.
