---
title: Compare predicates and projection width
description: Run all 30 public query cases across five pinned readers and retain correctness, selectivity, timing and I/O evidence.
---

# Compare predicates and projection width

The public query matrix compares compound predicates and projection width on
the same generated tables. It contains 30 cases, each accounting for
delta-arrow-reader, delta-rs, DuckDB, Polars and Daft. All tables in this slice
have no deletion vectors. DV and file-organization cases remain part of the
[full comparison protocol](selective-read-protocol.md).

These are TPC-H-derived scans. The Q6-derived case keeps the date, Decimal and
quantity predicates but projects rows without the revenue aggregation. The
wide derivative adds 64 deterministic integer payload columns. Neither is an
official TPC-H query suite or score.

## Cases and scales

Each row below runs on both clustered and shuffled layouts:

| Table | Query shapes | Output |
| --- | --- | --- |
| Original lineitem | `all-keys`, `all-full`, `empty` | Two keys, all 16 columns, or an empty 16-column result |
| Original lineitem | `date7-full`, `date7-keys`, `date7-limit` | Seven-day range with 16 columns, two keys, or at most 100 rows |
| Original lineitem | `q6-scan`, `eq2-in1`, `eq2-in20` | Q6-derived scan or date/shipmode equalities plus partkey membership |
| Wide lineitem | `eq1`, `eq2`, `eq2-in1`, `eq2-in20` | Date equality, then shipmode equality, then 1 or 20 membership values; 69 columns |
| Wide lineitem | `eq2-in20-keys`, `all-wide` | Same compound predicate with two keys, or an unfiltered 69-column scan |

The narrow and wide compound queries use the same physical wide table. Their
only difference is projection: the three predicate columns stay hidden in the
two-key output. The oracle checks the ordered output schema and every value.
An unordered LIMIT may return any qualifying rows up to its limit, with valid
values and multiplicities. Empty output must still carry the correct schema.

Use `smoke` (SF0.01) to check the machinery, `development` (SF1) for validation,
and `report` for SF10 original tables plus SF1 wide tables. Smoke has only five
qualifying distinct partkeys, so its `in20` cases use all five. SF1 and SF10
use 20. Smoke's one-file tables cannot demonstrate pruning across many files.

[`query-matrix.json`](https://github.com/mag1cfrog/delta-arrow-reader/blob/main/benches/selective_read/query-matrix.json)
checks in every canonical SQL statement, the three scales' frozen membership
values, generator provenance and Polars/Daft expression hashes.
`benches/selective_read/native-expressions.jsonl` contains the full native
expression identities, one JSON record per distinct reader/expression hash. The native
translations are constructed by the pinned adapters from these SQL statements
as filter, projection, then optional limit. Each real validation run also
preserves its native expressions. A campaign rejects a successful observation whose executed
translation hash differs from the checked-in query.

## Prepare and run all 150 entries

Follow the [fixture](selective-read-fixtures.md),
[reader](selective-read-runners.md) and [storage](selective-read-storage.md)
guides first. Use the oracle environment and new output directories. Prepare
all references before starting a timed campaign:

```sh
../selective-read-oracle-venv/bin/python -B benches/selective_read/matrix.py prepare \
  --fixtures ../selective-read-smoke --output ../matrix-smoke
```

Preparation independently scans the source and fixture for every case. It saves
the exact reference result, predicate-step counts, conservative candidate files
and actual matching files. To reuse references produced by the current oracle,
pass `--references PATH`, containing `CASE_ID/reference.json` directories.
Changed SQL, literals, source counts, reference hashes or protocol identities
fail preparation. A failed case remains in `matrix.json` with its reason.

Start MinIO and upload these same fixtures using the storage guide, then run:

```sh
../selective-read-oracle-venv/bin/python -B benches/selective_read/campaign.py \
  --state ../selective-read-storage --fixtures ../selective-read-smoke \
  --upload ../selective-read-upload.json --matrix ../matrix-smoke/matrix.json \
  --binary ../selective-read-dar-build/selective-read-dar \
  --binary ../selective-read-delta-rs-build/selective-read-delta-rs \
  --binary ../selective-read-duckdb-build/selective-read-duckdb \
  --binary ../selective-read-polars-build/selective-read-polars \
  --binary ../selective-read-daft-build/selective-read-daft \
  --output ../campaign-matrix-smoke
../selective-read-oracle-venv/bin/python -B benches/selective_read/matrix.py report \
  --campaign ../campaign-matrix-smoke --output ../matrix-smoke-report
```

`--matrix` selects all 30 isolated cases and their references. It cannot be
combined with case, reference or session overrides. Reuse sessions are separate
campaign jobs described in the [campaign guide](selective-read-campaign.md).
Every reader must pass an independent correctness gate before its case can
enter the frozen schedule. Five runnable readers produce 150 gates, 300 warmups,
1,500 timed samples, 150 plan runs, 300 diagnostic warmups and 600 paired
traced/untraced observations. All invocations run sequentially.

The same commands accept development or report fixtures. Finish compilation,
fixture generation and reference preparation before timing. Stop the owned
MinIO service when finished. All commands here are manual; they add no CI work.

## Read the report

`matrix-report.json` and `matrix-report.csv` retain 150 entries, including
preparation failures, unsupported readers, failed samples, ties and losses.
The exporter verifies the frozen campaign inputs and recomputes summary values
from raw observations before writing the report. It never substitutes a failed
sample or derives a speedup from an incomplete timing series.

Each entry includes predicate columns, projected column count, layout, actual
qualifying/output rows and selectivity. JSON retains the ordered predicate-step
counts and exact oracle candidate/matching file sets. These independently
computed sets describe the data. They are not the engine's planned file count.
Native plans are linked as artifacts; a normalized planned-file count is null
with an explanation because the current adapters do not export that counter.

Timing distributions and comparator/DAR ratios come from the untraced campaign.
Separate diagnostics record distinct touched Parquet objects, response-body
bytes accepted by MinIO, requests and observer overhead. The CSV keeps each
successful diagnostic's totals as arrays; JSON also retains every diagnostic's
run ID and failure status. A touched file can mean only a footer request.
Consult the [storage guide](selective-read-storage.md) before interpreting bytes
or counting a touched object as a full file read.

This 30-case report is one part of the protocol's 46-case publication inventory.
It does not reproduce private historical data or establish the old speedups.

## Check or regenerate the catalog

Run the bounded matrix check against a prepared smoke matrix:

```sh
../selective-read-oracle-venv/bin/python -B benches/selective_read/check_matrix.py \
  --fixtures ../selective-read-smoke --matrix ../matrix-smoke/matrix.json
```

It checks all SQL/literal scales, paired projections, translation binding,
changed metadata rejection and a synthetic 150-entry report containing failed
and unsupported cases. Real engine correctness still requires the campaign.

To regenerate the catalog deliberately, supply all three fixture profiles and
the pinned Polars and Daft executables:

```sh
../selective-read-oracle-venv/bin/python -B benches/selective_read/matrix.py freeze \
  --fixtures ../selective-read-smoke --fixtures ../selective-read-development \
  --fixtures ../selective-read-report \
  --binary ../selective-read-polars-build/selective-read-polars \
  --binary ../selective-read-daft-build/selective-read-daft \
  --output ../matrix-catalog
```

Review the generated `query-matrix.json` and `native-expressions.jsonl` before
replacing the checked-in copies.
The command checks generator SQL against the independent oracle and constructs
expressions in each adapter's own pinned environment. Keep generated data,
references and campaign outputs outside Git.
