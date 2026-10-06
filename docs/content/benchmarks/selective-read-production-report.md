# Report staged Q2/Q4 campaigns

Generate one overview from formal campaigns that ran native snapshots separately.
The report keeps all eight Q2/Q4 cases, all five readers and both open/reuse
profiles visible. A completed batch can leave most of the matrix pending.

Use the pinned oracle Python environment from
[Validate public results](selective-read-oracle.md). Keep the complete frozen
formal definition, fixture manifests, references and raw campaign artifacts.
Revision 6 uses the [Spark reader roster](selective-read-spark-matrix.md).
Revision 5 retains Daft. Use each revision's frozen harness for historical inputs;
the report rejects mixtures of comparison revisions.
Reporting loads metadata; reclaimed Parquet bulk does not need to be restored
just to generate the overview.

## Generate an overview

Supply the complete eight-case definition and each campaign once:

```sh
BENCH_PYTHON=/path/to/oracle-venv/bin/python
BENCH_RESULTS=/path/to/benchmark-results
"$BENCH_PYTHON" -B benches/selective_read/production_report.py \
  --definition "$BENCH_RESULTS/global-definition/workload.json" \
  --campaign "$BENCH_RESULTS/campaigns/q2-scattered-spark-v6" \
  --campaign "$BENCH_RESULTS/campaigns/q2-scattered-dv-spark-v6" \
  --output "$BENCH_RESULTS/report"
```

The output directory must be new and its parent must exist. Repeat `--campaign`
for other completed batches. Omitting it generates a pending inventory from
the definition, with no timings. The example covers only Q2 scattered snapshots;
the other six cases remain `not_run` until their campaigns are supplied.

The command writes:

| File | Content |
| --- | --- |
| `production-report.json` | Eight case definitions, 80 reader/profile entries, campaign hashes, shared conditions and audited measurements |
| `README.md` | Physical table dimensions, case coverage and query-time overview |

A successful command means the report was generated from valid inputs. Check
`formal_coverage_complete` for full formal coverage. `publication_ready` remains
false even when coverage is complete; publication still requires the reproduction
and owner review described in the execution contract.

## Read the results

Each time shows `median [Q1, Q3]` in seconds from five independent invocations.
Q1 and Q3 are the 25th and 75th percentiles, so the bracketed range covers the
middle 50% of timings. Open time includes opening the table and consuming the
complete query result. The reuse columns show initialization plus query 1, then
query 2 on the same native source.
The JSON retains initialization and individual query clocks, IQRs and eligible
same-case speedups. Query positions are separate measurements, not extra
independent samples. Do not subtract medians to estimate overhead.

Missing entries have `not_run` status and no timing. Failed or unsupported native
entries retain their status and evidence. A recorded incomplete campaign remains
visible and cannot complete a case. A batch interrupted before producing its
summary cannot pass the existing campaign audit; keep its raw records separately.

The JSON also retains exact-gate oracle geometry and separate I/O diagnostics.
New revision 6 campaigns can use the
[combined diagnostic amendment](selective-read-combined-diagnostics.md).
The overview records each campaign's mode and amendment hash. Those invocations
remain outside the timing distributions, and DuckDB EXPLAIN remains separate.
Single-snapshot revision 6 campaigns can also declare
[exact validation as storage warmup](selective-read-gate-warmup.md).
Each campaign also retains its upload verification method and whether staging
shared the controller process. These preparation steps stay outside query clocks.
The JSON records that method per campaign and reader/profile. Compare matching
cases and preparation methods, and retain earlier standalone-warmup observations.
Physical Parquet bytes describe table layout. Response bytes describe downloaded
traffic. Geometry and traffic do not measure decoded pages or row groups. DV
snapshots share their base snapshot's Parquet data.

## Input checks

Each campaign passes the existing single-campaign audit before aggregation.
Its case must match the complete definition, including SQL, fixture identity,
native expressions, writer settings and geometry. Each successful observation
must retain the formal resource and watchdog limits.

Reader builds, CPU placement and model, kernel, cache policy and transport
conditions must agree across batches. Localhost and emulated transport campaigns
cannot be mixed in one report. Duplicate case/profile coverage is rejected;
retries and partial schedules cannot fill another campaign's samples. Pilot and
historical observations cannot enter the formal report.

Run the bounded aggregation check manually:

```sh
"$BENCH_PYTHON" -B benches/selective_read/check_production_report.py
```

The check uses simulated records and produces no performance evidence. This
reporting command adds no reader build, dependency, CI job or timing threshold.
