# Verify or repeat the selective-read benchmark

The [evidence release](https://github.com/mag1cfrog/delta-arrow-reader/releases/tag/selective-read-benchmarks-2026-10-07)
contains the records behind the published 416-column and 90-column comparisons:
eight cases, five readers, two execution profiles, and 400 timed invocations.
It includes the original harness sources, definitions, build records and locks,
fixture metadata, reference results, complete reader exports, correctness
certificates, plans, request captures, raw observations, and reports.

Bulk Delta/source tables, installed engines, and MinIO replicas are excluded.
The queries use TPC-H-derived SF10 data; they are custom scans, not TPC-H queries
or a TPC-H score. Historical `q2` and `q4` IDs mean the 416-column and 90-column
tables. Their bytes and hashes remain unchanged in the archive.

## Verify the published results

Use Linux x86-64 with `gh`, `uv`, `bubblewrap` (`bwrap`), GNU coreutils, and tar.
The replay uses CPython 3.14.6 with the original hash-pinned PyArrow and DuckDB
wheels. It does not start readers, regenerate full tables, or measure performance.

Download into a new directory:

```sh
mkdir selective-read-download
cd selective-read-download
gh release download selective-read-benchmarks-2026-10-07 \
  --repo mag1cfrog/delta-arrow-reader
sha256sum --check SHA256SUMS
tar -xzf selective-read-evidence-2026-10-07.tar.gz

BUNDLE="$PWD/selective-read-evidence-2026-10-07"
HARNESS="$BUNDLE/original/selective-read-formal-323-rerun/staging-checksum-study/execution-source"
uv venv --python 3.14.6 ../selective-read-audit-venv
uv pip sync --python ../selective-read-audit-venv/bin/python --require-hashes \
  "$HARNESS/benches/selective_read/oracle-requirements.txt"
../selective-read-audit-venv/bin/python -B "$BUNDLE/audit.py" \
  "$BUNDLE" "$PWD/replayed"
```

The output directory must be new. `replayed/receipt.json` records the outcome.
The audit checks every archived file against its checksum, then:

1. Runs the unchanged report auditor against all eight campaigns.
2. Rechecks all 120 retained query exports against the independent references,
   including values, nulls, row membership, and counts.
3. Runs the original exporter and requires byte-identical report JSON, all three
   CSVs, and provenance JSON.

Bubblewrap mounts the extracted records at their original paths inside a private
Linux namespace. The original host benchmark directories are hidden, the archive
is read-only, and networking is disabled during replay. No original paths need
to exist on your machine. Hosts that disable unprivileged user namespaces must
enable bubblewrap before running this check.

`package.json` describes the retained source trees and exclusions. Eight transient
network configuration files were restored from the copies embedded in their
campaign records; every restored file matches its original SHA-256. The package
also retains the fixture preparation sources, manifests, write requests, and
protocol-repair records.

The archived `publication_ready: false` and old publication-status prose are
historical records. They are preserved, not rewritten. The release's
`verification.json` records delivery verification separately.

## What was exercised for this release

The archive replay was run from a different extraction directory, using a fresh
Python environment and no access to the original benchmark directories. A
corrupted-file check must fail before replay starts.

A separate bounded check generated fresh SF0.01 fixtures and checked all five
native readers in open and two-query reuse modes, against both a source-derived
reference and the existing Delta deletion-vector corpus. The package includes
its script, commands, logs, and receipt under `verification/`. It reused the
hash-verified reader binaries from the recorded clean source commit; it did not
rebuild the engines. These are functional checks, not replacement timing samples.

The existing SF10 fixture validation and native correctness gates are retained.
This release does not claim a second complete SF10 generation or performance run.

## Run a new campaign

A new measurement run needs Linux x86-64, Rust 1.98.1, CPython 3.14.6, a C/C++
toolchain, `uv`, Go 1.24.7, `curl` with AWS signing, and a systemd user manager
with cgroup v2. The storage launcher checks CPU topology and applies the declared
limits. The recorded readers used eight logical CPUs on four physical cores
with SMT and an 8 GiB process-tree cap. MinIO, request observation, and the proxy
used separate cores. The final layouts use a 512 MiB file target, 131,072-row
groups, and a 20,000-row page limit with a 1 MiB byte target and 1,024-row batches.

Use two clean checkouts. The first pins the reader adapters; the second pins the
final execution harness. Keep every output outside these checkouts:

```sh
gh repo clone mag1cfrog/delta-arrow-reader readers
git -C readers checkout --detach eb1f58b4be8dc0d8ec3bee1615f1777a4c41bd9f
gh repo clone mag1cfrog/delta-arrow-reader harness
git -C harness checkout --detach 09b262a158d185adc4cd31dc58c6957fbf2d9f57
mkdir run
RUN_ROOT="$PWD/run"
```

From `readers`, prepare `build-dar`, `build-delta-rs`, `build-duckdb`,
`build-polars`, and `build-spark` under `RUN_ROOT`, using
[the reader build commands](../../docs/content/benchmarks/selective-read-runners.md)
and [the Spark preparation command](../../docs/content/benchmarks/selective-read-spark.md).
The pinned roster is DAR 0.6.1, delta-rs 1.6.6, DuckDB 1.5.5, Polars 1.44.2,
and Spark 4.1.1 with Delta Lake 4.3.1. Do not prepare the historical Daft adapter.
Keep the build records beside the executables. New machines/builds retain their
own identities; never substitute them into the published archive.

From `harness`, create the pinned oracle environment as above. Set `PYTHON` to
its absolute interpreter path. Build the fixture generator and create the source:

```sh
CARGO_TARGET_DIR="$RUN_ROOT/target" RUSTFLAGS='-C target-cpu=x86-64' \
  cargo +1.98.1 build --release --locked -j8 \
  --manifest-path benches/selective_read/Cargo.toml
WRITER="$RUN_ROOT/target/release/selective-read-fixtures"
"$WRITER" --profile large --scale-factor 10 --fixture source \
  --disk-limit-mib 196608 --elapsed-limit-seconds 1800 \
  --output "$RUN_ROOT/source"
"$PYTHON" -B benches/selective_read/production_shapes.py \
  --source "$RUN_ROOT/source" --file-target-mib 512 --page-rows 20000 \
  --page-bytes 1048576 --write-batch-rows 1024 --output "$RUN_ROOT/plan"
```

Follow [probe, generation, and pairing](../../docs/content/benchmarks/selective-read-production-fixtures.md)
for `q2` and `q4`, each with `localized` and `scattered` layouts. Use the source,
plan, writer, and output paths under `RUN_ROOT`. For one-layout staging, pass
`--layout` to both the probe and pairing commands. Keep the probe until its full
layout has been generated. The source, current layout, reference/export files,
and MinIO copy must fit the 192 GiB data allowance together; builds are separate.
The original formal execution plans in the archive record phase accounting.

Each complete pair supplies a no-DV and real-DV case. For example, with a completed
416-column localized pair at `RUN_ROOT/pair-q2-localized`:

```sh
PAIR="$RUN_ROOT/pair-q2-localized"
"$PYTHON" -B benches/selective_read/production_workloads.py \
  --fixtures "$PAIR" --binary "$RUN_ROOT/build-polars/selective-read-polars" \
  --comparison-revision 6 --stage formal --output "$RUN_ROOT/definition-q2-localized"
WORKLOAD="$RUN_ROOT/definition-q2-localized/workload.json"
CASE=production.q2.localized
"$PYTHON" -B benches/selective_read/oracle.py prepare --fixtures "$PAIR" \
  --case "$CASE" --workload "$WORKLOAD" --output "$RUN_ROOT/reference-$CASE"

"$PYTHON" -B benches/selective_read/storage.py prepare --output "$RUN_ROOT/build-minio"
"$PYTHON" -B benches/selective_read/storage.py start \
  --build "$RUN_ROOT/build-minio" --state "$RUN_ROOT/storage-$CASE" --port 19090
"$PYTHON" -B benches/selective_read/network.py start \
  --state "$RUN_ROOT/storage-$CASE" --output "$RUN_ROOT/network-$CASE" --port 19092 \
  --latency-ms 200 --jitter-ms 20 --mbps 150 --seed 0

"$PYTHON" -B benches/selective_read/campaign.py \
  --state "$RUN_ROOT/storage-$CASE" --fixtures "$PAIR" --workload "$WORKLOAD" \
  --case "$CASE" --session "reuse.$CASE" --reference "$RUN_ROOT/reference-$CASE" \
  --binary "$RUN_ROOT/build-dar/selective-read-dar" \
  --binary "$RUN_ROOT/build-delta-rs/selective-read-delta-rs" \
  --binary "$RUN_ROOT/build-duckdb/selective-read-duckdb" \
  --binary "$RUN_ROOT/build-polars/selective-read-polars" \
  --binary "$RUN_ROOT/build-spark/selective-read-spark" \
  --upload-table "$CASE" --upload "$RUN_ROOT/upload-$CASE.json" \
  --combined-diagnostics --gate-warmup --output "$RUN_ROOT/campaign-$CASE"
"$PYTHON" -B benches/selective_read/network.py stop --state "$RUN_ROOT/storage-$CASE"
"$PYTHON" -B benches/selective_read/storage.py stop --state "$RUN_ROOT/storage-$CASE"
```

Prepare MinIO once; repeat the reference, service, campaign, and stop commands
for the `.dv` case and the other three pairs, each with new destinations. Remove
only completed phases' bulk replicas/data when needed for capacity, retaining
manifests, references, logs, sources, and results. Never delete a shared original
source or an unfinished case's inputs. Each formal case schedules five independent
invocations per reader and profile, plus untimed validation and diagnostics.

The example applies the final preparation method consistently to a new run.
The first two archived 416-column scattered campaigns used separate warmup and
diagnostics. Their original commands and sources are preserved in the archive;
do not relabel or pool those samples with the example's new measurements.

After all four pairs have been prepared, make the global definition with
`production_workloads.py --comparison-revision 6 --stage formal`, repeating
`--fixtures` for their four retained directories. Definitions and reporting need
their manifests; reclaimed bulk tables are not needed for these steps. Then run
`production_report.py --definition GLOBAL/workload.json --output NEW_REPORT`,
repeating `--campaign` for all eight campaign directories. Require complete
coverage and inspect every status. Keep any failed attempts separately.

These commands describe a new full measurement run. The release's verification
receipt distinguishes the commands exercised during delivery from the retained
SF10 execution evidence; the full rerun recipe was not executed again for release.
