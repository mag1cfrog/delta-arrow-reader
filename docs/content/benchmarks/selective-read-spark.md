# Run the single-machine Spark Delta pilot

The optional Spark adapter reads native Delta snapshots with Spark 4.1.1 and
Delta Lake 4.3.1. It is a feasibility pilot for replacing Daft in the main
comparison. Historical comparison revisions 2-5 retain their original reader
rosters. The adapter rejects timing requests and campaign membership until the
new roster contract is available.

## Prepare the reader

Use CPython 3.14.6 on Linux x86-64, `uv`, and the Temurin JDK with runtime version
`21.0.8+9-LTS`. Preparation copies the supplied JDK and records its file hashes.
The Spark source distribution, Python dependencies, Delta JARs and matching
Hadoop 3.4.2/AWS SDK dependencies have complete artifact checksums in `lock.json`.
The Python Delta package is unnecessary; the JVM loads the native Delta connector.
The reader uses path-based tables, without Unity Catalog.

```sh
python3 -B benches/selective_read/runners/spark/prepare.py \
  --java-home /path/to/temurin-21.0.8 \
  --output /path/to/build-spark
```

Keep `artifacts/` and the complete build. To prepare another build without
downloads, add `--artifacts /path/to/build-spark/artifacts`. Each output directory
must be new. Runtime checks verify the installed Python files, Spark source tree,
JDK and native JARs before table access. See the official
[Spark/Delta compatibility table](https://docs.delta.io/releases/).

## Check Delta and deletion vectors

Use the pinned oracle environment and a prepared smoke fixture:

```sh
"$BENCH_PYTHON" -B benches/selective_read/runners/spark/check.py \
  --binary /path/to/build-spark/selective-read-spark \
  --fixtures /path/to/smoke-fixtures \
  --output /path/to/spark-capability-check
```

The check reuses the saved Spark-written external-writer corpus. It verifies every
projected value for the 12-row original snapshot, the feature-enabled snapshot,
and the nine live rows after real deletion vectors. Deleted-only and mixed
live/deleted predicates run in both open and reuse modes. Missing versions and
DV files must fail; an unknown reader feature must report unsupported. Invalid
requests, SQL outside the canonical scan shape and formal campaign requests are
rejected. These bounded checks do not establish workload performance.

## Run an existing production case

Use a verified selected-snapshot upload and an independent reference:

```sh
"$BENCH_PYTHON" -B benches/selective_read/runners/spark/pilot.py \
  --binary /path/to/build-spark/selective-read-spark \
  --state /path/to/minio-state \
  --fixtures /path/to/production-pair \
  --upload /path/to/selected-snapshot-upload.json \
  --workload /path/to/workload.json \
  --case production.q2.scattered \
  --execution reuse --purpose validation \
  --reference /path/to/reference-q2-scattered \
  --output /path/to/spark-q2-validation
```

The command reuses the request builder and
[storage observer](selective-read-storage.md). Validation exports all projected
values for the existing independent oracle. The pilot process accepts Spark's
export identity without changing the historical oracle source or formal reader
roster. Separate `io` and `diagnostic` invocations record pilot clocks, network
traffic and the native plan; they do not need `--reference`. Do not change a
historical workload or its references to add Spark to its formal schedule.

Spark runs as `local[8]` with a 4 GiB JVM heap. The existing launcher must enforce
the same eight assigned logical CPUs, 8 GiB total process-tree limit and zero swap
as the other readers. Use the same proxy endpoint and network profile. Spark
changes the native URI scheme from `s3` to `s3a` while retaining the original URI
in the request identity. Credentials come from the existing AWS environment and
are omitted from settings and records.

JVM/Spark session startup is recorded separately as `startup_ns`. Open query time
includes reading the selected Delta snapshot and consuming the complete result.
Reuse initialization opens one versioned native DataFrame; each subsequent query
is planned again against that source. The reader does not call `cache`, `persist`,
or substitute a row count for the projected query.

The query calls the public
[DataFrame.toArrow API](https://spark.apache.org/docs/4.1.1/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.toArrow.html).
It collects the complete Arrow result, so there is no streaming first-batch clock.
The current selective queries return small outputs; a broad-result comparison
would need to account for the API's driver-memory limit. IPC export and plan
capture happen after the pilot query clock. `pilot_completion_ns` is feasibility
evidence, not a formal sample, and `publication_ready` remains false.
