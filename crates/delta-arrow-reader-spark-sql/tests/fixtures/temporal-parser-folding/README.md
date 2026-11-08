# Temporal parser constant folding

`temporal-fold-1123-spark.jsonl` records 44 independent Spark observations for
strict constant DATE, TIMESTAMP and TIMESTAMP_NTZ parsing in both ANSI modes.
The cases include direct and formatted calls, TRY_CAST, unreachable CASE and
COALESCE arguments, LIMIT 0, unused projections, and explicit temporal CAST
defaults in LAG and LEAD.

Use these records to check failure timing together with available Fields and
values. Spark returned 33 successful observations and 11 errors. Every error
occurred while requesting the optimized plan, recorded as `optimized_schema`.
The CASE, COALESCE, LIMIT 0 and unused-projection guards succeeded.

The capture uses Spark 4.2.0, revision
`32f7299601108917fb01920a54e084595b7b3bf8`, Python 3.12 and Java 21 in UTC.
Its header records runtime identity, settings, registered provider schemas and
rows, and source hashes. Each observation retains public, analyzed, optimized,
`executed_before_collect` and executed schemas where available, stage availability,
plans, exact rows or the original error condition, cause chain and failing
phase.

| Source | SHA256 |
| --- | --- |
| Spark reference | `5aef5b40dc2ad055c77b2f05bf643a959b075eacb217e19933aa2b7be7ecd3fe` |
| [Producer](../capture_temporal_fold_1123_spark.py) | `d0c78647c61f9313761f5451ab5e3d8f9bc4261ff70c3ab6e4186f86570eaa67` |
| [Request](../temporal-fold-1123-cases.json) | `156bb4ba1c0cf99477960b62173a48301f83102aad9400d7c5e35a6a110f17db` |
| [Shared encoder](../capture_floating_group_keys_spark.py) | `e8755f789b2ef55e25817bb864a05b99194d513bd49db11e418a80b371100943` |

The producer reads SQL and typed provider inputs from the adjacent request.
It uses `capture_spark_requests.py` for the five schema stages, errors and owned
JVM shutdown. The shared encoder writes typed TIMESTAMP output as UTC RFC3339
text; TIMESTAMP_NTZ keeps its wall-clock datetime text. Provider input encoding
and other output types retain their existing formats.

Recapture from the crate root with the pinned Python/PySpark build and Java 21.
Run the codec check and source/protocol check in separate fresh Python
processes. The output path must be new; the capture also writes a lifecycle
file beside it.

```sh
TZ=UTC PYTHONDONTWRITEBYTECODE=1 python3.12 tests/fixtures/check_capture_timestamp_encoding.py
python3.12 tests/fixtures/capture_temporal_fold_1123_spark.py --self-check
temporal_fold_capture_dir="$(mktemp -d)"
TZ=UTC SPARK_LOCAL_IP=127.0.0.1 PYTHONDONTWRITEBYTECODE=1 \
  python3.12 tests/fixtures/capture_temporal_fold_1123_spark.py \
  "$temporal_fold_capture_dir/temporal-fold-1123-spark.jsonl"
```

This is a UTC folding reference. The wider
[temporal contracts](../temporal-contracts/README.md) and
[timestamp separator references](../timestamp-separators/README.md) retain
registered-column, parser and timezone/DST coverage. Review those contracts
when changing temporal folding. The [integration test](../../temporal_parser_folding.rs) compares these records
with DAR. Its [retained differences](temporal-fold-1123-retained.json) preserve
separately tracked parser diagnostics and formatted-call declarations. The raw
Spark reference retains its original observations.
