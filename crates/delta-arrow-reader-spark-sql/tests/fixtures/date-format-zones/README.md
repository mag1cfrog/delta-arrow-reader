# DATE_FORMAT session zones

`spark.jsonl` records 122 Spark 4.2.0 observations at revision
`32f7299601108917fb01920a54e084595b7b3bf8` for #1014. The source-only
`requests.json` defines SQL, session settings and registered DATE/LTZ/NTZ/STRING
inputs. The maintained capture driver uses internal Spark rows and retains source
hashes, runtime identity, schemas, input readbacks and its owned JVM cleanup receipt.

The 104 exact cases cover both ANSI modes in UTC and America/Los_Angeles, plus
ordinary zone-free `Z`/`PST` alias controls and a fixed `+05:30` session. They exercise
literals, columns, NULLs, duplicate temporal values, scalar and per-row formats, winter/summer,
DST gaps and overlaps, short `z` names and `O`/`X` offsets. DATE and NTZ formatting
interprets local values in the session zone; ordinary NTZ CAST to STRING keeps its
stored wall time. STRING, TO_CHAR and FROM_UNIXTIME controls retain their existing
paths. Alias controls preserve zone-free behavior; the [session alias
references](../session-zone-aliases/README.md) add normalization, gap and fixed-prefix
name controls.

The other 18 observations record display-name differences under #1236. Long
`zzzz` names retain region IDs instead of Java's localized names. For modern
Asia/Kathmandu values, short `z` falls back to `+05:45` instead of Spark's `NPT`.
These are separate fallback checks, not exact Spark comparisons. Historical
Los Angeles short names in the original temporal fixture likewise retain TZDB
`LMT` instead of Java's `PST`. No localized-name table is added.

`temporal_contracts.rs` binds every ID, SQL statement, ANSI mode, zone, ordering
flag and scope to the source requests. At batch sizes 1 and 32 it compares 208
exact row/type contexts and 36 separate fallback contexts. Results preserve
multiplicity and validate returned Arrow batches; metadata, nullable flags and
error stages are outside these new checks. The unchanged 6,020-record temporal
fixture also runs at both batch sizes, retaining only its remaining documented
differences.

From the repository root:

```sh
cargo +1.96.0 test --locked \
    --manifest-path crates/delta-arrow-reader-spark-sql/Cargo.toml \
    --test temporal_contracts
```

Recapture from the Spark SQL crate root with pinned Python 3.12 / PySpark 4.2.0:

```sh
JAVA_HOME=<JDK 21> SPARK_LOCAL_IP=127.0.0.1 TZ=UTC \
    python tests/fixtures/date-format-zones/capture_spark.py \
    /tmp/fresh-date-format-zones-spark.jsonl
```
