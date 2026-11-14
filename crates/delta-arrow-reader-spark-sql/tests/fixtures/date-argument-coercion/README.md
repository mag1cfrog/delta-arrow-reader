# Date argument coercion

`spark.jsonl` contains 296 observations from Spark 4.2.0, revision
`32f7299601108917fb01920a54e084595b7b3bf8`, for #1194. The 74 query shapes run in both
ANSI modes in UTC and America/Los_Angeles. `requests.json` contains only SQL and source
input definitions. The capture uses the maintained owned-JVM driver and reads DATE days
from Spark's internal rows through the existing TRY_TO_DATE capture helper.

Both TO_DATE and TRY_TO_DATE receive INT, BIGINT, DECIMAL, DOUBLE, FLOAT, BOOLEAN, BINARY
and untyped NULL columns, with and without a format. Literals cover the same ordinary
coercion paths; STRING, DATE, TIMESTAMP and TIMESTAMP_NTZ literals provide controls.
Source rows include NULL, duplicate values, valid dates, numeric year text and invalid
date text. Valid-row projections check successful strict TO_DATE column conversions in
ANSI mode, including numeric formats and BINARY dates. Mixed valid/invalid queries check
TO_DATE's ANSI failures and TRY_TO_DATE's NULL results.

The header retains exact runtime/settings and source hashes, plus registered schemas and
inputs. The first case in each session also records exact source values read from Spark's
internal rows. All schema stages and raw parse failures remain in the fixture. The driver
stops and reaps its own JVM.

`try_to_date.rs` binds every record's ID, SQL, ANSI mode, zone and ordering flag to its
source definition. It replays the 296 cases at input/output batch sizes 1 and 32 over two
provider partitions: 592 contexts. Successes compare executed types and duplicate-preserving
row multisets. Parse failures require a parser-specific diagnostic, without matching exact
wording, stage, metadata or nullable flags. The same test file replays the unchanged 116
[temporal references](../try-to-date/README.md), including dictionary/view/large STRING and
Date64 controls. A native DataFusion SQL control retains integer-to-DATE day-count semantics.
ARRAY analysis rejection remains outside this fixture's acceptance.

From the repository root, replay with:

```sh
cargo +1.96.0 test --locked \
    --manifest-path crates/delta-arrow-reader-spark-sql/Cargo.toml \
    --test try_to_date
```

Recapture to a fresh path from the Spark SQL crate root with the pinned Python 3.12 /
PySpark 4.2.0 reference environment:

```sh
JAVA_HOME=<JDK 21> SPARK_LOCAL_IP=127.0.0.1 TZ=UTC \
    python tests/fixtures/date-argument-coercion/capture_spark.py \
    /tmp/fresh-date-arguments-spark.jsonl
```
