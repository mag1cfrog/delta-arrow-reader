# Formatted date zones

`spark.jsonl` contains 136 observations from Spark 4.2.0, revision
`32f7299601108917fb01920a54e084595b7b3bf8`, for #1015. There are 120 observations in
UTC and America/Los_Angeles across both ANSI modes, plus 16 zone-free preservation
observations in the `Z` and `PST` session aliases. `requests.json` contains only source
SQL and registered input definitions. The maintained capture driver reads DATE days from
Spark's internal rows through the existing TRY_TO_DATE capture helper.

TO_DATE and TRY_TO_DATE cover positive and negative offsets across midnight in both
directions, `Z` where UTC and Los Angeles have different dates, fractional seconds,
negative epochs and duplicate values. Existing `VV` support supplies Los Angeles and New
York region cases, including a spring gap and fall overlap. The source includes NULL
values/formats and both scalar-input/column-format and column-input/scalar-format shapes.
Literal controls include the issue's quoted `T` pattern, zone-free date-only and
time-containing formats, malformed TRY input, unformatted parsing, numeric/BINARY coercion
and existing DATE/NTZ/TIMESTAMP argument branches.

The parsed offset or region determines the instant; the session zone determines its date.
Zone-free formats keep their local date. Session aliases the reused timestamp parser
cannot interpret retain the prior date parser, preserving zone-free and NULL behavior.
Parsed-offset conversion in those alias sessions remains part of #1029. These alias
references cover zone-free preservation, not full alias timezone parity. No datetime
grammar or additional region-name policy is introduced.

The header keeps the runtime, settings, source hashes and registered schemas/inputs.
The first case in each session includes exact input readbacks. All captured schema stages
remain available; the driver stops and reaps its own JVM.

`try_to_date.rs` binds IDs, SQL, zones, ANSI modes and ordering flags to the source
definitions. It replays all 136 cases at input/output batch sizes 1 and 32: 272 contexts.
Successful results compare executed types and duplicate-preserving row multisets, without
metadata, nullable flags or error-stage matching. Existing date/coercion/encoded-input and
native SQL controls remain in that test file. `temporal_contracts.rs` replays its unchanged
6,020 raw references with the repaired offset expectations restored and other retained
differences unchanged.

From the repository root, replay with:

```sh
cargo +1.96.0 test --locked \
    --manifest-path crates/delta-arrow-reader-spark-sql/Cargo.toml \
    --test try_to_date --test temporal_contracts
```

Recapture to a fresh path from the Spark SQL crate root using pinned Python 3.12 /
PySpark 4.2.0:

```sh
JAVA_HOME=<JDK 21> SPARK_LOCAL_IP=127.0.0.1 TZ=UTC \
    python tests/fixtures/formatted-date-zones/capture_spark.py \
    /tmp/fresh-formatted-date-zones-spark.jsonl
```
