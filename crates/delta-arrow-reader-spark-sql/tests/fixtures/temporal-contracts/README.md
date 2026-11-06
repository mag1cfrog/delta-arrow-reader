# Temporal formatting, parsing and type contracts

`temporal-contracts-spark.jsonl` records 6,020 observations from Spark 4.2.0,
revision `32f7299601108917fb01920a54e084595b7b3bf8`, Java 17.0.20.1 and Python
3.12.14 (#153). The header stores the build properties, capture script hash,
settings and the two registered tables. Two captures were byte-identical.

The 1,505 queries run in both ANSI modes with UTC and `America/Los_Angeles`
session zones:

- `format`: DATE_FORMAT with 118 patterns covering Spark's pattern letters at
  accepted and rejected widths, quoted text, optional sections, rejected letters
  and reserved characters. Each pattern formats DATE, TIMESTAMP and TIMESTAMP_NTZ
  literals and the registered `temporal_values` columns, which hold pre-epoch
  fractions, years 1 and 9999, Los Angeles DST transitions, a pre-1883
  local-mean-time value and NULL. Probes add NULL and per-row patterns, an invalid
  per-row pattern, generated names and the frozen experiment's two DATE_FORMAT
  queries verbatim. Each record names the kind of input formatted in each output
  column.
- `parse`: CAST and TRY_CAST to DATE, TIMESTAMP, TIMESTAMP_NTZ and
  TIMESTAMP_LTZ, TO_DATE, TRY_TO_DATE, TO_TIMESTAMP, TO_TIMESTAMP_NTZ and
  TRY_TO_TIMESTAMP of 32 strings: invalid calendar dates and times, second 60,
  fractions of 0 to 9 digits, offsets and regions, Los Angeles DST gap and overlap
  times, the TIMESTAMP range limits, empty and invalid strings and NULL, as literals
  and `parse_inputs` column values. Non-empty strings without a quote are also
  typed literals. The frozen experiment's four parsing and precision queries are
  kept verbatim.
- `formatted`: TO_DATE, TRY_TO_DATE, TO_TIMESTAMP, TO_TIMESTAMP_NTZ,
  TO_TIMESTAMP_LTZ and TRY_TO_TIMESTAMP with 24 string and pattern pairs, including
  invalid, partial and mismatched input, offsets, regions, 12-hour clocks and
  invalid patterns.
- `convert`: casts between DATE, TIMESTAMP and TIMESTAMP_NTZ, casts to STRING,
  `hour` and `unix_micros` over the registered columns and over literals at the
  same boundaries.

STRING literals quote with backslash escapes. Doubled quotes are read differently
by the staged frontend (#1009) and are outside these references.

The capture excludes `ConvertToLocalRelation`, so column expressions run during
execution rather than optimization. Rows come from Spark's internal rows: DATE
values are epoch days, TIMESTAMP values are UTC-epoch microseconds and
TIMESTAMP_NTZ values are wall-clock microseconds. Each record keeps the analyzed,
optimized and executed schemas, or the error condition, first message line and
failing stage. The registered TIMESTAMP column is UTC-annotated, like a Delta
column read by DAR.

`temporal_contracts.rs` compares every record at batch sizes 1 and 32. DAR reports
date and timestamp parse failures with its parser's message instead of Spark's
condition, so an expected `CAST_INVALID_INPUT`, `INVALID_TYPED_LITERAL` or
`CANNOT_PARSE_TIMESTAMP` must occur at the same stage with one of the parser
messages listed in the test.

Existing differences keep their Spark expectations in the fixture. The test
replaces only the owned part with the current DAR result, derived from other
Spark records where possible, and checks each class's record count. A record can
belong to several classes:

- #937: 242 constant conversions that DAR rejects fail while collecting rows
  instead of during optimization. They include formatted parsing and 29
  out-of-range strings that Spark parses (#1011).
- #943: 12 typed TIMESTAMP literals with second 60 roll over to the next minute;
  their fields are those of the same literal for a valid time.
- #1003: 24 `CAST(DATE AS TIMESTAMP_NTZ)` literals are required at analysis.
- #1010: 324 invalid patterns fail with DAR's message while collecting rows;
  `#`, `{` and `}` are formatted as text.
- #1011: 212 strings beyond the date library's range behave like `'bad'`.
- #1012: 108 results over the UTC-annotated column are computed in UTC in the Los
  Angeles session.
- #1013: 14 TIMESTAMP to TIMESTAMP_NTZ casts keep the UTC wall-clock time, which
  equals `unix_micros` of the same value.
- #1014: 96 DATE_FORMAT results print other zone names and `O` offsets, or UTC
  zone tokens for DATE and TIMESTAMP_NTZ inputs; 24 keep the hour of the
  registered TIMESTAMP_NTZ local time skipped by the Los Angeles spring change,
  two of them also with zone tokens.
- #1015: 4 formatted TO_DATE calls ignore the parsed offset and keep the local
  date of TO_TIMESTAMP_NTZ.
- #1016: 712 DATE_FORMAT, ANSI formatted parsing and temporal-to-STRING literals
  are nullable at analysis.
- #1017: 448 TRY_TO_DATE calls fail as an unknown function while resolving.

Recapture from the crate root:

```sh
JAVA_HOME=<JDK 17 or 21> SPARK_LOCAL_IP=127.0.0.1 python \
    tests/fixtures/temporal-contracts/capture_temporal_contracts_spark.py \
    "$(mktemp -d)/temporal-contracts-spark.jsonl"
```
