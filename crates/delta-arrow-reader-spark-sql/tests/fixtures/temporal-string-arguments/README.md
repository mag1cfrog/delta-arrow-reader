# STRING date/time arguments

`requests.json` and `addendum-requests.json` contain only SQL and input definitions.
Their captures record 60 observations from Spark 4.2.0, revision
`32f7299601108917fb01920a54e084595b7b3bf8`, with Java 21 and Python 3.12.
Headers retain source and JAR hashes, settings and registered input readbacks.

The #942 cases cover literal and registered STRING arguments of DATEDIFF, YEAR,
MONTH, DAY, DAYOFYEAR, QUARTER, LAST_DAY, ADD_MONTHS, NEXT_DAY, DATE_TRUNC, HOUR,
MINUTE, MONTHS_BETWEEN and EXTRACT, plus DATE membership in lists and subqueries
in both operand directions. Spaces, ASCII controls, partial dates, offsets,
pre-epoch fractions, NULLs and duplicate values distinguish the shared Spark
parsers from native casts. Malformed inputs run without ANSI and must yield
NULL. Implicit timestamp arguments use the session LTZ type even when the
default timestamp type is NTZ. Typed temporal controls retain their own paths.

The #943 cases cover boundary trimming and fractions longer than nine digits
through CAST, TRY_CAST, TO_TIMESTAMP, TO_TIMESTAMP_NTZ and typed timestamp
literals. They include an offset after the fraction and values before the epoch.
Standalone TIME retains its existing precision limit; time-only timestamp
syntax and rejection of unsupported forms are outside this fixture.

`temporal_string_arguments.rs` replays all 60 records at batch sizes 1 and 32:
76 #942 and 44 #943 contexts. It checks exact rows, NULLs, multiplicity and
executed types, including DOUBLE bits, DECIMAL scale and timestamp annotations.
100 contexts compare every cell directly to Spark. In 20 contexts, the projected
IN-subquery NULL cells retain SQL UNKNOWN under the existing #1305 optimizer
boundary; all other cells still compare to Spark. These are labeled separately
in the replay output. Non-ANSI DATE/STRING membership compares strings, while
ANSI membership parses STRING operands as DATE. Unordered results use
duplicate-preserving multisets. Actual batches are
validated against the physical schema. Names, metadata, nullability and error
stages are outside these checks.

From the repository root, recapture each source set to a fresh output path:

```sh
JAVA_HOME=<JDK 21> SPARK_LOCAL_IP=127.0.0.1 TZ=UTC python3.12 \
  crates/delta-arrow-reader-spark-sql/tests/fixtures/temporal-string-arguments/capture_spark.py \
  /tmp/temporal-strings-spark.jsonl
JAVA_HOME=<JDK 21> SPARK_LOCAL_IP=127.0.0.1 TZ=UTC python3.12 \
  crates/delta-arrow-reader-spark-sql/tests/fixtures/temporal-string-arguments/capture_addendum_spark.py \
  /tmp/temporal-strings-addendum-spark.jsonl
```

Replay from the repository root:

```sh
cargo test --locked --manifest-path crates/delta-arrow-reader-spark-sql/Cargo.toml \
  --test temporal_string_arguments
```
