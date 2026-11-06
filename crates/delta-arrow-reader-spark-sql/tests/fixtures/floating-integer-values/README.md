# Floating to integer values

`spark.jsonl` contains the original independent Spark 4.2.0 capture: 176 SQL
queries in both ANSI modes, with complete stage observations and encoded rows.
Its header identifies the source files that actually produced it. The exact
original script and manifest are retained in `sources/`.

`capture_floating_integer_values.py` and `manifest.json` provide a portable
recapture of the same queries and five inputs. They reuse the existing stage
observer and typed codec. The script checks the original source, manifest and
reference hashes, then records its own source hashes and actual runtime
settings in a new output header. It resolves shared fixtures from this directory
rather than the original machine's absolute path.

The original preparation also checked an unmerged #796 reference file. Its hash
is retained under `capture_provenance.historical_reference_checks`; recapture
does not require that file. This changes no query or input.

Use the pinned Spark environment and a fresh output path:

```sh
TZ=UTC SPARK_LOCAL_IP=127.0.0.1 PYTHONDONTWRITEBYTECODE=1 \
JAVA_HOME=/path/to/pinned-jdk \
/path/to/pinned-spark-venv/bin/python \
capture_floating_integer_values.py /absolute/path/to/fresh-output.jsonl
```

Run from this directory or use an absolute script path. A completed capture has
353 lines: one provenance header and 352 records. `--check` verifies inputs and
provenance without importing Spark. Compare recaptured business observations
with the original records before replacing any accepted reference; headers are
expected to identify different capture sources.

The native replay observes both batch sizes 1 and 32. Its 704 comparisons cover
the measured matrix; #897 owns the Legacy floating conversion policy. Field
declarations require Spark's conservative nullable Fields. ANSI primitive
overflow requires `[CAST_OVERFLOW]` while preserving the original Arrow cause,
numeric value, target and failure stage. The checked BIGINT endpoint (#1020)
keeps its separate value contract.

`retained-baseline-facets.json` binds the exact remaining native differences to
real accepted-runtime observations. Its 480 old #796 nullable leaves remain
history and cannot permit an incorrect declaration. The gate requires 668 strict
comparisons and 36 exact retained differences: 28 #1020 endpoint contexts, four
#865 DIV diagnostics, and four #1031 substring contexts that fail later than
Spark and expose extra native Fields. The historical file keeps its original
owner IDs. Expected primitive errors gain only the overflow context; their
causes and stages cannot drift. For the two ANSI substring queries, expected
native schema copies require the corrected nullable declaration and preserve
every other component and boundary. Their four batch contexts retain the #1031
stage difference despite having the required overflow identifier.
The #1020 endpoint links include the original TRY literal's IEEE bits and the
matching provider input row. Non-TRY Legacy rows receive no retained-value
allowance. Strict Spark flags remain false for retained differences; scoped
regression preservation is reported separately from strict comparison.
