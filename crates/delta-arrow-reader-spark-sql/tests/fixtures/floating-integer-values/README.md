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

`retained-baseline-facets.json` preserves the accepted runtime observations and
their original owner IDs. Its 480 old #796 nullable leaves and four old #1031
late failures remain historical evidence. They cannot permit an incorrect
declaration or restore a late substring failure.

The current gate requires 672 strict comparisons and 32 exact retained
differences: 28 #1020 endpoint contexts and four #865 DIV diagnostics. Primitive
errors retain their original Arrow cause, value, target and required boundary.
The two ANSI substring queries now compare their four batch contexts against
whole measured observations in
[`substring-literal-preparation-current-native.json`](../substring-literal-preparation-current-native.json),
including the real public declarations and unavailable later stages.
The #1020 endpoint links include the original TRY literal's IEEE bits and the
matching provider input row. Non-TRY Legacy rows receive no retained-value
allowance. Strict Spark flags remain false for retained differences; scoped
regression preservation is reported separately from strict comparison.
