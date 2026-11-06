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
declarations (#796), checked overflow identity (#922), and the checked BIGINT
endpoint (#1020) have separate contracts.

`retained-baseline-facets.json` binds the exact remaining native differences to
real accepted-runtime observations. It permits only listed nullable leaves,
checked endpoint cells, and complete known error causes, stages and availability.
The #1020 endpoint links include the original TRY literal's IEEE bits and the
matching provider input row. Non-TRY Legacy rows receive no retained-value
allowance. Strict Spark flags remain false for retained differences; the scoped
#897 result is reported separately.
