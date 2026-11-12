# Map lookup references

`spark-reference.jsonl` is the unchanged independent Spark capture of 88 observations. Its
original script, cases, dependency sources and receipt are retained byte for byte.
`retained-native-contracts.jsonl` separately records the root-reviewed native
baseline and milestone observations for existing gaps, with their issue owners.
Those native contracts do not establish Spark compatibility.

The `nested_consumers` and `struct_paths` checks compare complete values and
schema stages against Spark. Their eight historical observations with `col1`
children remain in the native archive but do not define current expectations.
The other 64 retained controls keep their existing comparisons.

To recapture from a clean checkout on Linux or macOS, use a Python environment with
Spark 4.2.0 revision `32f7299601108917fb01920a54e084595b7b3bf8`. The recorded run used
JDK 21; set `JAVA_HOME` to your existing JDK 21 installation.

```sh
python crates/delta-arrow-reader-spark-sql/tests/fixtures/map-lookup-names/recapture.py --output /tmp/map-lookup-names-spark.jsonl
```

The output path must be new. Omitting `--output` uses
`map-lookup-names-spark.jsonl` in the working directory. The portable recipe runs
the archived script with a temporary manifest that rebases source paths to the
archived helpers. Every query, input, configuration, schema, raw row and typed
error capture stays in the original script. Only source path metadata and its
derived cases hash change; the recipe prints those differences and its hashes.
The initial reference header continues to describe the original capture.
