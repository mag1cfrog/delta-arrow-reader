# Conditional SLICE coordinates

This package captures six CASE queries in both ANSI modes with Spark 4.2.0 at
revision `32f7299601108917fb01920a54e084595b7b3bf8`. It uses the existing
`slice-coordinates` decoder and structured-error observer through relative paths.

`manifest.json` contains the exact SQL and provider inputs from the independent
v2 capture. `references/` archives that capture's original source and manifest
without changing their bytes. The lineage records their hashes and the original
raw hash, `10816e70cc7dfd46dafbed53b48a35a018a31fb3092458e7a78647a69120a4ac`.
The original raw remains in the review evidence; the portable recapture has its
own source hash, manifest hash, raw plans and tracebacks.

From this directory, use the pinned Spark Python environment and a fresh output
path:

```sh
python3 capture_spark.py --self-check
TZ=UTC python3 capture_spark.py /path/to/fresh-reference.jsonl
python3 verify_capture.py /path/to/fresh-reference.jsonl \
  --frozen-reference /path/to/894-conditional-coordinate-spark-v2.jsonl
```

The verifier compares SQL, provider inputs, ordered rows, complete schema-stage
maps, structured errors and error phases exactly with frozen v2. Generated plan
IDs and traceback source paths can differ; both raw files retain them in full.
Invalid reached literal branches fail during optimization, dead literal branches
return `[9]`, and the provider-dependent invalid branch fails during collection.
