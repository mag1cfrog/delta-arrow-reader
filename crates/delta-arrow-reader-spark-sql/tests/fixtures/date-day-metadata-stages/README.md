# Date-day metadata stages

This independent Spark 4.2.0 capture retains all 68 original correlated, lambda
and closed-lambda statements, plus 24 existing direct controls. It preserves
every original SQL, public field, row and required error. All 92 original
contracts match, including 37 required failures. The complete raw stages and
plans are in date-day-metadata-stages-spark.jsonl, SHA256
6e8a9b1c0d899eae5bca35ffb12b35299b467e06ecbfcba67fc6f10f6078b15e.

The capture records public, analyzed, optimized and executed schemas whenever
the corresponding stage completes. The executed metadata of each case is used
only for that exact full original observation. Fifteen correlated records have
an executed generated-alias marker absent from their public fields, including
three ANSI failures whose executed schema is available before collection. Other
correlated records retain empty metadata. A direct STRING-day control also has
a physical nullability refinement; it does not define a new #818 policy.

The DATE observer preserves all existing field, type, nullability, array and raw
Date32 checks. It compares the recorded public and executed metadata separately,
then retains the original public result, row and required-error assertions.
Cases without an independent executed reference retain the existing metadata
equality check. Full independent fields remain available for external paired
baseline/candidate review of inherited differences.

manifest.json pins all original fixture inputs and the 92 selected observations.
reference-manifest.json additionally freezes the completed capture, script and
engine receipt. The original scripts and source snapshots remain byte-exact in
sources. The raw JVM collector executes the original plan and reads Date32 days
through getInt/getArray, including values outside Python's calendar range.

Validate sources without an engine:

```sh
python3 capture_date_day_metadata_stages.py unused-output.jsonl --check
```

Recapture from this directory using a Python environment with the recorded
Spark build and a compatible JVM. Use a fresh output path:

```sh
python capture_date_day_metadata_stages.py recapture.jsonl
```

The script requires revision 32f7299601108917fb01920a54e084595b7b3bf8 and UTC.
Preserve the new raw output and complete log; do not replace recorded fields with
DAR observations. Compare cases by source and full original observation.
