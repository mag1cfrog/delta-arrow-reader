# File-origin references

`requests.json` supplies 28 source-only requests in both ANSI modes.
`spark-reference.jsonl` records Spark 4.2.0 revision
`32f7299601108917fb01920a54e084595b7b3bf8`, Python 3.12 and Java 21.
The capture uses explicit in-memory inputs and two local Parquet files with
fixed basenames. It records full file URIs, executed STRING types, nonfile empty
strings, stored and aliased `_metadata` fields, simple subquery and CTE origins,
file filters and grouping, qualified stars, multi-file-source rejection, and
LEFT/RIGHT/FULL unmatched nonfile rows returning empty filenames.
Twenty-four successful observations compare complete values and types. Two compare
multi-source rejection. The two file/nonfile join observations are explicit
policy controls: Spark's captured BuildLeft broadcast plan streams the memory
side and reports empty filenames. Controlled repeats with the file side streamed
report the real paths. The reader preserves per-row file origins; its control
must match the exact standalone file reference and retain the observed mismatch.
These controls do not count as strict Spark value matches.

The Rust comparison replaces only the captured temporary directory URI with
the real Delta fixture's directory URI. Basenames, row membership, values and
logical result types must match. Parquet's nullable read schema is not a Delta
schema reference; the file controls compare types and values while separate
physical-array checks validate the reader's declared nullability and encodings.
The URI buffers remain Arrow string views, and partition strings retain their
existing dictionary encoding.

The capture environment has no Delta Lake installation. Open-source Spark's
Parquet temporary views do not expose `_metadata`, so these records do not claim
Spark Delta metadata parity. The consumer offers only `_metadata.file_path` and
`_metadata.file_name`, with the Core URI format documented in the crate README.
Real partitioned Delta files, surviving deletion-vector rows, column mapping,
hidden fields and actual storage requests have separate integration checks.

The driver reads only requests and explicit inputs. It owns its JVM, records the
capture lifecycle, and removes its own temporary Parquet directories. Capture to
a fresh output path with the maintained environment:

```sh
TZ=UTC python capture_spark.py --self-check
TZ=UTC python capture_spark.py /path/to/new/spark-reference.jsonl
```
