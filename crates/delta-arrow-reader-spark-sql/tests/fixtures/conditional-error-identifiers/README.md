# Conditional error identifier references

These fixtures cover searched CASE condition identifiers, explicit SQL RAISE_ERROR, and public ASSERT_TRUE dispatch. The integration test checks complete schemas, rows, required error phases, and user error bodies. [#860](https://github.com/mag1cfrog/delta-arrow-reader/issues/860)

## Capture provenance

| File | Source and scope |
| --- | --- |
| `original-case-errors-spark.jsonl` | Ten original CASE records copied without changes, including the two empty-provider controls and the original capture header. The header describes the original full corpus. |
| `historical-case-conditions-spark.jsonl` | Sixteen exact historical untyped-NULL CASE queries recaptured with their full replay IDs, SQL, ANSI modes, batch sizes and source hashes. |
| `conditional-error-identifiers-spark.jsonl` | 92 independent Spark observations from `manifest.json`: CASE guards, explicit user messages, NULL messages, dead branches, empty providers, and connected internal diagnostics. |
| `assertion-and-internal-controls-spark.jsonl` | 76 independent observations from `assertion-and-internal-controls-manifest.json`: 32 assertion queries and six internal SQL probes in each ANSI mode. |
| `internal-error-controls-before.jsonl` | 48 local observations from the RED commit `b858918c`, including original internal schemas, rows, failure phases, and error bodies. |

Both independent captures use Spark 4.2.0 revision `32f7299601108917fb01920a54e084595b7b3bf8`, UTC, and Java 21. Their headers contain build information and the manifest, capture script, and shared capture helper hashes. The scripts reuse `../case-explicit-else/capture_case_explicit_else.py`; keep these historical source files intact when reproducing the recorded hashes.

The independent capture SHA-256 values are:

- `conditional-error-identifiers-spark.jsonl`: `abe1f4bb021a12b00f6fc9e12cb3467cadf2f05c66b4e833791081fd009102f7`
- `assertion-and-internal-controls-spark.jsonl`: `64006d9cd7e674ea63a1f2a5ad2fb8852c39e4e0a97f7f285c97dd648fbe9197`

To capture again, use the pinned Python environment and set JAVA_HOME to Java 21. From this directory, choose output paths that do not exist:

```sh
PYTHONDONTWRITEBYTECODE=1 TZ=UTC SPARK_LOCAL_IP=127.0.0.1 python capture_conditional_error_identifiers.py /tmp/conditional-errors-new.jsonl
PYTHONDONTWRITEBYTECODE=1 TZ=UTC SPARK_LOCAL_IP=127.0.0.1 python capture_assertion_and_internal_controls.py /tmp/assertion-controls-new.jsonl
```

## Test scope and preserved differences

`conditional_error_identifiers.rs` runs both ANSI modes with DataFusion batch sizes 1 and 32. The original provider helper retains its two-partition layout. The NULL-first message provider uses one ordered batch, and the supplemental assertion providers use one populated partition, so concurrent failures cannot choose a later message first. Direct UDF checks cover scalar and array UTF8, LargeUTF8, UTF8View, first-row NULLs, and zero rows.

The test logs every main and supplemental observation with the strict Spark comparison result. It checks exact parity for the owned error paths and preserves the local diagnostics listed below. The dynamic array subscript control checks Spark values and executed result types, plus an ANSI bounds failure when appropriate. Its former resolution failure remains in the raw baseline. [#863](https://github.com/mag1cfrog/delta-arrow-reader/issues/863)

| Issue | Preserved diagnostic |
| --- | --- |
| [#861](https://github.com/mag1cfrog/delta-arrow-reader/issues/861) | Configured native constant division fails during physical planning; default native division fails during collection. Each configuration is compared before and after Spark planning. |
| [#862](https://github.com/mag1cfrog/delta-arrow-reader/issues/862) | CASE validates an invalid condition before incompatible ARRAY/MAP results; Spark reports the result mismatch first. |
| [#864](https://github.com/mag1cfrog/delta-arrow-reader/issues/864) | ELEMENT_AT and ARRAY_INSERT index-zero errors lack their own identifiers; their empty-input field declarations also differ. |
| [#865](https://github.com/mag1cfrog/delta-arrow-reader/issues/865) | ANSI integral DIV returns an unclassified Arrow divide-by-zero error. |

The supplemental `literal_index` and `floating_div` probes remain raw diagnostics. Spark returns NULL rows with a non-nullable field for `ARRAY(v)[1]`, and rejects DOUBLE DIV at resolution. In ANSI mode, `literal_index` reaches the local default RaiseError and preserves its INVALID_ARRAY_INDEX cause, with the Spark mismatch retained. The `floating_div` probe does not establish an internal execution path.
