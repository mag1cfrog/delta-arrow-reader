# CLI output formats

For saving and reopening a result, start with [Save and read results](../cli-results.md).
This reference defines typed rows, output receipts, optional provenance, and
inspection records. Success and failure records on stderr are documented in
[CLI diagnostics](cli-diagnostics.md).

## Read a small result as typed JSON

Choose columns or aggregates that answer the task. For example, to count the rows
in a pinned snapshot:

```sh
printf 'SELECT count(*) AS n FROM t\n' > count.sql
dar query /data/orders --table-version 42 --sql-file count.sql --format json
```

The response contains `kind: "rows"`, `format_version: 1`, each alias and its actual table
version in `bindings`, a `schema`, positional `rows`, and `row_count` as a decimal
string. Success reports `completion: "complete"` and `scope: "query"`. An empty result retains its
schema. Fields retain names, nullability, and Arrow type parameters; optional
metadata and physical dictionary IDs are omitted.

Rows and nested structs use arrays in field order, preserving duplicate names.
Integer and decimal values are strings; decimals store the unscaled integer.
Dates, times, timestamps, and durations are integer strings in their Arrow unit.
Binary values use padded base64. Lists are arrays, maps are ordered key/value
pairs, and dictionaries contain decoded values. Booleans, strings, and nulls
use native JSON. Finite floats round-trip at their Arrow precision; `-0`, `NaN`,
`Infinity`, and `-Infinity` are strings. Unions, intervals, and run-end encoded
types fail with `unsupported_json_type`; use Arrow for these or for NaN payloads.

`dar capabilities --schema rows` describes the envelope and common value rules,
including decimal scale, date/time units, timestamp epoch and timezone, and
JSON nulls. Select further encodings such as `--detail binary` from its encoding
list. Individual field and encoding lookups remain available, including
`--detail rows` and `--detail decimal`. These lookups do not read the table.

The default 8 KiB stdout budget covers the entire response. For larger results,
use `--output PATH` with an explicit byte budget when needed. Keep the same SQL, authorization, table bindings, and
snapshot versions when recovering the same result. Selecting fewer columns,
aggregating, or adding SQL `LIMIT` changes the requested result. `--max-rows`
never truncates a successful result.

## Save a result

<span id="save-a-result-with-a-manifest"></span>

Use `--output PATH` on `scan` or `query` to save one complete result without
printing its rows. The directory must exist and `PATH` must be unused.
There is no overwrite option. An unrelated `PATH.manifest.json` does not block
ordinary export.

```sh
dar --diagnostics=json query /data/orders --table-version 42 \
  --sql-file query.sql --format json --output totals.json
dar --diagnostics=json scan /data/orders --table-version 42 \
  --column customer_id --column total --output orders.arrow
```

Stdout returns a JSON receipt with `format_version: 2`, `kind: "result_artifact"`,
and the absolute `result` path. It contains no rows or schema. With
`--diagnostics=json`, stderr completion v3 reports the command and receipt byte
count. Require exit 0 and the completion record before accepting success.

The CLI stages the result on the destination filesystem. After the read and
temporary-query cleanup succeed, it publishes the complete file without replacing
an existing name. A failure after publication may leave a complete result;
preserve it and choose an unused path if completion is uncertain. Crashes and
uninterruptible IO can leave temporary files; power-loss durability is not promised.

Read the result using the selected `--format` (default Arrow IPC), regardless
of the filename extension. JSON uses the result's `schema.fields` to decode
values. The result file can be read or moved on its own. Default export creates
no sidecar and computes no result or replay-input hashes.

### Record provenance with --manifest

Add `--manifest` to save `PATH.manifest.json` with the sources and options needed
for replay. This flag requires `--output`; both destinations must be unused.
The receipt uses `format_version: 1` and includes absolute `result` and `manifest`
paths.

This mode stages both files and publishes the manifest after the result. The two
publications are not one atomic transaction. This pair requires a matching
manifest with `completion: "complete"`; a lone result does not establish that
the requested provenance was saved.

The manifest records the result path/format/SHA-256, rows, source batches, byte
count, completion/scope, actual binding versions, build versions, and replay
inputs. `limited_scan` covers a requested prefix; a query covers its SQL result.
JSON preserves rows but not source batch boundaries. For replay, reuse the
original input files with `replay.options` in `replay.working_directory`, and
select a new `--output`. Pin every alias using `bindings`; for `--tables-file`, copy the
referenced declarations beside the original tables file, preserving relative paths,
and set each `table_version` to its recorded version. Numeric replay options are
decimal strings; omit null/false options and repeat `--column` for each value.
Keep authorization references provisioned. Snapshot/data/deletion-vector files
must remain available, and SQL ordering or volatile expressions can still vary.

Only noncredential input bytes are hashed. SQL text, credential contents,
credential hashes, and signed URL secrets are not copied into the manifest.
The manifest can contain local paths, profile names, columns, and resource
settings. Treat it and the result as local data. Temporary files are owner-only
on Unix; on Windows use a destination directory with a private inherited ACL.

Inspect only the fields needed for the next read or replay step:

```sh
dar capabilities --schema result-manifest --detail result
dar capabilities --schema result-manifest --detail replay.inputs
```

For a JSON Schema validator, save the full definition with
`dar capabilities --schema result-manifest --full > manifest-schema.json`.

<span id="verify-a-json-result"></span>

For `--manifest` output, require manifest `completion: "complete"` and
`result.path` matching the receipt, then use `result.format` to choose the reader.
Decode only the columns the task needs. For a separate integrity audit, use the
[JSON](../cli-verify-json.md) or [Arrow](../cli-verify-arrow.md) audit example.

Write, serialization, budget, or publication failures never fall back to rows
on stdout. Existing destinations are preserved, including files created during
the read. If publication has started, cancellation or a failed receipt can leave
a complete pair despite a nonzero exit status. Check its completion, output path
and requested bindings before using it or retrying with unused names. Crashes
and uninterruptible IO can leave temporary files. Receipts are capped at 8 KiB and manifests at 1 MiB;
paths or provenance that exceed those limits fail before publication.

## Inspection JSON

A successful inspection writes one UTF-8 JSON object followed by a newline to
stdout. Diagnostic logging uses stderr and does not change this JSON.
With `--view summary`, the default result is:

```json
{"format_version":1,"kind":"inspection_summary","table_version":"0","checks":{"scan_protocol":"passed","data_files":"not_checked"}}
```

`scan_protocol` is `passed` or `failed` according to Core's reader-protocol
validation for the loaded snapshot.
`data_files` is always `not_checked`: neither Parquet nor deletion-vector access
was tested. These checks do not establish authorization for a subsequent scan.

With `--column id`, `fields` replaces `checks`:

```json
{"format_version":1,"kind":"inspection_summary","table_version":"0","fields":[{"name":"id","data_type":"Int32","nullable":true}]}
```

Types use Arrow's Serde encoding. Nested fields use the same reduced fields;
`dict_is_ordered` appears only for dictionary fields. Dictionary IDs are omitted.
Metadata is absent unless explicitly requested, and empty metadata is always
omitted. This selected representation is not a serialized `arrow_schema::Schema`.
With `--column-prefix i`, `column_names: ["id"]` replaces `checks` instead.
Names, timezone text, and metadata are untrusted table content.

The full inspection view represents an empty schema as:

```json
{"format_version":1,"table_version":"0","schema":{"fields":[],"metadata":{}}}
```

`table_version` is a decimal string so JSON consumers preserve the full u64
range. `schema` uses the upstream `arrow_schema::Schema` Serde representation
at the locked workspace Arrow version (currently 58.4.0). It retains field
order, nested types, nullability, field metadata, and schema metadata.

<details markdown="1">
<summary>Field encodings and format compatibility</summary>

For example, a nullable string field is encoded as:

```json
{
  "name": "city",
  "data_type": "Utf8",
  "nullable": true,
  "dict_id": 0,
  "dict_is_ordered": false,
  "metadata": {}
}
```

Parameterized types use Serde's enum encoding, such as
`{"Decimal128":[20,4]}` or `{"Struct":[...]}`. Rust consumers using the matching
Arrow version with its `serde` feature can deserialize the `schema` value
directly into `arrow_schema::Schema`. JSON object key order is not significant.
Incompatible changes to the envelope or schema encoding require a new
`format_version`.

</details>

The envelope does not include table locations or storage options. Requested
schema metadata is returned as table data, including any sensitive strings
the table author put there.
