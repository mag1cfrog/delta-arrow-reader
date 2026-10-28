# CLI input files

Use this reference when writing a tables manifest, a predicate, or execution
settings. For common SQL tasks, start with [Filter, group, and join](../cli-workflows.md).

## Join table snapshots

Declare the tables in a UTF-8 JSON file, then use their aliases in SQL. For a
`tables.json` beside the `orders/` and `customers/` Delta directories:

```json
{"format_version":1,"tables":[
  {"alias":"orders","location":"./orders","table_version":"42"},
  {"alias":"customers","location":"./customers"}
]}
```

```sql
SELECT o.order_id, c.name
FROM orders o LEFT JOIN customers c ON o.customer_id = c.customer_id
ORDER BY o.order_id, c.customer_id
```

Save the query as `join.sql` and run:

```sh
dar query --tables-file tables.json --sql-file join.sql --diagnostics=json > joined.arrow
```

`--tables-file` conflicts with positional `TABLE`, `--table-version`,
`--storage-options-file`, `--env-file`, `--auth-source`, and `--databricks-profile`.
Manifests accept local paths, direct storage URLs, and Unity Catalog table names.
Command-line file paths use the working directory. Locations inside the manifest
use the manifest's directory.
The complete manifest is validated before any table is opened.

<details markdown="1">
<summary>Tables manifest fields and limits</summary>

| Field | Accepted value |
| --- | --- |
| `format_version` | Required JSON integer `1` |
| `tables` | Required array of 1-64 table objects |
| `tables[].alias` | Unique name matching `[a-z][a-z0-9_]{0,62}` |
| `tables[].location` | Nonempty local path, direct storage URL, or `uc://catalog.schema.table`, without control characters or embedded credentials |
| `tables[].table_version` | Optional decimal string from `"0"` to `"18446744073709551615"`; omit to select latest |
| `tables[].authorization` | Optional object selecting one explicit credential source |
| `tables[].authorization.source` | Required when `authorization` is present; an installed `--auth-source` name or `env-file` for direct storage, or `databricks` for UC |
| `tables[].authorization.storage_options_file` | Local JSON storage-options file reference; required for source `json`, optional for other sources |
| `tables[].authorization.env_file` | Local credential env-file reference; required for source `env-file`, invalid for other sources |
| `tables[].authorization.databricks_profile` | Existing Databricks CLI profile name; required for source `databricks`, invalid for other sources |

The file is limited to 1 MiB. Unknown and repeated fields are errors. Versions
must use canonical digits without signs or leading zeros; JSON numbers and
`null` are rejected. The Core version-loading limits described under
[snapshot selection](../cli.md#inspect-a-snapshot) still apply.

Aliases cannot be `t`, `datafusion`, `public`, `information_schema`, `pg_catalog`,
`system`, or a SQL keyword recognized by the parser. URLs cannot contain a
password, query string, or fragment. Azure Blob's `container@account` URL form
is accepted. With stdout output, omitting `authorization` uses default authentication
for direct storage. Remote `--output` requires an authorization object for each
binding. UC entries require source `databricks` and a profile. An explicit
authorization must be an object; `null` is invalid.

</details>

<details markdown="1">
<summary>Per-table authorization and credential file paths</summary>

Each table can select a separate source and storage-options file. For example,
this entry uses S3 credentials from a referenced JSON file:

```json
{"alias":"orders","location":"s3://example-bucket/orders",
 "authorization":{"source":"json","storage_options_file":"./credentials/orders.json"}}
```

The direct-storage sources and their provider restrictions are the same as
[`--auth-source`](cli-authorization.md#explicit-credential-sources): `json`, `env`, `aws-web-identity`,
`aws-container`, `azure-workload-identity`, and `azure-managed-identity`.
The manifest accepts references only; put credential values in the selected
storage-options file, env file, or inherited environment. To select a provisioned
env file, use source `env-file`:

```json
{"alias":"orders","location":"s3://example-bucket/orders",
 "authorization":{"source":"env-file","env_file":"./credentials/orders.env"}}
```

An optional `storage_options_file` can supply noncredential settings, such as
region or endpoint, alongside `env_file`. JSON settings take precedence over
the env file's configuration. Credential values in that JSON file are rejected.
`env_file` cannot accompany another source or a Databricks profile.

`storage_options_file` and `env_file` are relative to the manifest directory.
Credential file paths inside either file use its directory. For example,
`GOOGLE_APPLICATION_CREDENTIALS=adc.json` in `credentials/orders.env` refers to
`credentials/adc.json`. JSON references such as `google_application_credentials`
or `aws_web_identity_token_file` use the options file's directory.
Absolute paths stay absolute. Paths inherited from environment variables use
the process working directory, as they do for positional table commands.

For read or parse errors in `env_file` or `storage_options_file`, diagnostics
identify `--tables-file` and the reference's JSON Pointer, such as
`/tables/1/authorization/env_file`. Any reported line and column refer to the
contents of that referenced file.

The CLI reads the options and env files for every binding before loading any
table. Each binding retains its selected provider; changing those files during
the query cannot switch its identity. Native providers can still renew credentials
and reread rotating identity-token files under their existing rules.

</details>

For a UC/local join, follow the [Unity Catalog example](../unity-catalog.md#join-with-a-local-delta-table).
A manifest can also include direct-storage entries using the authorization
references above. Different UC entries can select different profiles; see
[profile and binding rules](unity-catalog.md#table-names-and-profiles).

Each table loads once and retains that snapshot for the query. Completion
reports the actual version even when latest was requested. These are independent
snapshots and do not form a transaction across tables. For repeatable row
ordering, use `ORDER BY` with columns that resolve ties. Volatile functions and
floating-point aggregation can still produce different values between runs.

## Filter rows

`dar scan --predicate-file PATH` and `dar preview --predicate-file PATH` read a typed JSON predicate from a local
UTF-8 file. The option may appear once; omitting it applies no predicate.
Paths resolve against the working directory, and `-` names a literal file.
The CLI never reads a predicate from stdin.

For a table with an `Int64` column named `id`, this selects IDs greater than
or equal to 10 and returns only `value`:

```sh
cat > predicate.json <<'JSON'
{"op":"ge","column":"id","value":{"type":"int64","value":"10"}}
JSON
dar scan --predicate-file predicate.json --column value /data/orders > filtered.arrow
```

Filter columns need not appear in the output projection. Filtering uses the
native reader and happens before `--limit` counts output rows. For example,
the following file combines a comparison with a null check:

```sh
cat > predicate.json <<'JSON'
{
  "op": "and",
  "args": [
    {"op": "ge", "column": "id", "value": {"type": "int64", "value": "10"}},
    {"op": "is_not_null", "column": "region"}
  ]
}
JSON
dar scan --predicate-file predicate.json --limit 100 /data/orders > sample.arrow
```

<details markdown="1">
<summary id="predicate-objects">Predicate operators and logical combinations</summary>

The file must contain exactly one predicate object. Only the fields shown
for each operator are accepted:

| Operator | Object |
| --- | --- |
| Constant | `{"op":"constant","value":true}` (or `false`) |
| Comparison | `{"op":"eq","column":"id","value":{"type":"int64","value":"10"}}` |
| Null check | `{"op":"is_null","column":"region"}` |
| Non-null check | `{"op":"is_not_null","column":"region"}` |
| AND | `{"op":"and","args":[PREDICATE, ...]}` |
| OR | `{"op":"or","args":[PREDICATE, ...]}` |
| NOT | `{"op":"not","arg":PREDICATE}` |

Comparison operators are `eq` (equal), `ne` (not equal), `lt` (less than),
`le` (less than or equal), `gt` (greater than), and `ge` (greater than or
equal). `PREDICATE` in the table means another predicate object. Empty AND
arguments evaluate to true; empty OR arguments evaluate to false.

Only rows where the predicate evaluates to true are returned. Comparisons
with null values produce unknown results, including under NOT. Use the null
check operators to test for nulls explicitly.

Column names are passed unchanged. They must identify an unambiguous top-level
logical field; dots are not interpreted as nested access. There is no SQL
parser or schema-based scalar type inference.

</details>

<details markdown="1">
<summary id="scalar-objects">Scalar types, validation, and input limits</summary>

Comparison values must be non-null objects with a `type` tag and the fields
listed below. Types must exactly match the column's Arrow type, including
timestamp timezone and decimal precision and scale. No widening or string
coercion is performed.

| `type` | Required fields besides `type` | Native scalar |
| --- | --- | --- |
| `boolean` | `value`: JSON boolean | Boolean |
| `int8`, `int16`, `int32`, `int64` | `value`: canonical integer string fitting the signed width | Int8, Int16, Int32, Int64 |
| `float32` | `value`: finite JSON number, finite after conversion to f32 | Float32 |
| `float64` | `value`: finite JSON number fitting f64 | Float64 |
| `utf8`, `large_utf8` | `value`: JSON string | Utf8, LargeUtf8 |
| `binary`, `large_binary` | `value`: array of integer bytes from 0 through 255; may be empty | Binary, LargeBinary |
| `fixed_size_binary` | `value`: byte array; `size`: positive i32 JSON integer equal to the array length | FixedSizeBinary |
| `date32` | `value`: canonical i32 integer string, days since the Unix epoch | Date32 |
| `timestamp_us` | `value`: canonical i64 integer string, microseconds since the Unix epoch; `timezone`: null or a nonempty string | TimestampMicrosecond |
| `decimal128` | `value`: canonical i128 integer string of the unscaled value; `precision`: u8 JSON integer; `scale`: i8 JSON integer | Decimal128 |

Canonical integer strings are `"0"` or an optional minus sign followed by a
nonzero digit and further digits. `"-0"`, leading zeros, plus signs, whitespace,
fractions, exponents, and overflow are rejected. Integers are parsed directly
at their declared width without passing through floating point.

Bytes, `size`, `precision`, and `scale` use integer JSON tokens, so `1.0` and
`1e0` are invalid for those fields. Float32 conversion may round or underflow
to zero. Dates and timestamps accept negative epoch offsets without calendar
parsing or timezone conversion. `timezone` is required even when null.
Decimals are not rescaled or rounded; negative scales are allowed.

The file limit is 1 MiB (1,048,576 bytes). The tree limit is 1024 predicate
nodes and 32 levels, counting the root as one node at level 1. Duplicate keys,
unknown fields or tags, incorrect shapes or types, unexpected nulls, invalid
UTF-8, and trailing non-whitespace content are rejected before table loading.
These failures produce status 2 with `configuration/invalid_input_json`;
file open or read failures use `configuration/input_file_io`.

Column resolution, scalar/schema compatibility, and decimal precision, scale,
and value compatibility are validated by the core during scan planning.
Failures there produce status 1 with `scan_planning/unsupported_predicate`
and leave stdout empty.

</details>

## Execution options

Omit these options to use the reader's defaults. `--target-partitions` changes
how many groups of files can work in parallel. `--execution-options-file` supplies
a JSON object for file-read limits, buffering, and backend selection.

See [Configuration](../configuration.md#adjust-queries-when-needed) for the shared
reader tradeoffs; its examples use Python. The CLI's
accepted fields and validation rules are listed below.

<details markdown="1">
<summary>Execution flags, complete JSON example, and accepted fields</summary>

`--target-partitions N` on `scan` or `preview` sets the execution partition
target. It may appear once and accepts positive unsigned decimal digits fitting the
executable's `usize` range. Zero, signs, whitespace, fractions, exponents, and
overflow are rejected before table loading with status 2 and
`configuration/invalid_cli_argument`. Leading zeros are accepted.

Omit the flag to keep the core's
[automatic partition selection](../scan-planning.md#choose-a-partition-target).
The built scan may have fewer partitions than requested, for example when
there are fewer files. This flag does not change the table's partition columns.
`inspect` rejects it.

`--execution-options-file PATH` on `scan` or `preview` reads execution settings
from a local UTF-8 JSON object. The option may appear once.
Omitting the file or supplying `{}` uses the native reader's defaults. Each
invocation starts with fresh settings.

```sh
cat > execution.json <<'JSON'
{
  "parquet_backend": "direct",
  "max_concurrent_file_reads_per_scan": 1,
  "max_concurrent_file_reads_per_partition": 1,
  "output_buffer_batches_per_partition": 1,
  "prefetch_files_per_partition": 0,
  "parquet_metadata_size_hint_bytes": null,
  "parquet_full_file_read_threshold_bytes": 1048576
}
JSON
dar scan --target-partitions 2 --execution-options-file execution.json \
  --limit 100 /data/orders > sample.arrow
```

Only these fields are accepted. Omitted fields retain their current native
defaults:

| Field | Accepted value | Current default |
| --- | --- | --- |
| `parquet_backend` | `"direct"` or `"delta_kernel"` | `"direct"` |
| `max_concurrent_file_reads_per_scan` | Positive capacity or null | null, deriving capacity from the partition target and per-partition limit |
| `max_concurrent_file_reads_per_partition` | Positive capacity | `3` |
| `output_buffer_batches_per_partition` | Positive capacity | `1` |
| `prefetch_files_per_partition` | Nonnegative integer | `2` |
| `parquet_metadata_size_hint_bytes` | Positive integer or null | `65536` |
| `parquet_full_file_read_threshold_bytes` | Positive integer or null | null, disabling full-file buffering |

Numeric values must be JSON integer tokens fitting the executable's `usize`
range. Booleans, strings, negative values, fractions, and exponent forms such
as `1e0` are rejected. The three capacity fields are also capped at
`tokio::sync::Semaphore::MAX_PERMITS` (`2305843009213693951` on 64-bit builds).
This capacity bound does not apply to byte sizes or prefetch counts.

Explicit null is accepted only for the three nullable fields in the table.
For `parquet_metadata_size_hint_bytes`, omission keeps the default hint while
null disables it. Null on `max_concurrent_file_reads_per_scan` selects derived
capacity; null on `parquet_full_file_read_threshold_bytes` disables full-file
buffering. Zero is accepted only for `prefetch_files_per_partition`, where it
disables preparation of future files.

Both backends use the concurrency and output-buffer limits. Prefetch, metadata
hints, and full-file buffering apply to the direct backend. See the
[execution options reference](execution-options.md) for their effects.
Execution settings compose with predicates, projection, and row limits.

File paths resolve against the working directory; `-` names a literal file.
The limit is 1 MiB (1,048,576 bytes). Invalid UTF-8, duplicate or unknown keys,
non-object roots, wrong types, and trailing non-whitespace content are rejected
before table loading. These failures use status 2 with
`configuration/invalid_input_json`; file access failures use
`configuration/input_file_io`.

The native setters validate positive values and capacity limits before table
loading. Their failures also exit 2, preserving the core's redacted
`configuration/invalid_configuration` diagnostic and leaving stdout empty.

</details>
