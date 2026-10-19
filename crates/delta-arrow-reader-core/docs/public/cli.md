# Command-line reference

Look up command arguments, input formats, and process behavior here. For a first
read, use the [CLI quickstart](cli-quickstart.md). The shell examples use Bash.

| Task | Command or section |
| --- | --- |
| See the table's columns and version | [`dar inspect`](#inspect-a-snapshot) |
| Save selected rows as Arrow | [`dar scan`](#scan-rows) |
| Select rows by a condition | [Filter rows](#filter-rows) |
| Connect to remote storage | [Storage options file](#storage-options-file) |
| Inspect a Unity Catalog table by name | [Databricks profile](#inspect-a-unity-catalog-table) |
| Change parallel reads or buffering | [Execution options](#execution-options) |
| Handle a command's result in a script | [Inspection JSON](#inspection-json) and [exit statuses](#errors-and-process-status) |

The standalone command is `dar`. A Python wheel installs `delta-arrow-reader`
instead; substitute that name in the examples. Both accept the same arguments.
For packages and runtime requirements, see [Installation](installation.md).

## Inspect a snapshot

`inspect` returns the table version and column schema as JSON. It reads Delta
metadata without reading row data or deletion-vector files. For a direct path or
storage URL, a table whose scan protocol is unsupported can still be inspected
if its snapshot and schema load. Unity Catalog targets require a supported read
protocol.

```sh
dar inspect /data/orders
dar inspect --table-version 0 /data/orders
dar inspect --storage-options-file ./storage.json s3://example-bucket/orders
dar inspect -- './-table with spaces, and commas'
```

The command accepts exactly one UTF-8 local path or URL. Relative paths use
the process working directory. Quote paths and option values that contain
spaces; each must arrive as one command-line argument. `--` ends option
parsing and allows a table name beginning with `-`.

```text
dar inspect [--table-version N] [--storage-options-file PATH]
            [--databricks-profile PROFILE] [--] TABLE
```

<details markdown="1">
<summary>Version bounds and argument parsing</summary>

Each option may appear once. Omit `--table-version` to select the latest
snapshot, or pass unsigned decimal digits from `0` through
`18446744073709551615`. Signs, whitespace, fractions, exponents, and overflow
are rejected. Missing values, unknown options, and extra arguments are errors.
Running `dar` without a subcommand exits with status 2.

The parser accepts the full u64 range. The current Kernel cannot load version
`18446744073709551615`; requesting it returns a snapshot error (status 1).

Explicit versions use a forward listing of retained log entries up to the
requested version. Kernel validates the commits and checkpoints in that listing.
Discovery does not search numeric version gaps.

</details>

## Inspect a Unity Catalog table

Use an existing Databricks CLI workspace profile to resolve a table name and
obtain temporary cloud credentials. DAR then reads the Delta log directly from
S3 or Azure on your machine. It returns the snapshot's schema and version;
Unity Catalog column metadata does not determine the output schema. This
command does not submit SQL or start a cluster or SQL warehouse.

```sh
dar inspect uc://main.sales.orders --databricks-profile work
dar inspect uc://main.sales.orders --databricks-profile work --table-version 0
```

Install the Databricks CLI in the same environment as DAR and make it available
on `PATH`. DAR binaries, wheels, and images do not bundle it. The required
commands and flags were checked with Databricks CLI 1.6.0; earlier versions and
live workspace compatibility have not been validated. The CLI must support
`tables get --include-manifest-capabilities` and
`temporary-table-credentials generate-temporary-table-credentials --operation READ`.
It handles the selected profile's PAT, user OAuth, or service-principal OAuth
authentication. DAR does not start a login flow or read the CLI's token cache.

An administrator must enable external data access on the metastore. The
requesting principal needs `USE CATALOG`, `USE SCHEMA`, `SELECT`, and explicit
`EXTERNAL USE SCHEMA`. Both the workspace and storage endpoint must be reachable.
See [Databricks external access setup](https://docs.databricks.com/aws/en/external-access/admin).
Selecting a profile does not grant additional permissions.

For S3, supply the bucket's region through the existing storage options file.
The credential response does not include it, and DAR does not assume a default:

```json
{"aws_region": "eu-west-1"}
```

```sh
dar inspect uc://main.sales.orders --databricks-profile work \
  --storage-options-file ./storage.json
```

These rules apply to Unity Catalog targets:

- Use exactly three unquoted name components, separated by dots. Each component
  contains ASCII letters, digits, underscores, or hyphens and cannot start with
  a hyphen. Quoted identifiers, URL escapes, query strings, and fragments are
  unsupported. The profile must be explicit and nonempty. Quote profile names
  containing spaces. `--databricks-profile` is invalid for direct paths or URLs.
- Only ordinary managed or external Delta tables with a supported read
  protocol are admitted. Views, pipeline tables, shallow clones, row filters,
  column masks, catalog commits (`catalogManaged`), and coordinated commits
  are unsupported. Authorization failures never fall back to cloud credentials
  from the environment.
- Supported credentials are S3 temporary key/secret/session-token credentials
  and Azure user-delegation SAS. Azure locations must identify the account and
  container using `abfss://` or an Azure Blob/DFS HTTPS URL. Other credential
  types, S3 access points, and other Azure endpoints are unsupported.
- Storage options may supply supported region and transport settings, including
  endpoints, timeouts, and proxies. Credential, bucket, account, container,
  authentication-source, and unsigned-access overrides are rejected, including
  their aliases. S3 needs one nonempty region setting. Unknown options fail.
- Credentials remain in memory and are acquired once. If they expire while
  metadata is loading, the command fails; rerun it for fresh credentials.
  `scan` does not yet accept Unity Catalog targets.

Each Databricks subprocess has a 30-second timeout, no interactive stdin, and
a 1 MiB limit on each output stream. DAR captures and discards child diagnostics
instead of forwarding them. Errors use fixed JSON messages without profile
names, table names, storage URLs, or credentials. Check the selected profile's
login and permissions separately when a metadata or credential request fails.

## Scan rows

```sh
dar scan /data/orders > orders.arrow
dar scan --column customer_id --column total --limit 100 /data/orders > sample.arrow
dar scan --no-columns /data/orders > row-counts.arrow
dar scan --table-version 0 /data/orders > original.arrow
```

The output uses Arrow IPC **stream** framing. Open it with a streaming IPC
reader. Stdout must be a file or pipe; a terminal is rejected with status 2
before table loading, including for `--limit 0`.

```text
dar scan [--table-version N] [--storage-options-file PATH]
         [--predicate-file PATH] [--column NAME ... | --no-columns]
         [--execution-options-file PATH] [--target-partitions N] [--limit N] [--] TABLE
```

Table paths, snapshot selection, and storage options follow the inspection
rules. Each invocation reads one immutable snapshot.

CLI scans skip table warmup and S3 connection sampling, so partial-page reads
stay off. File, row-group, and whole-page skipping remain available. For repeated
queries with automatic connection sampling, use the [Python](python.md) or
[Rust](streaming-reader.md) API.

<details markdown="1">
<summary>Column selection and row-limit rules</summary>

Omit projection flags to read all columns. Repeat `--column` once per logical
column, in the desired output order. A name containing commas or dots remains
one name. The reader validates names and rejects duplicates. `--no-columns`
selects zero columns and preserves the number of live rows in each batch. It
conflicts with `--column` and may appear once.

`--limit` accepts unsigned decimal digits in the platform's `usize` range and
may appear once. Zero is valid; omitting it reads all rows. Limits count live
rows after deletion vectors and predicate filtering. The command does not
promise a global row order.
Empty results, including `--limit 0`, contain the selected schema in a valid
IPC stream.

</details>

<details markdown="1">
<summary>Streaming, schema preservation, and memory use</summary>

The CLI completes scan planning before writing the schema, then writes and
flushes each batch before requesting another. It preserves logical types,
nullability, nested values, and schema and field metadata. It does not collect
the whole result. Memory use includes the core's read buffers, the current
batch, and IPC encoding state; variable-sized batches have no fixed byte bound.

</details>

Require both successful Arrow decoding and a successful child exit status.
IPC readers can accept EOF at a message boundary, so readable output can still
be incomplete. Scan or encoding errors exit 1 without writing the IPC end
marker. Stdout failures, including an early pipe close, exit 3. Status 0 means
the end marker was written and stdout was flushed. Shell redirects can leave
partial files after failure; discard those files.

## Filter rows

`dar scan --predicate-file PATH` reads a typed JSON predicate from a local
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

## Storage options file

`--storage-options-file` accepts a local UTF-8 JSON file containing an object
of string keys and string values. For example, to access an HTTP endpoint:

```json
{"allow_http":"true"}
```

The CLI forwards keys and values unchanged, including case, spaces, and
commas. The core reader interprets provider options. Omitting the file supplies
an empty explicit map and keeps the core's environment and provider credential
behavior. An empty object is also valid.

<details markdown="1">
<summary>Storage file parsing and size limits</summary>

The file limit is 1 MiB (1,048,576 bytes), measured before JSON parsing. Reading
stops when that limit is exceeded. Invalid UTF-8, malformed JSON, trailing
non-whitespace content, duplicate keys (including equivalent escaped keys),
and non-string values are rejected before table loading. File paths resolve
against the working directory. A path of `-` names a local file; it does not
read standard input.

</details>

## Execution options

Omit these options to use the reader's defaults. `--target-partitions` changes
how many groups of files can work in parallel. `--execution-options-file` supplies
a JSON object for file-read limits, buffering, and backend selection.

See [Configuration](configuration.md) to understand the tradeoffs. The CLI's
accepted fields and validation rules are listed below.

<details markdown="1">
<summary>Execution flags, complete JSON example, and accepted fields</summary>

`--target-partitions N` sets the execution partition target for one scan. It
may appear once and accepts positive unsigned decimal digits fitting the
executable's `usize` range. Zero, signs, whitespace, fractions, exponents, and
overflow are rejected before table loading with status 2 and
`configuration/invalid_cli_argument`. Leading zeros are accepted.

Omit the flag to keep the core's
[automatic partition selection](scan-planning.md#choose-a-partition-target).
The built scan may have fewer partitions than requested, for example when
there are fewer files. This flag does not change the table's partition columns.
`inspect` rejects it.

`dar scan --execution-options-file PATH` reads execution settings from a local
UTF-8 JSON object. The option may appear once and is accepted only by `scan`.
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
[execution options reference](reference/execution-options.md) for their effects.
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

## Inspection JSON

A successful inspection writes one UTF-8 JSON object followed by a newline to
stdout and leaves stderr empty. This example represents an empty schema:

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

## Errors and process status

The CLI subscribes to the core's tracing events and writes plain text to stderr.
By default it shows WARN and ERROR, so successful commands are quiet unless a
warning occurs. Core failures retain their operation identity, phase, code,
reason, and available source category. The CLI reports its own failures with
the same `error_phase`, `error_code`, and `error_reason` fields. For example:

```text
ERROR delta_arrow_reader_cli: CLI command failed event="cli.failed" operation="cli_command" outcome="failed" error_phase="configuration" error_code="invalid_cli_argument" error_reason="argument_parse_failed" argument_kind=UnknownArgument
```

This replaces the former JSON stderr object with `phase`, `code`, and `message`.
Stderr is diagnostic text; its layout, field order, and human messages are not a
machine-readable interface. Scripts should use the exit status and stdout
format. Inspection JSON, Arrow IPC, and the exit statuses below are unchanged.
Core event fields are described in the [tracing reference](reference/tracing.md).

Diagnostics omit argument values, file paths, URLs, input contents, credential
names and values, and dependency messages. Argument failures include the
parser's `argument_kind`. JSON failures include `json_kind` (`Syntax`, `Data`, or
`Eof`), `json_line`, and `json_column`. Oversized input and predicate validation
use fixed reasons, such as `input_file_too_large` or
`predicate_depth_limit_exceeded`. Serde schema failures share `Data`; their
messages are not parsed or printed. I/O failures retain `io_kind` and an OS error
number when available. Arrow failures use the core's safe source classifier.

Set `RUST_LOG` to `error`, `warn`, `info`, `debug`, or `trace` for more or less
detail. `off` suppresses diagnostics without changing the command's exit status.
Comma-separated target prefixes also work, for example
`delta_arrow_reader=debug,delta_arrow_reader_cli=warn`. A bare level sets the
default; otherwise unmatched targets use WARN. The `delta_arrow_reader` prefix
also matches the CLI target; a more specific CLI directive overrides it.
Target directives follow
`tracing-subscriber`'s `Targets` filter, without dynamic span-value or regex
matching. An unset or empty value uses WARN. An invalid value produces a safe
warning and falls back to WARN without printing the value.

Only `delta_arrow_reader`, `delta_arrow_reader_cli`, and their `::` subtargets
are allowed. Dependency targets remain disabled even if explicitly requested,
including with `RUST_LOG=trace`. More detail can add lines and spans on stderr;
it does not change stdout. There is no file sink or format flag. Redirect stderr
with your shell:

```sh
RUST_LOG=debug dar inspect /data/table > snapshot.json 2> dar.log
RUST_LOG=trace dar scan /data/table > rows.arrow 2> dar.log
```

Use `--help` for usage. A failed command can have emitted partial stdout; check
its exit status before accepting the result.

| Status | Meaning |
| --- | --- |
| 0 | Complete success, including help and version |
| 2 | Invalid CLI arguments or local input configuration before loading |
| 1 | Runtime, reader, or Arrow IPC encoding failure |
| 3 | stdout write or flush failure, including a broken pipe |

<details markdown="1">
<summary>Diagnostic codes and output after a failure</summary>

| CLI phase | Code | Meaning |
| --- | --- | --- |
| `configuration` | `invalid_cli_argument` | Invalid command, argument, or option value |
| `configuration` | `invalid_input_json` | Invalid JSON structure, encoding, duplicate key, or size |
| `configuration` | `input_file_io` | Input file could not be opened or read |
| `configuration` | `invalid_configuration` | A native execution setting is outside its allowed range |
| `execution` | `runtime_initialization` | Runtime creation failed or reader execution panicked |
| `execution` | `output_write` | stdout could not be written or flushed |
| `execution` | `arrow_ipc` | Arrow IPC encoding failed |
| `configuration` | `invalid_uc_reference` | Invalid UC name/profile, or a profile on a direct path |
| `configuration` | `unsupported_uc_scan` | UC targets currently support inspection only |
| `configuration` | `invalid_uc_storage_options` | Unsupported, conflicting, or duplicate region options |
| `configuration` | `uc_s3_region_required` | S3 needs an explicit region |
| `configuration` | `databricks_cli_missing` | Databricks CLI is unavailable on PATH |
| `execution` | `databricks_cli_failed`, `databricks_cli_timeout` | Child process failed or exceeded its timeout |
| `execution` | `uc_table_request_failed`, `uc_credential_request_failed` | Metadata or credential request failed; check login and permissions |
| `execution` | `invalid_databricks_response`, `unsupported_uc_table` | Response is malformed, oversized, or unsupported |
| `execution` | `uc_credentials_expired` | Credentials expired before inspection finished |
| `execution` | `uc_cancelled` | UC inspection was interrupted |

Provider configuration rejected during table loading is a reader failure
(status 1), even if the option came from a valid local JSON file.

The complete inspection JSON is built before writing. Failures before output
leave stdout empty. Consumers must check the process status and discard output
from any unsuccessful invocation, including partial output after a write
failure. Status 3 is preserved even when stderr cannot be written.

</details>

<details markdown="1">
<summary>Threads, blocked output, cancellation, and signals</summary>

Direct-path commands run in one process. UC inspection also starts bounded
Databricks CLI subprocesses. The CLI drives its asynchronous runtime on
the main thread and ignores `TOKIO_WORKER_THREADS`. Scans use a separate output
thread so a blocked stdout pipe does not stall network reads. The output thread
finishes writing and flushing each batch before requesting another. The core
uses background threads for blocking work. Runtime shutdown does not wait for
unfinished blocking work, including core engine cleanup.

An unrecoverable panic, including failure to start a reader worker, terminates
the process with status 1 and a synchronous ERROR with code
`runtime_initialization` and reason `process_panicked`. Panic payloads and
backtraces are omitted. Runtime setup failures have their own reason and safe
I/O category.

UC inspection handles SIGINT/SIGTERM while resolving and loading metadata,
terminates any pending Databricks child, and exits 130/143. Cancellation emits
an INFO `cli.cancelled` event with code `uc_cancelled`, visible with
`RUST_LOG=info`. It keeps cancellation responsive during stdout writes. On
Windows, Ctrl+C exits 130.

For direct-path commands outside PID 1, Linux signals keep their normal
termination behavior: a killed process is unsuccessful and need not emit an
error event. This applies while loading,
waiting for a batch, or writing to a blocked pipe. A parent that cancels the
child must also wait for it to exit and reap it.

</details>

## Installation alternatives

[Installation](installation.md#cli-binaries-and-container-images) lists the
available packages. Use these details when choosing a runtime or embedding the
command in another application.

<details markdown="1">
<summary id="use-the-cli-from-a-python-wheel">Use the CLI from a Python wheel</summary>

The `delta-arrow-reader` Python package includes a native command with the same
name. Install it from PyPI with pip to get the Python API and CLI in one
environment, or use uv tool/uvx for a separate tool environment. Follow the
[Python installation instructions](https://mag1cfrog.github.io/delta-arrow-reader/installation/#python)
for installation commands.

The wheels support Linux x86_64 with glibc 2.28 or newer, Windows x86_64, and
macOS arm64 with macOS 12.0 or newer. They require CPython 3.10 or newer and
install PyArrow even when you only need the CLI. The installation guide lists
the tested Python versions and runtime requirements. No Rust toolchain is needed
to install a wheel.

Use the full command name for wheel installations:

```sh
delta-arrow-reader --version
delta-arrow-reader inspect /data/orders
delta-arrow-reader scan --column id --limit 100 /data/orders > sample.arrow
```

The reference examples use the standalone name `dar`. Substitute
`delta-arrow-reader` when using a wheel. Both commands accept the same arguments
and have the same output and exit statuses; wheels do not install a `dar` alias.
The Linux archives and containers below have their own platform and runtime
requirements.

</details>

<details markdown="1">
<summary id="use-a-linux-binary">Use a Linux binary</summary>

CLI releases provide validated archives for these platforms:

| Platform | Rust target | Minimum runtime |
| --- | --- | --- |
| Linux amd64 | `x86_64-unknown-linux-gnu` | glibc 2.36, baseline x86-64 CPU |
| Linux arm64 | `aarch64-unknown-linux-gnu` | glibc 2.36, baseline ARMv8-A CPU |

Both targets are built on Debian 12. The same binaries are tested on Debian 12,
Ubuntu 24.04, and Fedora 44. They use glibc and libgcc shared libraries. On Debian
or Ubuntu, install `libc6`, `libgcc-s1`, and `ca-certificates`; on Fedora, install
`glibc`, `libgcc`, and `ca-certificates`. The certificate bundle is needed for
HTTPS storage. The archives do not support Alpine Linux's musl runtime or a
`scratch` container. Other platforms can build from source.

Follow the [version-pinned installation and checksum steps](installation.md#cli-binaries-and-container-images)
to download the two archives, `cli-artifacts.json`, and `SHA256SUMS` from a
completed GitHub Release. Extract the archive for your CPU after verification.
Keep `LICENSE` and `NOTICE` alongside `dar`, including when distributing it with
a service. The executable works outside a source checkout and does not require
Python or Rust.

</details>

<details markdown="1">
<summary id="use-a-container-or-a-go-service">Use a container or a Go service</summary>

The CLI container packages the same standalone binary with its runtime libraries
and CA certificates. Copy the index digest from the completed Release's CLI
section and replace `RELEASE_INDEX_HASH` below. Docker selects the matching
architecture from the index:

```sh
dar_image='ghcr.io/mag1cfrog/delta-arrow-reader@sha256:RELEASE_INDEX_HASH'
docker run --rm --read-only "$dar_image" --version
docker run --rm --read-only \
  --mount type=bind,src=/absolute/path/to/table,dst=/table,readonly \
  "$dar_image" inspect /table
docker run --rm --read-only \
  --mount type=bind,src=/absolute/path/to/table,dst=/table,readonly \
  "$dar_image" scan --column id --limit 100 /table > sample.arrow
```

The image runs `dar` directly as UID/GID `65532:65532`. Omitting arguments prints
help. Each invocation exits when the command finishes; the image does not run a
Reader server. Mounted files and their parent directories must be readable by that user.
Supply storage credentials and configuration at runtime. Keep HTTPS certificate
verification enabled and provide a readable custom trust bundle when required
by your storage endpoint. See [HTTPS configuration](installation.md#https-and-build-prerequisites).

When `dar` is the container's PID 1, SIGINT and SIGTERM stop it with exit status
130 and 143, respectively. The CLI does not flush buffered output or finish an
IPC stream after either signal. Treat that result as an interrupted scan.

A Go service can invoke the binary with `exec.CommandContext` and decode stdout
with the official Arrow Go streaming IPC reader. Pass each CLI argument
separately, drain stderr separately, and require both successful IPC decoding and
a zero child exit status. An early stop or decoding failure must cancel, close,
and reap the child before returning.

The runnable [Go example](https://github.com/mag1cfrog/delta-arrow-reader/tree/main/examples/go-cli)
includes bounded stderr capture, cancellation tests, and a consumer Dockerfile.
Its `COPY --from` stages copy `/usr/local/bin/dar` and `/usr/share/doc/dar/` from
an explicit CLI image into a Debian 12 service image. The service still needs
glibc 2.36 or newer, the binary manifest's shared libraries, and CA certificates.
Copying the executable alone does not support Alpine or `scratch`. The Go
application needs no cgo or Rust toolchain at runtime.

</details>

<details markdown="1">
<summary id="build-from-source">Build from source</summary>

Install the [Rust build prerequisites](installation.md#https-and-build-prerequisites),
then run these commands from a checkout containing the CLI crate:

```sh
cargo build --locked -p delta-arrow-reader-cli
./target/debug/dar --version
./target/debug/dar --help
```

To install the binary into Cargo's binary directory, usually `~/.cargo/bin`:

```sh
cargo install --locked --path crates/delta-arrow-reader-cli
dar inspect --help
```

The CLI package is built from source and is not published to crates.io. It
shares the workspace version, and `dar --version` prints `dar <version>` plus
a newline. Help and version work outside the checkout without opening files,
loading a table, starting a runtime, or contacting storage.

If the executable is named `delta-arrow-reader` (`delta-arrow-reader.exe` on
Windows), help and version display `delta-arrow-reader`. Both names accept the
same arguments and produce the same inspection JSON, Arrow streams, diagnostics,
and exit statuses.

`-h` is short for `--help`, and `-V` is short for `--version`. For inspection
help, use `dar inspect --help` or `dar help inspect`. Scan help is available
through `dar scan --help` or `dar help scan`.

</details>
