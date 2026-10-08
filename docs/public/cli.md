# Command-line inspection

`dar inspect` prints a Delta table's snapshot version and complete logical
Arrow schema as JSON. It reads Delta log metadata without scanning rows or
reading Parquet data or deletion-vector files. A table with an unsupported
scan protocol can still be inspected if its snapshot and schema can be loaded.

## Build from source

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

`-h` is short for `--help`, and `-V` is short for `--version`. For inspection
help, use `dar inspect --help` or `dar help inspect`.

## Inspect a snapshot

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
dar inspect [--table-version N] [--storage-options-file PATH] [--] TABLE
```

Both options may appear once. Omit `--table-version` to select the latest
snapshot, or pass unsigned decimal digits from `0` through
`18446744073709551615`. Signs, whitespace, fractions, exponents, and overflow
are rejected. Missing values, unknown options, and extra arguments are errors.
Running `dar` without a subcommand exits with status 2.

The parser accepts the full u64 range. The current Kernel cannot load version
`18446744073709551615`; requesting it returns a snapshot error (status 1).

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

The file limit is 1 MiB (1,048,576 bytes), measured before JSON parsing. Reading
stops when that limit is exceeded. Invalid UTF-8, malformed JSON, trailing
non-whitespace content, duplicate keys (including equivalent escaped keys),
and non-string values are rejected before table loading. File paths resolve
against the working directory. A path of `-` names a local file; it does not
read standard input.

## JSON output

A successful inspection writes one UTF-8 JSON object followed by a newline to
stdout and leaves stderr empty. This example represents an empty schema:

```json
{"format_version":1,"table_version":"0","schema":{"fields":[],"metadata":{}}}
```

`table_version` is a decimal string so JSON consumers preserve the full u64
range. `schema` uses the upstream `arrow_schema::Schema` Serde representation
at the locked workspace Arrow version (currently 58.4.0). It retains field
order, nested types, nullability, field metadata, and schema metadata.

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

The envelope does not include table locations or storage options. Requested
schema metadata is returned as table data, including any sensitive strings
the table author put there.

## Errors and process status

An ordinary failure writes one JSON object plus newline to stderr. Its only
fields are the strings `phase`, `code`, and `message`:

```json
{"phase":"configuration","code":"invalid_cli_argument","message":"Invalid command-line arguments."}
```

Diagnostics omit argument values, file paths, URLs, configuration contents,
credential names and values, and underlying dependency messages. Reader
failures retain the core reader's phase, code, and redacted Display message.
The CLI does not start a tracing logger or mix progress or usage text into
errors. Use `--help` for usage.

| Status | Meaning |
| --- | --- |
| 0 | Complete success, including help and version |
| 2 | Invalid CLI arguments or local input configuration before loading |
| 1 | Runtime initialization or reader failure, including schema conversion |
| 3 | stdout write or flush failure, including a broken pipe |

| CLI phase | Code | Meaning |
| --- | --- | --- |
| `configuration` | `invalid_cli_argument` | Invalid command, argument, or option value |
| `configuration` | `invalid_input_json` | Invalid JSON structure, encoding, duplicate key, or size |
| `configuration` | `input_file_io` | Input file could not be opened or read |
| `execution` | `runtime_initialization` | Tokio runtime could not be created |
| `execution` | `output_write` | stdout could not be written or flushed |

Provider configuration rejected during table loading is a reader failure
(status 1), even if the option came from a valid local JSON file.

The complete inspection JSON is built before writing. Failures before output
leave stdout empty. Consumers must check the process status and discard output
from any unsuccessful invocation, including partial output after a write
failure. Status 3 is preserved even when stderr cannot be written.

Each inspection runs in one process with its own Tokio runtime. Runtime
shutdown does not wait indefinitely for blocking background work. Linux
signals keep their normal termination behavior: a killed process is
unsuccessful and need not emit an error object.
