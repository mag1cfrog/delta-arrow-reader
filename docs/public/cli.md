# Command-line reader

`dar inspect` prints a Delta table's snapshot version and complete logical
Arrow schema as JSON. It reads Delta log metadata without scanning rows or
reading Parquet data or deletion-vector files. A table with an unsupported
scan protocol can still be inspected if its snapshot and schema can be loaded.

`dar scan` reads one snapshot and writes its rows as an Arrow IPC stream. Use
it with redirected stdout or a subprocess pipe.

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
help, use `dar inspect --help` or `dar help inspect`. Scan help is available
through `dar scan --help` or `dar help scan`.

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

Explicit versions use a forward listing of retained log entries up to the
requested version. Kernel validates the commits and checkpoints in that listing.
Discovery does not search numeric version gaps.

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
         [--column NAME ... | --no-columns] [--limit N] [--] TABLE
```

Table paths, snapshot selection, and storage options follow the inspection
rules. Each invocation reads one immutable snapshot.

Omit projection flags to read all columns. Repeat `--column` once per logical
column, in the desired output order. A name containing commas or dots remains
one name. The reader validates names and rejects duplicates. `--no-columns`
selects zero columns and preserves the number of live rows in each batch. It
conflicts with `--column` and may appear once.

`--limit` accepts unsigned decimal digits in the platform's `usize` range and
may appear once. Zero is valid; omitting it reads all rows. Limits count live
rows after deletion vectors. The command does not promise a global row order.
Empty results, including `--limit 0`, contain the selected schema in a valid
IPC stream.

The CLI completes scan planning before writing the schema, then writes and
flushes each batch before requesting another. It preserves logical types,
nullability, nested values, and schema and field metadata. It does not collect
the whole result. Memory use includes the core's read buffers, the current
batch, and IPC encoding state; variable-sized batches have no fixed byte bound.

Require both successful Arrow decoding and a successful child exit status.
IPC readers can accept EOF at a message boundary, so readable output can still
be incomplete. Scan or encoding errors exit 1 without writing the IPC end
marker. Stdout failures, including an early pipe close, exit 3. Status 0 means
the end marker was written and stdout was flushed. Shell redirects can leave
partial files after failure; discard those files.

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
| 1 | Runtime, reader, or Arrow IPC encoding failure |
| 3 | stdout write or flush failure, including a broken pipe |

| CLI phase | Code | Meaning |
| --- | --- | --- |
| `configuration` | `invalid_cli_argument` | Invalid command, argument, or option value |
| `configuration` | `invalid_input_json` | Invalid JSON structure, encoding, duplicate key, or size |
| `configuration` | `input_file_io` | Input file could not be opened or read |
| `execution` | `runtime_initialization` | Runtime creation failed or reader execution panicked |
| `execution` | `output_write` | stdout could not be written or flushed |
| `execution` | `arrow_ipc` | Arrow IPC encoding failed |

Provider configuration rejected during table loading is a reader failure
(status 1), even if the option came from a valid local JSON file.

The complete inspection JSON is built before writing. Failures before output
leave stdout empty. Consumers must check the process status and discard output
from any unsuccessful invocation, including partial output after a write
failure. Status 3 is preserved even when stderr cannot be written.

Each command runs in one process. The CLI drives its asynchronous runtime on
the main thread and ignores `TOKIO_WORKER_THREADS`. Scans use a separate output
thread so a blocked stdout pipe does not stall network reads. The output thread
finishes writing and flushing each batch before requesting another. The core
uses background threads for blocking work. Runtime shutdown does not wait for
unfinished blocking work, including core engine cleanup.

An unrecoverable panic during reader execution, including failure to start a
runtime thread, terminates the process with status 1 and a redacted
`runtime_initialization` diagnostic. Panic payloads and backtraces are omitted.
Linux signals keep their normal termination behavior: a killed process is
unsuccessful and need not emit an error object. This applies while loading,
waiting for a batch, or writing to a blocked pipe. A parent that cancels the
child must also wait for it to exit and reap it.
