# CLI diagnostics reference

Use this reference when integrating the CLI into a service or configuring
specific log targets. For ordinary troubleshooting, see
[Errors and process status](../cli.md#errors-and-process-status).

## Machine diagnostics

Use the global option `--diagnostics=json` before or after the command:

```sh
dar --diagnostics=json inspect /path/to/table > snapshot.json 2> diagnostics.jsonl
dar scan /path/to/table --limit 10 --diagnostics=json > rows.arrow 2> diagnostics.jsonl
```

The option requires `=json` and must precede `--`, which marks the end of
options. Argument errors also use JSON when an invalid argument appears before
`--diagnostics=json`. A value such as `--column=--diagnostics=json` does not
enable this mode.

This mode writes one UTF-8 JSON object followed by a newline to stderr. It
emits no human logs or ANSI escapes, regardless of `RUST_LOG`. Stdout and exit
statuses are unchanged. The wheel command, `delta-arrow-reader`, accepts the
same option. A handled failure produces:

```json
{
  "format_version": 1, "kind": "diagnostic", "terminal": true,
  "phase": "configuration", "code": "invalid_cli_argument",
  "reason": "argument_parse_failed", "source_kind": null,
  "message": "CLI command failed.", "argument_kind": "UnknownArgument",
  "retryable": false, "recovery": "fix_input"
}
```

Core failures retain their typed phase, code, reason, and source category.
`source_kind` is null when absent. Safe optional fields include `argument_kind`,
`json_kind`, JSON `line` and `column`, `io_kind`, and `os_error`. No raw dependency
errors, input values, paths, credentials, or captured child output are included.
`message` is fixed text; use the classification fields for decisions.

Argument failures include a `hint` describing how to correct the invocation.
When Clap identifies a declared argument, `argument` gives its name and
`expected` describes the accepted format. Numeric options include their range:
`--limit` and `--table-version` accept zero; `--target-partitions` requires at
least one. Row and partition limits use the binary's native integer width.
For example:

```sh
dar scan /path/to/table --limit invalid --diagnostics=json
```

This exits 2 and reports `argument: "--limit"` with its unsigned decimal range.
Unknown command and option names are omitted; the hint directs callers to
`--help`. These details never include supplied values. Treat `hint` and
`expected` as guidance; use `code`, `argument_kind`, and `recovery` for decisions.
The same correction fields appear in text-mode argument errors.

JSON input and file-read failures identify the supplying option in `argument`,
such as `--predicate-file`, without exposing the filename. `expected` describes
that input's structure, and `hint` gives a next action for malformed or incomplete
JSON, schema violations, unreadable files, oversized inputs, or predicate limits.
Schema errors report known field locations in `field_path`. Invalid `op`, scalar
`type`, or `parquet_backend` values include the permitted choices in `expected`.
Type errors describe the required JSON type or integer range. Missing and
duplicate fields identify the known field to add or deduplicate. For example,
an invalid scalar type inside a comparison reports `/value/type`.

`field_path` is a JSON Pointer. Unknown fields identify their containing object
and list its permitted fields. Storage option errors also identify the containing
object, since its keys may contain credentials. An empty path, `""`, identifies
the document root. A missing field's path identifies where to add it. Unknown
keys and supplied values never appear in these paths or hints. When the document
cannot be analyzed safely, diagnostics keep the input's general guidance.

Execution settings rejected by a native range check also include `field_path`,
a JSON Pointer made from known schema fields. For example, an execution file
containing `{"max_concurrent_file_reads_per_partition":0}` produces
`argument: "--execution-options-file"` and
`field_path: "/max_concurrent_file_reads_per_partition"`. Here `expected` gives
the field's allowed integer range and whether it accepts null. Set the field
within that range, or remove it to use the default. These fields appear in both
text and machine diagnostics. `field_path` is omitted when no specific field
can be identified.

Predicate depth and node-count failures identify the node exceeding the limit.
Float32 overflow identifies the scalar's `value`; fixed-size binary validation
identifies its `size`. Paths include nesting and zero-based array indexes, such
as `/args/1/arg/value/value`. `expected` and `hint` describe the requirement at
that location. Paths contain only known schema field names and indexes.

Unknown or repeated projection columns and supported predicate planning errors
also include correction fields. For unknown columns or scalar type mismatches,
run `inspect` with the same table and snapshot selection, then use its
`schema.fields` to correct `--column` or the predicate file. These planning
errors retain the core's `code` and `reason`, with exit status 1 and
`recovery: "fix_input"`. They omit `field_path` because the core does not identify
the failing predicate node. Text mode adds an ERROR `cli.input_correction`
event after the core failure event; machine mode keeps one terminal record.

`recovery` uses `fix_input` for invalid input, `authorize` for unauthenticated
sources, `request_access` for permission failures, `retry` for known transient
I/O failures, or `change_request` for unsupported operations. Unclassified
failures use `unknown` and `retryable: null`. A true `retryable` value means
starting a new command, after discarding any output from the failed invocation.
Query output and memory exhaustion use `adjust_budget` and `retryable: false`.

A successful command emits `kind: "completion"`, `terminal: true`, and
`completion: "complete"` after stdout has been flushed. It includes `command`,
`scope`, `stop_reason`, `has_more`, and `output_bytes`. Inspect, scan, and query include
the actual snapshot in `bindings`, for example
`[{"alias":"t","table_version":"42"}]`; help and version use an empty array.
Scan and query also report `rows` and `batches`. Versions and counts are decimal strings.

Scope is `inspect`, `scan`, `query`, `help`, or `version`, except that any scan with
`--limit` uses `limited_scan`. Reaching the limit, including zero, reports
`stop_reason: "row_limit"` and `has_more: null`: it does not prove that another
row exists. Otherwise `stop_reason` is `end_of_input` and `has_more` is false.
For queries, end of input means the complete SQL result, including any SQL
`LIMIT`. Output ceilings cannot produce a successful partial query completion.

Consumers must check exit status 0, valid stdout, and the completion record.
Records are at most 8 KiB. Ignore additional fields and JSON key order; reject
unsupported `format_version` values. Incompatible field meanings or types
require a new version. This opt-in format replaces the former unversioned
JSON stderr object; plain text remains the default.

## Target filters

These filters apply only to the default human diagnostics.

Set `RUST_LOG` to comma-separated target directives, for example
`delta_arrow_reader=debug,delta_arrow_reader_cli=warn`. A bare level sets the
default; otherwise unmatched targets use WARN. The `delta_arrow_reader` prefix
also matches the CLI target; a more specific CLI directive overrides it.
Directives use `tracing-subscriber`'s `Targets` filter, without dynamic
span-value or regex matching.

Only `delta_arrow_reader`, `delta_arrow_reader_cli`, and their `::` subtargets
are allowed. Explicit dependency directives do not enable other targets.
An unset or empty value uses WARN. An invalid value emits a safe warning and
falls back to WARN without printing the value.

## Failure fields

Core failures retain their operation identity, phase, code, reason, and
available source category. CLI-owned failures use the same `error_phase`,
`error_code`, and `error_reason` fields. For example:

```text
ERROR delta_arrow_reader_cli: CLI command failed event="cli.failed" operation="cli_command" outcome="failed" error_phase="configuration" error_code="invalid_cli_argument" error_reason="argument_parse_failed" argument_kind=UnknownArgument
```

The [tracing reference](tracing.md) defines core event fields.
Diagnostic text layout, field order, and human messages are not a machine
interface; scripts should use machine diagnostics and the exit status.

## Diagnostic codes and output failures

| CLI phase | Code | Meaning |
| --- | --- | --- |
| `configuration` | `invalid_cli_argument` | Invalid command, argument, or option value |
| `configuration` | `invalid_input_json` | Invalid JSON structure, encoding, duplicate key, or size |
| `configuration` | `input_file_io` | Input file could not be opened or read |
| `configuration` | `invalid_storage_authorization` | Explicit credential source is incomplete, conflicting, or unsupported |
| `configuration` | `invalid_configuration` | A native execution setting is outside its allowed range |
| `execution` | `runtime_initialization` | Runtime creation failed or reader execution panicked |
| `execution` | `output_write` | stdout could not be written or flushed |
| `execution` | `arrow_ipc` | Arrow IPC encoding failed |
| `configuration` | `invalid_sql_input` | SQL file is malformed, oversized, or contains a disallowed statement |
| `configuration` | `unsupported_uc_query` | Unity Catalog SQL queries are not yet supported |
| `datafusion` | `sql_initialization`, `sql_planning`, `sql_execution` | SQL runtime, planning, or execution failed |
| `execution` | `output_limit` | Complete query result exceeds its row or serialized-byte ceiling |
| `execution` | `command_cancelled` | A local SQL query was interrupted |
| `configuration` | `invalid_uc_reference` | Invalid UC name/profile, or a profile on a direct path |
| `configuration` | `invalid_uc_storage_options` | Unsupported, conflicting, or duplicate region options |
| `configuration` | `uc_s3_region_required` | S3 needs an explicit region |
| `configuration` | `databricks_cli_missing` | Databricks CLI is unavailable on PATH |
| `execution` | `databricks_cli_failed`, `databricks_cli_timeout` | Child process failed or exceeded its timeout |
| `execution` | `uc_table_request_failed`, `uc_credential_request_failed` | Metadata or credential request failed; check login and permissions |
| `execution` | `invalid_databricks_response`, `unsupported_uc_table` | Response is malformed, oversized, or unsupported |
| `execution` | `uc_credentials_expired` | An initial or renewed grant is already expired |
| `execution` | `uc_cancelled` | A UC command was interrupted |

Diagnostics omit argument values, file paths, URLs, input contents, credential
names and values, and dependency messages. Argument failures include the
parser's `argument_kind`. JSON failures include `json_kind` (`Syntax`, `Data`, or
`Eof`), `json_line`, and `json_column` in human mode (`line` and `column` in
machine mode). Oversized input and predicate validation
use fixed reasons, such as `input_file_too_large` or
`predicate_depth_limit_exceeded`. Serde schema failures share `Data`; their
messages are not parsed or printed. Line and column record where parsing stopped;
field guidance may identify another problem in an input with multiple errors.
I/O failures retain `io_kind` and an OS error
number when available. Arrow failures use the core's safe source classifier.

Provider configuration rejected during table loading is a reader failure
(status 1), even if the option came from a valid local JSON file.

The complete inspection JSON is built before writing. Failures before output
leave stdout empty. Consumers must check the process status and discard output
from any unsuccessful invocation, including partial output after a write
failure. Status 3 is preserved even when stderr cannot be written.

## Threads, cancellation, and signals

Direct-path commands run in one process. UC commands also start bounded
Databricks CLI subprocesses. The CLI drives its asynchronous runtime on
the main thread and ignores `TOKIO_WORKER_THREADS`. Scans and queries use a separate output
thread so a blocked stdout pipe does not stall network reads. The output thread
finishes writing and flushing each batch before requesting another. The core
uses background threads for blocking work. Runtime shutdown does not wait for
unfinished blocking work, including core engine cleanup.

An unrecoverable panic, including failure to start a reader worker, terminates
the process with status 1 and a synchronous diagnostic with code
`runtime_initialization` and reason `process_panicked`. Panic payloads and
backtraces are omitted. Runtime setup failures have their own reason and safe
I/O category.

UC commands handle SIGINT/SIGTERM during authorization, metadata loading, and
scanning, terminate any pending Databricks child, and exit 130/143. In text mode,
cancellation emits an INFO `cli.cancelled` event with code `uc_cancelled`,
visible with `RUST_LOG=info`. Machine mode emits a terminal diagnostic with
that code, including with `RUST_LOG=off`. Cancellation remains responsive
during stdout writes. On Windows, Ctrl+C exits 130.

Local SQL queries use the same signal handling during table loading and
execution, with code `command_cancelled`. SQL file reading and parsing happen
before the asynchronous signal handler starts.

On Unix, UC scans and local SQL queries also detect a closed stdout pipe while
waiting for credentials or storage. They terminate pending work and exit 3 without finishing
the Arrow stream. On other platforms, a closed pipe is detected at the next
stdout write; a pending credential command still has its 30-second timeout.

For direct-path inspect and scan commands outside PID 1, Linux signals keep their normal
termination behavior: a killed process is unsuccessful and need not emit an
error event. This applies while loading,
waiting for a batch, or writing to a blocked pipe. A parent that cancels the
child must also wait for it to exit and reap it.

Machine mode attempts one terminal record for handled errors and panics on
the main or worker threads. Unhandled signals, aborts, SIGKILL, allocation failure, and
unwritable stderr cannot guarantee a terminal record. Missing completion is
never evidence of success.
