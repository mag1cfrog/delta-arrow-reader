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

`recovery` uses `fix_input` for invalid input, `authorize` for unauthenticated
sources, `request_access` for permission failures, `retry` for known transient
I/O failures, or `change_request` for unsupported operations. Unclassified
failures use `unknown` and `retryable: null`. A true `retryable` value means
starting a new command, after discarding any output from the failed invocation.

A successful command emits `kind: "completion"`, `terminal: true`, and
`completion: "complete"` after stdout has been flushed. It includes `command`,
`scope`, `stop_reason`, `has_more`, and `output_bytes`. Inspect and scan include
the actual snapshot in `bindings`, for example
`[{"alias":"t","table_version":"42"}]`; help and version use an empty array.
Scan also reports `rows` and `batches`. Versions and counts are decimal strings.

Scope is `inspect`, `scan`, `help`, or `version`, except that any scan with
`--limit` uses `limited_scan`. Reaching the limit, including zero, reports
`stop_reason: "row_limit"` and `has_more: null`: it does not prove that another
row exists. Otherwise `stop_reason` is `end_of_input` and `has_more` is false.

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

Diagnostics omit argument values, file paths, URLs, input contents, credential
names and values, and dependency messages. Argument failures include the
parser's `argument_kind`. JSON failures include `json_kind` (`Syntax`, `Data`, or
`Eof`), `json_line`, and `json_column` in human mode (`line` and `column` in
machine mode). Oversized input and predicate validation
use fixed reasons, such as `input_file_too_large` or
`predicate_depth_limit_exceeded`. Serde schema failures share `Data`; their
messages are not parsed or printed. I/O failures retain `io_kind` and an OS error
number when available. Arrow failures use the core's safe source classifier.

Provider configuration rejected during table loading is a reader failure
(status 1), even if the option came from a valid local JSON file.

The complete inspection JSON is built before writing. Failures before output
leave stdout empty. Consumers must check the process status and discard output
from any unsuccessful invocation, including partial output after a write
failure. Status 3 is preserved even when stderr cannot be written.

## Threads, cancellation, and signals

Direct-path commands run in one process. UC inspection also starts bounded
Databricks CLI subprocesses. The CLI drives its asynchronous runtime on
the main thread and ignores `TOKIO_WORKER_THREADS`. Scans use a separate output
thread so a blocked stdout pipe does not stall network reads. The output thread
finishes writing and flushing each batch before requesting another. The core
uses background threads for blocking work. Runtime shutdown does not wait for
unfinished blocking work, including core engine cleanup.

An unrecoverable panic, including failure to start a reader worker, terminates
the process with status 1 and a synchronous diagnostic with code
`runtime_initialization` and reason `process_panicked`. Panic payloads and
backtraces are omitted. Runtime setup failures have their own reason and safe
I/O category.

UC inspection handles SIGINT/SIGTERM while resolving and loading metadata,
terminates any pending Databricks child, and exits 130/143. In text mode,
cancellation emits an INFO `cli.cancelled` event with code `uc_cancelled`,
visible with `RUST_LOG=info`. Machine mode emits a terminal diagnostic with
that code, including with `RUST_LOG=off`. Cancellation remains responsive
during stdout writes. On Windows, Ctrl+C exits 130.

For direct-path commands outside PID 1, Linux signals keep their normal
termination behavior: a killed process is unsuccessful and need not emit an
error event. This applies while loading,
waiting for a batch, or writing to a blocked pipe. A parent that cancels the
child must also wait for it to exit and reap it.

Machine mode attempts one terminal record for handled errors and panics on
the main or worker threads. Unhandled signals, aborts, SIGKILL, allocation failure, and
unwritable stderr cannot guarantee a terminal record. Missing completion is
never evidence of success.
