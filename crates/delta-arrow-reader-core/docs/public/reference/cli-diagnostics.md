# CLI diagnostics reference

Use this reference when integrating the CLI into a service or configuring
specific log targets. For ordinary troubleshooting, see
[Fix a failed command](../cli-recovery.md).
For a failed Databricks-backed read, start with
[Unity Catalog troubleshooting](../unity-catalog.md#if-a-command-fails).

## Machine diagnostics

Use the global option `--diagnostics=json` before or after the command:

```sh
dar --diagnostics=json inspect /path/to/table --view summary > snapshot.json 2> diagnostics.jsonl
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
  "format_version": 2, "kind": "diagnostic", "terminal": true,
  "phase": "configuration", "code": "invalid_cli_argument",
  "reason": "argument_parse_failed", "source_kind": null,
  "argument_kind": "UnknownArgument",
  "retryable": false, "recovery": "fix_input"
}
```

Core failures retain their typed phase, code, reason, and source category.
`source_kind` is null when absent. Safe optional fields include `argument_kind`,
`json_kind`, JSON `line` and `column`, `io_kind`, and `os_error`. No raw dependency
errors, arbitrary input values, paths, credentials, or captured child output are included.
Diagnostic version 2 removes the fixed `message` from version 1. Use the
classification fields for decisions.

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

With `--env-file`, credential validation identifies the supplying file when its
origin is known. Credentials in the optional JSON configuration point to
`--storage-options-file`; an incomplete or conflicting file identity points to
`--env-file`, with a lookup for that storage service's supported variables.
Manifest calls identify the corresponding authorization reference in `field_path`.
Neither form includes credential contents.

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
also include correction fields. For unknown columns, run
`inspect TABLE --view summary --column-prefix PREFIX` with the same authorization
and `--table-version N`. Use the returned names to correct `--column` or the
predicate file. Query a type with `--column NAME` instead of `--column-prefix`.

Scalar type mismatches include `expected`, `supplied`, and the failing scalar's
`field_path`. For example, an `int64` literal compared with an `Int32` column
reports `expected: "int32"`, `supplied: "int64"`, and
`field_path: "/value/type"`. Nested comparisons include their path from the root,
such as `/args/1/arg/value/type`. Decimal precision/scale and fixed binary size
mismatches identify the relevant parameter. Timezone mismatches identify
`/value/timezone` and report whether a timezone is set, without its text.
Unsupported column types get general guidance without nested names or metadata.
`expected` and `supplied` are type descriptions, not a replacement for the input
schema.

These planning errors retain the core's `code` and `reason`, with exit status 1
and `recovery: "fix_input"`. Errors without a known predicate location omit
`field_path`. Text mode adds an ERROR `cli.input_correction` event after the core
failure event; machine mode keeps one terminal record.

When `databricks_cli_missing` occurs, `hint` directs the caller to make the
`databricks` executable available on PATH and retry with the same
`--databricks-profile`. The diagnostic does not include the PATH or profile value.

`recovery` uses `fix_input` for invalid input, `authorize` for unauthenticated
sources, `request_access` for permission failures, `retry` for known transient
I/O failures, or `change_request` for unsupported operations. Unclassified
failures use `unknown` and `retryable: null`. A true `retryable` value means
starting a new command, after discarding any output from the failed invocation.

Query byte and row exhaustion use `adjust_budget`, identifying
`--max-output-bytes` or `--max-rows`. Explicitly increase the applicable budget
and keep the same SQL, authorization, table bindings, and snapshot versions.
For SQL planning or result errors after all snapshots load, the diagnostic includes `bindings`
with the validated aliases and actual versions. Set each manifest entry's
`table_version` from those bindings before retrying. For positional input, set
`--table-version` from the `t` binding. Snapshot-loading failures and cancellation omit these fields. A retry without pinned versions can read newer commits.
For large JSON results, use `--output PATH` and an explicit byte budget when
needed, then require exit 0 and the command's completion record. Adding
SQL `LIMIT` changes the result and is not a repair for the same request.

Preview schema exhaustion uses `change_request`: narrow the selected columns
or explicitly increase the byte budget. First-row exhaustion uses
`adjust_budget` and identifies `--max-output-bytes`; keep the projection,
predicate, row limit, authorization, snapshot, and deadline when retrying.
Preview deadlines and query memory exhaustion also use `adjust_budget`.
These resource failures set `retryable: false`.

A successful command emits `kind: "completion"` after stdout has been flushed.
It includes `command` and `output_bytes`. For stdout data, inspect, scan, preview, and query include the actual snapshot
in `bindings`, for example
`[{"alias":"t","table_version":"42"}]`. Queries using `--tables-file` include
each manifest alias and its resolved version in declaration order. Other version 1
commands use an empty array. Explain keeps its bindings in stdout.
Stdout scan, preview, and query also report `rows`. Versions and counts are decimal strings.

`preview` and `query --format json` to stdout use completion `format_version: 2`, containing only the
version, kind, command, bindings, rows, and output bytes. The completion kind
marks terminal success for the requested command. For a query, check
`completion: "complete"` and `scope: "query"` in stdout. A preview instead
reports `completion: "sample"`, `stop_reason`, and `has_more` there. Check
stdout bindings and row count in both cases.

`scan` and `query` with `--output` use completion version 3. It contains only
`format_version`, `kind`, `command`, and `output_bytes`, which counts the stdout
receipt. The default v2 receipt names one complete result file. With explicit
`--manifest`, receipt v1 also references the provenance file containing counts
and bindings. A receipt failure can leave published files;
[check the output mode's recovery steps](../cli-automation.md#read-a-saved-result)
before retrying. `artifact_io` diagnostics identify failed staging, writes or
publication, without printing paths or OS messages. Existing files are preserved.

`explain` also uses completion version 3. Its byte count covers the query-plan
document on stdout; check bindings there. There are no result row counts.

Other commands and Arrow stdout queries use version 1, which also includes
`terminal: true`, `completion: "complete"`, `scope`, `stop_reason`, and `has_more`,
plus `batches` for scan and query.

In version 1, scope equals the command, except that any scan with `--limit`
uses `limited_scan`. Reaching the limit, including zero, reports
`stop_reason: "row_limit"` and `has_more: null`: it does not prove that another
row exists. Other version 1 completions use `stop_reason: "end_of_input"` and
`has_more: false`. In all versions, a query completes only after the full SQL
result, including any SQL `LIMIT`. Output ceilings cannot produce a successful
partial query completion.

Consumers must check exit status 0, valid stdout, and the completion record.
`dar capabilities --schema diagnostic --full` and
`dar capabilities --schema completion --full` return the JSON Schemas for these
stderr records; extract each response's `schema` field
for validation. Both commands work offline.
Records are at most 8 KiB. Ignore additional fields and JSON key order; reject
unsupported `format_version` values for each record kind. Incompatible field meanings or types
require a new version.

<span id="json-stderr-migration"></span>

This opt-in format replaces the former unversioned
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
| `execution` | `runtime_initialization` | Runtime or worker setup failed, a worker panicked, or discovery exceeded its deadline |
| `execution` | `output_write` | Stdout or completion output could not be written or flushed |
| `execution` | `artifact_io` | A result file, completion manifest, or artifact receipt could not be written or published |
| `execution` | `arrow_ipc` | Arrow IPC encoding failed |
| `execution` | `json_output` | Typed JSON encoding failed |
| `execution` | `unsupported_json_type` | The selected schema contains a type unsupported by typed JSON, even if the result is empty |
| `execution` | `deadline_exceeded` | A table command exceeded its whole-command deadline |
| `configuration` | `invalid_sql_input` | SQL file is malformed, oversized, or contains a disallowed statement |
| `datafusion` | `sql_initialization`, `sql_planning`, `sql_execution` | SQL runtime, planning, or execution failed |
| `execution` | `output_limit` | A configured output ceiling was exceeded; includes a preview schema or first row that cannot fit |
| `execution` | `command_cancelled` | A command was interrupted; prepared UC inspect/scan/query/explain requests use `uc_cancelled` |
| `configuration` | `invalid_uc_reference` | Invalid UC name/profile, or a profile on a direct path |
| `configuration` | `invalid_uc_storage_options` | Unsupported, conflicting, or duplicate region options |
| `configuration` | `uc_s3_region_required` | S3 needs an explicit region |
| `configuration` | `databricks_cli_missing` | Databricks CLI is unavailable on PATH |
| `execution` | `databricks_cli_failed`, `databricks_cli_timeout` | Child process failed or exceeded its timeout |
| `execution` | `uc_table_request_failed`, `uc_credential_request_failed` | Metadata or credential request failed; check login and permissions |
| `execution` | `invalid_databricks_response`, `unsupported_uc_table` | Response is malformed, oversized, or unsupported |
| `execution` | `uc_credentials_expired` | An initial or renewed grant is already expired |
| `execution` | `uc_cancelled` | A UC inspect, scan, query, or explain command was interrupted |

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

Inspection and typed JSON results are built before writing. Failures before output
leave stdout empty. A later preview row exceeding the byte budget returns the
preceding sample with `stop_reason: "byte_limit"` and `has_more: true`. Query
overflow fails; it cannot return a successful prefix.
Consumers must check the process status and discard output
from any unsuccessful invocation, including partial output after a write
failure. Status 3 is preserved even when stderr cannot be written.

## Threads, cancellation, and signals

Direct-path commands run in one process. UC commands also start bounded
Databricks CLI subprocesses. The CLI ignores `TOKIO_WORKER_THREADS`. Inspect,
scan, preview, and query run on a worker supervised by the main thread, so
synchronous input and credential reads cannot block deadlines or signal handling.
Help, version, and capabilities output also run on a supervised worker.

Scan, preview, and query write results on a separate output thread. Arrow output
flushes each batch before requesting another; typed JSON buffers the result
within its byte budget before writing. The core uses background threads for
blocking work. Runtime shutdown does not wait for unfinished blocking work,
including core engine cleanup.

An unrecoverable panic, including failure to start a reader worker, terminates
the process with status 1 and a synchronous diagnostic with code
`runtime_initialization` and reason `process_panicked`. Panic payloads and
backtraces are omitted. Runtime setup failures have their own reason and safe
I/O category.

All commands handle Unix SIGINT/SIGTERM and exit 130/143, including at Linux PID 1.
On Windows, Ctrl+C exits 130. Table commands remain cancellable from input loading
through completion reporting, including synchronous credential reads and blocked
output. Cancellation normally uses code `command_cancelled`; UC inspect, scan,
query and explain retain `uc_cancelled` once their UC requests are prepared. In text mode,
these are INFO `cli.cancelled` events, visible with `RUST_LOG=info`. Machine mode
emits the terminal diagnostic even with `RUST_LOG=off`.

`--deadline-ms N` sets a positive whole-command budget in milliseconds, covering
input files, authorization, snapshot loading, planning, execution, stdout, and
completion reporting. Preview, explain and summary inspection default to 30000 ms; query
and file-output scans default to 300000 ms. Full inspection and stdout scan retain no default deadline.
Set one explicitly for unattended use. Expiry exits 1 with `deadline_exceeded`.
Help, version, and every capabilities view instead have a fixed 5000 ms deadline
and use `discovery_deadline_exceeded`.

Table commands allow at most one additional second for cleanup, then all commands
allow at most one second for failure reporting. Pending initial or renewal
Databricks children started by the CLI are terminated and reaped during cleanup.
The CLI does not wait indefinitely for blocked worker threads during shutdown.
For example, `dar --diagnostics=json scan ./table --table-version 0 --deadline-ms 1000`
can leave partial stdout after expiry. Discard it; any permitted retry must keep
the request, snapshot, authorization, and other limits, with a larger deadline
chosen explicitly.

On Unix, table commands detect a closed stdout pipe while waiting for credentials
or storage and exit 3 without a success completion. Other platforms detect the
failure at the next write; the selected total deadline still applies. A parent
that cancels the CLI must also wait for it to exit and reap it. OS-uninterruptible
IO and uncatchable termination cannot guarantee timely cleanup.

Machine mode attempts one terminal record for handled errors and panics on
the main or worker threads. Unhandled signals, aborts, SIGKILL, allocation failure, and
unwritable stderr cannot guarantee a terminal record. Missing completion is
never evidence of success.
