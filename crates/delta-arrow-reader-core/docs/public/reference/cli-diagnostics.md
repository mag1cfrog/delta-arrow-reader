# CLI diagnostics reference

Use this reference when integrating the CLI into a service or configuring
specific log targets. For ordinary troubleshooting, see
[Errors and process status](../cli.md#errors-and-process-status).

## Target filters

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
interface; scripts should use the exit status and stdout format.

## JSON stderr migration

Text diagnostics replace the JSON stderr object containing `phase`, `code`,
and `message`. Inspection JSON, Arrow IPC, and exit statuses are unchanged.

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

Diagnostics omit argument values, file paths, URLs, input contents, credential
names and values, and dependency messages. Argument failures include the
parser's `argument_kind`. JSON failures include `json_kind` (`Syntax`, `Data`, or
`Eof`), `json_line`, and `json_column`. Oversized input and predicate validation
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

Each command runs in one process. The CLI drives its asynchronous runtime on
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
Outside PID 1, Linux signals keep their normal termination behavior: a killed
process is unsuccessful and need not emit an error event. This applies while loading,
waiting for a batch, or writing to a blocked pipe. A parent that cancels the
child must also wait for it to exit and reap it.
