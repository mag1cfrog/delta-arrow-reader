# Python log records and handlers

Use this reference when formatting structured records or writing a logging
handler. It also defines TRACE mapping, event ownership, and delivery behavior.
To turn on ordinary application logs, start with
[Enable Python logging](../python-api/logging.md).

## Configure structured records

This example sends INFO and higher levels to stderr, including each event's
structured fields:

```python
import logging
from delta_arrow_reader import DeltaTable

logger = logging.getLogger("delta_arrow_reader")
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(levelname)s %(message)s %(dar_fields)s"))
logger.addHandler(handler)
logger.setLevel(logging.INFO)
logger.propagate = False

table = DeltaTable("path/to/table")
latest = table.refresh()
```

If your application already configures logging, use its handlers. The package
adds only a `NullHandler` to its own logger, so unconfigured applications remain
quiet even when they catch reader exceptions. It does not configure the root
logger or add console or file output. Records follow normal Python propagation.
Changes to logger and handler levels apply to later events, including those
from existing tables, sessions, and partially consumed readers.

## Level mapping

| Core level | Python level |
| --- | --- |
| ERROR | ERROR (40) |
| WARN | WARNING (30) |
| INFO | INFO (20) |
| DEBUG | DEBUG (10) |
| TRACE | 5, below DEBUG |

To include TRACE, set the logger's level to `5` and allow level 5 in its
handlers. The package does not register a global name for that numeric level.

## Read a log record

Each record has `dar_target`, the Rust event's target, and `dar_fields`, a
dictionary of the event's fields. Numbers and booleans keep their types.
Dictionaries contain at most 64 fields; text values are limited to 1,024
characters.

Core lifecycle records identify the operation and available snapshot context.
Failures include `error_phase`, `error_code`, `error_reason`, and, when
available, `error_source_kind`. The [tracing reference](tracing.md)
defines these fields, event ownership, levels, and redaction rules.

Execution records start when batches are requested. A completed DataFusion
partition does not establish whole-query success, and independent partitions
can each fail. Check the result of the Python call or Arrow read. Converting a
core failure into an exception, or replaying a failed Arrow read, does not add
another failure record.

## Exceptions and diagnostic events

For the exceptions to catch in application code, see
[Handle Python errors](../python-api/errors.md).

Before row consumption, the binding converts core errors into Python exceptions
with the core's `phase`, `code`, `reason`, and safe `source_kind` attributes.
`source_kind` is `None` when the core error has no underlying source. These
attributes remain available when logging is disabled. Core scan-option validation
keeps its `ValueError` type and carries the same attributes. SQL planning errors
also expose `reason` and `source_kind`. Their `cause` attribute contains the
reader's classification when a typed reader error is available, otherwise `None`.
Other binding-owned errors, such as schema export failures, have `reason = None`
and `source_kind = None`; their diagnostic events carry the available details.

During consumption, PyArrow exceptions carry the safe source category and any
typed reader cause in their messages. Arrow's C stream interface transports an
error string, so those exceptions do not have the `DeltaReaderError` attributes.

## Failures reported by the binding

The binding forwards core events and reports failures it owns on the
`delta_arrow_reader_python` target, through the same Python logger. Its records
omit SQL, identifiers, argument values, schema contents, paths, and dependency
messages at every level.

### Binding events

| Event | When it occurs | Available context |
| --- | --- | --- |
| `validation.failed` | A core predicate-column or scan-option helper returns an error without logging | Core phase, code, reason, source category, and a fixed `argument`: `filters`, `execution_options`, or `target_partitions` |
| `sql_planning.failed` | SQL logical planning fails | `sql_parse` for syntax, `datafusion_plan` for planning failures such as an unknown table, or `schema` for unknown columns |
| `sql_execution.failed` | A SQL operator outside the core fails, including physical planning on the first batch request | Safe source kinds such as `arrow_cast` or `divide_by_zero`; the Arrow error retains `phase=datafusion code=sql_execution` |
| `schema_export.failed` | Conversion to the Arrow schema interface fails | `arrow_schema_contains_nul` for a NUL byte in a field name or timestamp timezone; otherwise `arrow_schema_export_failed` and the available Arrow source category |
| `runtime_creation.failed` | A reader runtime cannot be created | `runtime` (`reader` or `SQL`), source category, and `error_os_code` when supplied by the OS |

Each failure in this table emits one ERROR. Typed core failures are forwarded
without another ERROR. SQL planning success means a reader is ready; execution
begins when the consumer requests batches.

SQL source classification uses `multiple_errors` for collections, `unknown`
for unrecognized sources, and `source_chain_limit` after 16 links. An I/O
failure can also have an unknown category; its OS code can still distinguish
causes such as exhausted file descriptors.

Invalid or duplicate SQL table names use the core code
`datafusion_adapter` in both the exception and event. The exception's `reason`
distinguishes an empty name, an invalid name, and failed registration.

Schema export keeps the existing exception's `phase=schema`,
`code=schema_conversion`, and message. Runtime creation keeps its
`RuntimeError` message.

### Exceptions without another event

| Boundary | Behavior |
| --- | --- |
| Python argument types, ranges, filter literals, and conversions such as `__fspath__` | Preserve the Python exception without logging caller values |
| Requested Arrow schema, repeated export, closed or unavailable exporter, and unsupported recursive reader calls | Preserve the protocol or state exception; a rejected schema request leaves the stream usable |
| Package metadata, imports, capsule allocation, consumer conversion, and attaching exception attributes | Preserve the original Python exception; called core operations retain their own events |
| Signals and handler control-flow exceptions | Preserve interruption and core abandonment/drop behavior without an extra ERROR |

Requested-schema import errors use the fixed `ValueError` message
`invalid requested schema`. Dependency text and detailed Arrow causes are not
exposed. The consumer can inspect or replace the requested schema and retry.

## Write a logging handler

Handlers run synchronously on the calling thread or a reader worker. Keep them
short: do not start reader work or wait for another thread to finish reader
work. The bridge uses a scoped Rust subscriber and preserves other native
extensions' global tracing configuration.

### Exceptions and recursive calls

| Handler behavior | Result |
| --- | --- |
| Raises an ordinary exception | The exception is ignored; the reader result is preserved |
| Raises `KeyboardInterrupt` or `SystemExit` during loading, planning, or synchronous failure reporting | The control-flow exception propagates to the caller |
| Raises a control-flow exception during batch consumption | The Arrow read fails with `phase=execution code=python_signal reason=reader_wait_interrupted`; later reads repeat that safe error without retaining a traceback |
| Raises during an Arrow release callback | The release callback has no return channel for the exception |

Calls that start reader work or close/export a `RecordBatchStream` raise
`RuntimeError` inside a handler. Reading an exported Arrow reader raises a
PyArrow error and leaves the reader usable. Pure validation and schema
inspection preserve their exceptions without recursively forwarding logs.

An Arrow consumer's `close()` can release its stream from a handler. If a read
is active, cleanup waits until its native callback returns, and the current
batch may still be returned. Later reads see a closed stream. Follow the
consumer's threading rules; this behavior does not permit concurrent reads
and closes.

Callbacks use PyO3's best-effort interpreter shutdown check. Delivery during
finalization is not guaranteed.
