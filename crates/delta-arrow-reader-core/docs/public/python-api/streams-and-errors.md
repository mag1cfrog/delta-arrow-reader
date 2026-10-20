# Streams and errors

For normal PyArrow reads, use [DeltaTable.to_reader()](table.md#delta_arrow_reader.DeltaTable.to_reader)
and close the reader with a `with` block. This page covers failures and the
lower-level interface for handing results to another Arrow library.

## Handle a failed read

Opening or planning can raise `DeltaReaderError`. Invalid Python arguments may
raise built-in exceptions instead. While consuming rows, catch PyArrow errors
around iteration or `read_all()` as well.

::: delta_arrow_reader.DeltaReaderError

## Configure diagnostic logging

Table loading, refresh, scan planning and consumption, and SQL table registration
forward core tracing events to the `delta_arrow_reader` Python logger. SQL
planning and operator failures use the same logger. Configure logging in your
application:

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

The package adds a `NullHandler` to its own logger so an unconfigured application
stays quiet, including when it catches a reader exception. It does not configure
the root logger or install console or file handlers. Existing application
handlers receive records through normal propagation. Logger and handler level
changes take effect on later events, including calls on an existing table,
session, or partially consumed reader.

| Core level | Python level |
| --- | --- |
| ERROR | ERROR (40) |
| WARN | WARNING (30) |
| INFO | INFO (20) |
| DEBUG | DEBUG (10) |
| TRACE | 5, below DEBUG |

To include TRACE, use `logging.getLogger("delta_arrow_reader").setLevel(5)`
and allow level 5 in your handlers. The package does not register a global name
for that numeric level.

Each record has `dar_target` and a `dar_fields` dictionary containing the
event's own fields. Core lifecycle records include `event`, `operation_id`, and the
known snapshot context. Failures include `error_phase`, `error_code`,
`error_reason`, and `error_source_kind` when available. Numbers and booleans keep
their types. Each dictionary contains at most 64 fields; text values are limited
to 1,024 characters. See the [core tracing reference](../reference/tracing.md)
for event meanings and redaction rules.

Core failures during loading or planning still raise `DeltaReaderError`.
Failures during Arrow consumption raise the consumer's exception, such as
`pyarrow.ArrowInvalid`. Converting or replaying a failed read does not emit
another failure record. Registration records retain the core's
`error_code = "datafusion_adapter"`, while the Python exception keeps its existing
`code = "sql_registration"`.

SQL logical planning failures emit one ERROR `sql_planning.failed` event on the
`delta_arrow_reader_python` target. Its safe `error_source_kind` distinguishes
syntax errors (`sql_parse`), planning errors such as an unknown table
(`datafusion_plan`), and unknown columns (`schema`). SQL text, identifiers,
literal values, paths, and dependency error messages are omitted at all levels.
Planning success means a reader is ready for consumption; execution starts when
the consumer requests batches.

Ordinary scan execution starts with the first batch request. Its INFO records
report start, completion, or an unfinished stream being dropped. Completion
distinguishes EOF from a row limit. A failure produces one ERROR record with
its plan and execution identities and whether output has already started.
Closing an unused stream produces no execution record. There is no lifecycle
record per batch.

SQL consumption forwards provider planning and partition execution records,
including records from DataFusion workers. Each partition has its own execution
identity linked to its plan. A partition completing does not mean the whole
query succeeded; a later SQL operator can still fail. Independent partitions
can each report a failure.

The binding forwards typed DAR failures without adding another ERROR. Other SQL
execution failures emit one `sql_execution.failed` ERROR on the
`delta_arrow_reader_python` target. The `error_source_kind` field distinguishes
causes such as `arrow_cast` and `divide_by_zero`, while the Arrow exception
keeps `phase=datafusion code=sql_execution`. This includes physical planning
performed on the first batch request. Collections of failures use
`multiple_errors`; unknown sources use `unknown`, and classification beyond 16
links uses `source_chain_limit`. Raw errors and query data remain omitted.

Python argument validation and schema export are not yet forwarded by the
Python bridge.

Handlers run synchronously on the calling thread or a reader worker. They must
not start reader work or wait for another thread to finish reader work. Calls
that start reader work or close/export a `RecordBatchStream` raise `RuntimeError`
inside a callback. Reading an exported reader raises a PyArrow error instead;
the rejected read leaves it usable. An Arrow consumer's `close()` releases its
stream, including when called from a handler. The C stream release callback
cannot report an error. If a read is active, cleanup waits for its native
callback to return; the current batch can still be returned. Further reads see
a closed stream. This does not make Arrow consumers safe for concurrent reads
and closes; follow the consumer's threading rules.

Ordinary handler exceptions are ignored. Control-flow exceptions such as
`KeyboardInterrupt` and `SystemExit` propagate to the waiting caller during
loading or planning. During batch consumption, they interrupt the read with
the Arrow error `phase=execution code=python_signal reason=reader_wait_interrupted`.
The reader repeats that safe error on subsequent reads and does not retain the
Python exception or its traceback. A handler exception raised during an Arrow
release callback has no return channel to the consumer.

Logging callbacks use PyO3's best-effort interpreter shutdown check; delivery
during finalization is not guaranteed.
The bridge uses a scoped Rust subscriber and leaves other native extensions'
global tracing configuration intact.

## Export a scan to an Arrow consumer

Use `DeltaTable.scan()` when a consumer needs the Arrow stream protocol.
`to_reader()` already performs this conversion for PyArrow. Once a stream is
exported, close its consuming reader to release the scan.

::: delta_arrow_reader.RecordBatchStream

## Threads and interruption

<details markdown="1">
<summary>GIL behavior, Ctrl+C, and work already in progress</summary>

Loading a table, planning a scan, and reading batches release the GIL so other
Python threads can run. During loading or planning on Python's main thread,
Ctrl+C raises `KeyboardInterrupt` and cancels the pending operation. Interruption
during a batch read reaches the caller as a PyArrow exception.

Synchronous Kernel work already running may still finish after interruption;
cancellation does not stop it immediately.

</details>
