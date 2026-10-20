# Streams and errors

For normal PyArrow reads, use [DeltaTable.to_reader()](table.md#delta_arrow_reader.DeltaTable.to_reader)
and close the reader with a `with` block. This page covers failures and the
lower-level interface for handing results to another Arrow library.

## Handle a failed read

Opening or planning can raise `DeltaReaderError`. Invalid Python arguments may
raise built-in exceptions instead. While consuming rows, catch PyArrow errors
around iteration or `read_all()` as well.

::: delta_arrow_reader.DeltaReaderError

## Log table loading and refresh

`DeltaTable(...)` and `table.refresh()` forward core tracing events to the
`delta_arrow_reader` Python logger. Configure logging in your application:

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
changes take effect on later events, including calls on an existing table.

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

Each record has `dar_target` and a `dar_fields` dictionary containing the core
event's own fields. Lifecycle records include `event`, `operation_id`, and the
known snapshot context. Failures include `error_phase`, `error_code`,
`error_reason`, and `error_source_kind` when available. Numbers and booleans keep
their types. Each dictionary contains at most 64 fields; text values are limited
to 1,024 characters. See the [core tracing reference](../reference/tracing.md)
for event meanings and redaction rules.

A load or refresh failure still raises `DeltaReaderError`; converting that error
does not emit another failure record. Argument validation, scan planning, SQL,
and deferred batch consumption are not yet forwarded by the Python bridge.

Handlers run synchronously on the calling thread or a reader worker. They must
not start reader work or wait for another thread to finish reader work. Calls
that start reader work or close/export a `RecordBatchStream` raise `RuntimeError`
inside a callback. Reading an exported reader raises a PyArrow error instead;
the rejected read leaves it usable. Ordinary handler
exceptions are ignored; control-flow exceptions such as `KeyboardInterrupt` and
`SystemExit` propagate to the waiting caller. Logging callbacks use PyO3's best-effort
interpreter shutdown check; delivery during finalization is not guaranteed.
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
