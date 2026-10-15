# Streams and errors

Most callers can use [DeltaTable.to_reader()][delta_arrow_reader.DeltaTable.to_reader]
to get a PyArrow reader. `RecordBatchStream` is the Arrow exporter returned by
`DeltaTable.scan()`.

::: delta_arrow_reader.RecordBatchStream

::: delta_arrow_reader.DeltaReaderError

## Threads and interruption

Loading a table, planning a scan, and reading batches release the GIL so other
Python threads can run. During loading or planning on Python's main thread,
Ctrl+C raises `KeyboardInterrupt` and cancels the pending operation. Interruption
during a batch read reaches the caller as a PyArrow exception.

Synchronous Kernel work already running may still finish after interruption;
cancellation does not stop it immediately.
