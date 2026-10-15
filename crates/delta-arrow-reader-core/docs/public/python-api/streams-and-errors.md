# Streams and errors

For normal PyArrow reads, use [DeltaTable.to_reader()](table.md#delta_arrow_reader.DeltaTable.to_reader)
and close the reader with a `with` block. This page covers failures and the
lower-level interface for handing results to another Arrow library.

## Handle a failed read

Opening or planning can raise `DeltaReaderError`. Invalid Python arguments may
raise built-in exceptions instead. While consuming rows, catch PyArrow errors
around iteration or `read_all()` as well.

::: delta_arrow_reader.DeltaReaderError

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
