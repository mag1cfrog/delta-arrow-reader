# Arrow stream interface

<span id="streams-and-errors"></span>

Use this page when passing a scan to another Arrow library. For normal PyArrow
reads, use [DeltaTable.to_reader()](table.md#delta_arrow_reader.DeltaTable.to_reader)
and close the reader with a `with` block. For DataFrames, use the
[Pandas and Polars examples](../python.md#use-pandas-or-polars).
For exceptions from normal reads, see [Handle Python errors](errors.md).

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

## Handle a failed read

<span id="delta_arrow_reader.DeltaReaderError"></span>
<span id="delta_arrow_reader.DeltaReaderError.phase"></span>
<span id="delta_arrow_reader.DeltaReaderError.code"></span>
<span id="delta_arrow_reader.DeltaReaderError.reason"></span>
<span id="delta_arrow_reader.DeltaReaderError.source_kind"></span>

[Handle Python errors](errors.md) covers the exceptions to catch and the
`DeltaReaderError` attributes.

## Configure diagnostic logging

[Enable logging](logging.md) shows how to send reader events to your
application's handlers.
