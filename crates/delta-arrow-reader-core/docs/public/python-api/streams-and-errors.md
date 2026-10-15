# Streams and errors

Most callers can use [DeltaTable.to_reader()][delta_arrow_reader.DeltaTable.to_reader]
to get a PyArrow reader. `RecordBatchStream` is the Arrow exporter returned by
`DeltaTable.scan()`.

::: delta_arrow_reader.RecordBatchStream

::: delta_arrow_reader.DeltaReaderError
