# Python API

Use this section to look up a method or parameter. For your first query, follow
the [Python quickstart](../python.md); it starts with reading ten rows and then
adds column selection, filters, and SQL.

## Find the operation you need

| Task | API |
| --- | --- |
| Open a table and inspect its columns | [DeltaTable](table.md) |
| Select rows and columns as PyArrow batches | [DeltaTable.to_reader()](table.md#delta_arrow_reader.DeltaTable.to_reader) |
| Run SQL over one or more tables | [Session](session.md) |
| Read a newer table version | [DeltaTable.refresh()](table.md#delta_arrow_reader.DeltaTable.refresh) |
| Limit concurrent file reads or buffering | [ScanExecutionOptions](options.md) |
| Handle a failure or export an Arrow stream | [Streams and errors](streams-and-errors.md) |

## Choose settings after the first query

Start with the defaults. [Configuration](../configuration.md) explains the cost
of opening a table and the tradeoffs of changing resource limits. The API pages
below give exact signatures, accepted values, defaults, and exceptions.
