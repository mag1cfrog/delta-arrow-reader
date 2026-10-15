# Python API

Use `DeltaTable` to read a Delta table as PyArrow batches. For SQL, register the
table with a `Session`, then run your query. The [Python quickstart](../python.md)
walks through both approaches with a sample table.

| API | Use it to |
| --- | --- |
| [DeltaTable](table.md) | Open a table, inspect its schema, select columns and filter rows. |
| [Session](session.md) | Register tables and query them with SQL. |
| [ScanExecutionOptions](options.md) | Control how much work a query runs at once. |
| [Streams and errors](streams-and-errors.md) | Work with Arrow stream exports and reader errors. |

For help choosing initialization and resource settings, start with
[Configuration](../configuration.md). These API pages list the available
parameters, defaults and return types.
