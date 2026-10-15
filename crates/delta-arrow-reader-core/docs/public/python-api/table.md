# DeltaTable

Open a table once, then reuse it for queries. The constructor below opens it;
[to_reader()](#delta_arrow_reader.DeltaTable.to_reader) selects rows and columns.
Use [schema](#delta_arrow_reader.DeltaTable.schema) to see the available columns,
and [refresh()](#delta_arrow_reader.DeltaTable.refresh) when you want newer data.

For a complete example, use the [Python quickstart](../python.md). For SQL,
register the loaded table with a [Session](session.md). Opening-time settings
are explained in [Configuration](../configuration.md).

::: delta_arrow_reader.DeltaTable
