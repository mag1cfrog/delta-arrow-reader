# ScanExecutionOptions

Leave these settings at their defaults for your first query. When you need a
file-read limit or different buffering, [Configuration](../configuration.md)
explains which setting to try and its tradeoff.

Pass an options object to `DeltaTable` for direct-scan defaults, to `to_reader()`
for one scan, or to `Session.register_table()` for SQL. A supplied object replaces
the complete set of options at that scope. The reference below lists exact
names, types, defaults, and validation rules.

::: delta_arrow_reader.ScanExecutionOptions
