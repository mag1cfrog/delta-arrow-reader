# Session

A session gives loaded tables names that SQL queries can use. Start with
[register_table()](#delta_arrow_reader.Session.register_table), then call
[sql()](#delta_arrow_reader.Session.sql) to get a PyArrow reader for the result.

The [SQL quickstart](../python.md#query-with-sql) shows both steps. This reference
covers accepted table names, query restrictions, scan settings, and errors.

::: delta_arrow_reader.Session
