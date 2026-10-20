# Enable Python logging

Use Python's `logging` module to see what the reader is doing. Logging is
optional: failed reads still raise exceptions when it is disabled.

## Show progress in a script

Configure logging before opening the table:

```python
import logging
from delta_arrow_reader import DeltaTable

logging.basicConfig(level=logging.INFO)

table = DeltaTable("/path/to/table")
with table.to_reader(limit=10) as reader:
    result = reader.read_all()
```

Replace `/path/to/table` with your table's directory. This reads up to ten rows
and prints INFO, WARNING, and ERROR messages to the terminal. For example,
`table_load.started` means the reader began opening the table;
`scan_execution.completed` means this read finished.

In a notebook or an application that already configures logging,
`basicConfig()` may leave the existing settings unchanged. Set the reader's
level in that configuration:

```python
logging.getLogger("delta_arrow_reader").setLevel(logging.INFO)
```

The application's log output must also allow INFO messages. The reader uses
the application's logging configuration.

## Choose how much to log

| Python level | What you see |
| --- | --- |
| `logging.ERROR` | Failed operations |
| `logging.WARNING` | Failures and problems the reader can recover from |
| `logging.INFO` | Operation progress as well as warnings and failures |
| `logging.DEBUG` | Internal planning and execution details |

Change the level to `logging.DEBUG` when investigating a problem. Level changes
apply to later events, including reads from an existing table.

## When a read fails

Handle the exception from opening, planning, or reading batches. Logging does
not change whether the call succeeds. See [Handle errors](errors.md) for the
exceptions to catch and their error attributes.

## Customize log records

If you need structured fields such as error codes in your log output, or want
to write a custom handler, use [Log records and handlers](../reference/python-logging.md).
