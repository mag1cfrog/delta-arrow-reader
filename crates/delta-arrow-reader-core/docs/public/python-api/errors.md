# Handle Python errors

A read can fail when opening a table, preparing a query, or consuming batches.
Keep error handling around the whole read:

```python
import pyarrow
from delta_arrow_reader import DeltaReaderError, DeltaTable

try:
    table = DeltaTable("/path/to/table")
    with table.to_reader(limit=10) as reader:
        result = reader.read_all()
except DeltaReaderError as error:
    print(f"Could not prepare the read: {error}")
except pyarrow.ArrowException as error:
    print(f"Could not read batches: {error}")
```

Opening or planning can raise `DeltaReaderError`. Failures during iteration or
`read_all()` use PyArrow exceptions. Invalid Python arguments can raise
`TypeError` or `ValueError`; correct the argument before retrying.

If a read fails after returning some batches, the result is incomplete. The
`with` block closes the reader when the block finishes or an exception occurs.

## Inspect a reader error

`phase` tells you where the read stopped, and `code` identifies the kind of
failure. For example, `phase=table_location` with `code=invalid_table_location`
points to a problem with the supplied path or URL. Check that you are using
the table's directory, which contains `_delta_log`, rather than a data file.
For cloud tables, also check the storage settings for
[S3](../configuration.md#read-from-s3) or [Azure](../configuration.md#read-from-azure).

`reason` and `source_kind` provide more detail when available. These attributes
work even when logging is disabled; you can include them when reporting a
problem.

For SQL planning failures, `source_kind` distinguishes syntax (`sql_parse`),
schema (`schema`), and other planning failures (`datafusion_plan`). Unknown or
ambiguous columns use `reason=column_not_found` or `reason=ambiguous_column`.
If SQL wraps a reader failure, `cause` contains its `phase`, `code`, `reason`,
and `source_kind`; otherwise `cause` is `None`.

PyArrow exceptions raised during consumption include the safe source category
and any typed reader cause in their messages. For example, division by zero has
`source_kind=divide_by_zero`. These exceptions do not expose the attributes above.

::: delta_arrow_reader.DeltaReaderError

## Get more context

[Enable logging](logging.md) to see reader progress or collect more detail
while investigating a failure.
