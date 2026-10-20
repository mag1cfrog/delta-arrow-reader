# Python

<span id="python-api"></span>

[Install the package](../installation.md#python), then follow the
[Python quickstart](../python.md) to read your first rows. Queries run in your
Python process. You do not need a Spark or Databricks cluster.

Once you can read a table, choose a page below for the task at hand.

<span id="find-the-operation-you-need"></span>
<span id="choose-settings-after-the-first-query"></span>

## Common tasks

| Task | Read |
| --- | --- |
| Select columns or filter rows | [Read a table](../python.md#choose-columns) |
| Work with a Pandas or Polars DataFrame | [Convert the result](../python.md#use-pandas-or-polars) |
| Run a SQL query | [Query with SQL](../python.md#query-with-sql) |
| Process a result that may not fit in memory | [Read in batches](../python.md#process-a-larger-result-in-batches) |
| Read an older version or pick up new data | [Table versions](../python.md#read-again-or-use-newer-data) |
| Connect to cloud storage | [S3](../configuration.md#read-from-s3) or [Azure](../configuration.md#read-from-azure) |
| Catch a failure or inspect an error code | [Handle errors](errors.md) |
| Show reader progress or enable more detailed logs | [Enable logging](logging.md) |
| Reduce work before the first query | [Table preparation](../configuration.md#want-less-work-before-the-first-query) |
| Limit concurrent file reads | [Parallel read settings](../configuration.md#limit-parallel-reads) |

## API reference

Use these pages to look up signatures, parameters, defaults, and exceptions.

| Task | API |
| --- | --- |
| Open a table and inspect its columns | [DeltaTable](table.md) |
| Select rows and columns as PyArrow batches | [DeltaTable.to_reader()](table.md#delta_arrow_reader.DeltaTable.to_reader) |
| Run SQL over one or more tables | [Session](session.md) |
| Read a newer table version | [DeltaTable.refresh()](table.md#delta_arrow_reader.DeltaTable.refresh) |
| Limit concurrent file reads or buffering | [ScanExecutionOptions](options.md) |

## Advanced integration

Use these pages when implementing an Arrow stream integration or customizing
how your application handles diagnostic records. Pandas and Polars users can
use the conversion example above.

| Task | Read |
| --- | --- |
| Export a scan to another Arrow library | [Arrow stream interface](streams-and-errors.md) |
| Format structured log fields or write a logging handler | [Log records and handlers](../reference/python-logging.md) |
