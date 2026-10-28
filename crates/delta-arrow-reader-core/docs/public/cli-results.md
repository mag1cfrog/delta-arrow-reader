# Save and read results

Use `--output` to save a query result or a scan to a local file. For a first
query, start with the [quickstart](cli-quickstart.md) or
[SQL examples](cli-workflows.md).

## Save a complete result

Run your SQL file at the chosen snapshot version, adding `--output`:

```sh
dar query /path/to/table --table-version 0 --sql-file totals.sql \
  --format json --output totals.json
```

Replace the table, version, and SQL file with yours. Keep the SQL and
[authorization](cli-storage.md) unchanged. For a join, keep your `--tables-file`
with its pinned versions instead of a positional table and `--table-version`.

Remote file exports require an explicit credential file, source, or Databricks
profile for every table. If a stdout query relied on implicit authorization,
select that same identity explicitly before exporting. For a join, put the
authorization reference in each table entry; see
[per-table authorization](reference/cli-inputs.md#join-table-snapshots).

The destination directory must exist and the filename must be unused. When the
command succeeds, stdout reports the absolute path in `result`:

```json
{"format_version":2,"kind":"result_artifact","result":"/work/report/totals.json"}
```

The result is saved in that file. JSON file output defaults to a 16 MiB limit;
set `--max-output-bytes` if the complete answer needs a larger budget.
Adding SQL `LIMIT` or dropping columns would change the answer.

For an Arrow consumer, select `--format arrow` and use an `.arrow` filename.
The extension does not choose the format: without `--format`, output is Arrow.
To export rows without SQL, use [scan](cli.md#scan-rows) with `--output`.

## Read a saved result

After exit status 0, open the reported path with a reader for the selected
format. The file can be moved or copied on its own.

JSON output contains `schema.fields` and positional `rows`. For example, a
`SELECT count(*) AS n FROM t` result has one field named `n`; `rows: [["12"]]`
means the count is 12. `row_count` counts the result rows, so it is `"1"` in this
example. It is not the table's row count.

Match each row value to its field before interpreting it. Integers are strings;
decimal strings contain the unscaled integer, so `"1234"` with scale 2 means
12.34. Dates and times use the unit in their field type. See
[JSON type encodings](reference/cli-output.md#read-a-small-result-as-typed-json)
for other types, or request one encoding with
`dar capabilities --schema rows --detail decimal`.

Arrow output is an IPC **stream**, not the IPC file format. For example, with
[PyArrow installed](installation.md#python):

```python
import pyarrow.ipc as ipc

with open("totals.arrow", "rb") as source:
    with ipc.open_stream(source) as reader:
        for batch in reader:
            print(batch.to_pydict())
```

This reads the saved output without opening the Delta table again. For large
results, process each batch instead of collecting the entire file in memory.

## If saving fails

A nonzero exit status means the invocation failed. A complete file may already
exist at the destination if saving finished before the failure. Preserve that
file. If completion is uncertain, fix the reported error and retry with an
unused filename and the same SQL, versions, and authorization.

See [Fix a failed command](cli-recovery.md) for output limits and other errors.
For a program that consumes receipts and completion records, continue with
[Automation and replay](cli-automation.md).
