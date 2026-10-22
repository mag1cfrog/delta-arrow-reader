# Read a table from the command line

Use the CLI to see a Delta table's columns or save some of its rows to a file.
This quickstart starts with a local table and reads at most ten rows.

## Before you start

[Install the CLI](installation.md#cli-binaries-and-container-images) and have a
Delta table available. Replace `/path/to/table` below with the table's directory,
which contains `_delta_log`, rather than a single Parquet file. Run the shell
examples in Bash.

The examples use the standalone command, `dar`. If you installed the
[Python package](installation.md#python), use `delta-arrow-reader` in its place.
Both commands accept the same arguments.

## Inspect the snapshot

```sh
dar inspect /path/to/table --view summary
```

The command prints `table_version` and checks scan protocol support. It does not test access to row or deletion-vector data.
To find columns, search by a prefix and pin the reported version:

```sh
dar inspect /path/to/table --view summary --table-version 0 --column-prefix id
dar inspect /path/to/table --view summary --table-version 0 --column id
```

Replace `0` with the reported version, and `id` with a prefix or name from your
table. The first lookup returns matching names; the second returns the selected
column's type. If the response is too large, narrow the prefix or select fewer
columns.

## Save a small result

```sh
dar scan --limit 10 /path/to/table > sample.arrow
```

This saves up to ten rows, with all columns, to `sample.arrow`. The limit keeps
your first result small. Row order is not guaranteed.

The file contains an **Arrow IPC stream**, a binary format that Arrow libraries
can read. It is not CSV or text, so `scan` requires output to a file or pipe
instead of printing rows in the terminal.

Only use the file if the command succeeds. If it reports an error, discard the
file: a failed scan can leave an incomplete result.

## Read the saved result

Open the file with an Arrow IPC stream reader in your application. For example,
if you have Python and PyArrow installed:

```python
import pyarrow.ipc as ipc

with ipc.open_stream("sample.arrow") as reader:
    print(reader.read_all().to_pydict())
```

This prints the saved rows as a dictionary of columns. Collecting the result
in memory is convenient for this small sample. For larger results, process
one batch at a time.

## Choose columns

Once the first scan works, repeat `--column` to select the columns you need:

```sh
dar scan --column id --column region --limit 10 /path/to/table > selected.arrow
```

Replace `id` and `region` with names from your table. The output contains those
columns in the order you requested. Omit `--limit` when you are ready to read all
rows.

## Where to go next

- [Diagnose a failed command](cli.md#errors-and-process-status) or save a log for investigation.
- [Filter rows](cli.md#filter-rows) when you only need rows matching a condition.
- [Read a table from storage such as S3](cli.md#inspect-a-snapshot), with
  [storage options](cli.md#storage-options-file) when your environment needs them.
- [Use a container or call the CLI from Go](cli.md#use-a-container-or-a-go-service)
  when adding the reader to a service.
- Look up flags, output formats, and exit statuses in the [CLI reference](cli.md).

For help in the terminal, run `dar inspect --help` or `dar scan --help`.
