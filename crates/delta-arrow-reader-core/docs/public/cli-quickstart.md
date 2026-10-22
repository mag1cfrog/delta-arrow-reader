# Read a table from the command line

Use the CLI to see a Delta table's columns or save some of its rows to a file.
This quickstart starts with a local table and reads at most ten rows.

To use an existing Databricks login instead of a storage path, follow
[Read Unity Catalog tables](unity-catalog.md).

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

The command prints `table_version` and checks scan protocol support. It does not
test access to row or deletion-vector data. Set `version` to the reported number,
replacing `0` below, and use it for column lookups and later reads:

```sh
version=0
dar inspect /path/to/table --view summary --table-version "$version" --column-prefix ''
dar inspect /path/to/table --view summary --table-version "$version" --column id
```

The empty prefix lists column names. Replace `id` with a returned name to look
up its type. If the list exceeds the response budget, narrow the prefix or
increase `--max-output-bytes` for that lookup.

## Preview selected rows

```sh
dar preview /path/to/table --table-version "$version" --column id --limit 3
```

Use the version and column from inspection. This prints up to three rows as
JSON, with the column type and actual snapshot version. Integer values are
strings so large values remain exact. The result is marked `sample`; reaching
three rows does not prove that the table has no more rows.

Preview defaults to an 8 KiB response budget and a 30-second deadline. Choose
only the columns and rows you need. See [preview limits](cli.md#preview-rows)
for byte-limit behavior and overrides.

## Save a small result

```sh
dar scan --table-version "$version" --limit 10 /path/to/table > sample.arrow
```

This saves up to ten rows, with all columns, to `sample.arrow`. The limit keeps
your first result small. Row order is not guaranteed.

The file contains an **Arrow IPC stream**, a binary format that Arrow libraries
can read. It is not CSV or text, so `scan` requires output to a file or pipe
instead of printing rows in the terminal.

Only use the file if the command succeeds. If it reports an error, discard the
file: a failed scan can leave an incomplete result.

## Read the saved result

Open the file with an Arrow IPC stream reader in your application. To display
this small sample in the terminal, [install uv](https://docs.astral.sh/uv/getting-started/installation/)
if needed, then run the command below in Bash. uv supplies Python and PyArrow
in an isolated environment.

```sh
uv run --no-project --with pyarrow python - sample.arrow <<'PY'
import sys
import pyarrow.ipc as ipc

with ipc.open_stream(sys.argv[1]) as reader:
    print(reader.read_all().to_pydict())
PY
```

Replace `sample.arrow` in the first line with the file you saved. If your active
Python environment already has PyArrow, replace
`uv run --no-project --with pyarrow python` with `python`.

This prints the saved rows as a dictionary of columns. Collecting the result
in memory is convenient for this small sample. For larger results, process
one batch at a time.

## Choose columns

Once the first scan works, repeat `--column` to select the columns you need:

```sh
dar scan --table-version "$version" --column id --column region --limit 10 \
  /path/to/table > selected.arrow
```

Replace `id` and `region` with names from your table. The output contains those
columns in the order you requested. Omit `--limit` when you are ready to read all
rows.

## Where to go next

- [Diagnose a failed command](cli.md#errors-and-process-status) or save a log for investigation.
- [Filter rows](cli.md#filter-rows) when you only need rows matching a condition.
- [Read a Unity Catalog table](unity-catalog.md) with a Databricks profile, including local SQL.
- [Read a table from storage such as S3](cli.md#inspect-a-snapshot), with
  [storage options](cli.md#storage-options-file) when your environment needs them.
- [Use a container or call the CLI from Go](cli.md#use-a-container-or-a-go-service)
  when adding the reader to a service.
- Look up flags, output formats, and exit statuses in the [CLI reference](cli.md).

For help in the terminal, run `dar inspect --help` or `dar scan --help`.
