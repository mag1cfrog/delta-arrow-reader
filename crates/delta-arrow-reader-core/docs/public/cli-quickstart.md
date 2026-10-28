# Read your first rows

Use the command line to look at a few values in a Delta table, then count its
rows with SQL. You need a table directory containing `_delta_log`, not an
individual Parquet file. The examples use Bash.

## Before you start

[Install a CLI with these commands](installation.md#cli-binaries-and-container-images).
The standalone command is `dar`. If you installed the Python package, replace
`dar` with `delta-arrow-reader`; the arguments are the same.

Start with a local table. For a cloud URL and an existing credential file, use
[Connect to storage](cli-storage.md). For a catalog name and a Databricks
profile, use [Read Unity Catalog tables](unity-catalog.md).

## Choose a snapshot and column

<span id="inspect-the-snapshot"></span>
<span id="see-the-tables-columns"></span>

Set `table` to your table's directory:

```sh
table=/path/to/table
```

If your task supplies a snapshot version, use it for `version` below and skip
inspection. Otherwise, discover a version:

```sh
dar inspect "$table" --view summary
```

Replace `0` with the supplied version or the inspection's `table_version`:

```sh
version=0
```

Later commands read the same snapshot even if new data arrives. Inspection
checks table metadata; it does not test access to every data file.

Choose a column relevant to your question. If you already know its name, skip
the name search. Otherwise, search by a likely prefix, replacing `id` below:

```sh
dar inspect "$table" --view summary --table-version "$version" --column-prefix id
```

Use a returned name in the preview below. An empty name list means no column
matches that prefix. Try another prefix. If there are too many matches, make
the prefix more specific. Preview returns the selected column's type with its
values, so a separate type lookup is unnecessary here.

## Preview selected rows

```sh
dar preview "$table" --table-version "$version" --column id --limit 3
```

This prints at most three values as JSON, along with their type and snapshot
version. Repeat `--column` to choose another field you need. Integer values are
strings so large integers remain exact.

The result is a **sample**, and row order is unspecified. Reaching three rows
does not prove that the table has only three rows. Preview defaults to an 8 KiB
response budget and a 30-second deadline. Choose a small projection; see
[Handle a result that is too large](cli-recovery.md#the-result-is-too-large)
if even one value cannot fit.

## Count all rows with SQL

SQL calls the selected table `t`. Save a query and run it at the same version:

```sh
printf 'SELECT COUNT(*) AS row_count FROM t;\n' > count.sql
dar query "$table" --table-version "$version" --sql-file count.sql --format json
```

The response's `rows` array contains the count as a string, such as `[["120"]]`.
Its envelope's `row_count` is `"1"`, because the query returned one result row.
`completion: "complete"` and `scope: "query"` describe the complete SQL result.
Counting can read the whole snapshot even though it returns only one row.

Only use results from a command that exits successfully. If it fails, follow
[Fix a failed command](cli-recovery.md) before retrying.

## Continue with your task

<span id="save-a-small-result"></span>
<span id="read-the-saved-result"></span>
<span id="choose-columns"></span>
<span id="where-to-go-next"></span>

- [Filter, group, sort, or join data](cli-workflows.md) with SQL.
- [Connect to storage](cli-storage.md) using a provisioned credential file.
- [Save and read a complete result](cli-results.md#save-a-complete-result)
  when the rows should stay in a file.
- [Read a saved result](cli-results.md#read-a-saved-result) when another
  application needs Arrow IPC.

For one command's flags, use `dar preview --help` or its
[CLI reference](cli.md#preview-rows). You do not need the full reference for
the examples above.
