# Read Unity Catalog tables

Use your Databricks CLI profile to read a table by its catalog name. DAR obtains
temporary storage credentials through Unity Catalog, then reads the Delta data
and runs SQL on your machine. You do not need a Databricks cluster or SQL warehouse.

The examples use Bash and the standalone command `dar`. If you installed the
Python wheel, use `delta-arrow-reader` instead.

## Before you start

- [Install DAR](installation.md#cli-binaries-and-container-images) with Unity
  Catalog support. The v0.7.0 release predates this feature; use a
  [source build](installation.md#build-cli-from-source) if your installed command
  does not recognize `dar capabilities --authorization unity-catalog`.
- Install the [Databricks CLI](https://docs.databricks.com/aws/en/dev-tools/cli/install)
  and have a working workspace profile. Its `databricks` executable must be on
  `PATH` in the same environment as DAR. DAR packages do not include it.
- Choose a managed or external Delta table that permits direct reads. Your
  administrator must enable external data access and grant `USE CATALOG`,
  `USE SCHEMA`, `SELECT`, and `EXTERNAL USE SCHEMA`. Both the workspace and its
  storage must be reachable. See the [access requirements](reference/unity-catalog.md#table-access)
  if the table has policies or uses a special table type.

The Python and Rust APIs do not resolve `uc://` names; this guide uses the CLI.

<details markdown="1">
<summary id="user-oauth-login">Set up or renew a user OAuth login</summary>

An existing working profile needs no new login. Otherwise, use your workspace
URL and choose a profile name:

```sh
databricks auth login --host https://YOUR_WORKSPACE_HOST --profile work
```

Use the workspace URL, not the account console URL. Repeat this command if the
login expires or is revoked. Login does not grant table permissions. For a
service principal or PAT, follow
[Databricks CLI authentication](https://docs.databricks.com/aws/en/dev-tools/cli/authentication).

</details>

<span id="inspect-and-pin-the-table"></span>

## Choose a snapshot

Replace the table name and profile below with yours. The name has three parts:
`uc://catalog.schema.table`.

For a table stored in S3, also supply a [region options file](reference/unity-catalog.md#s3-region)
with `--storage-options-file storage.json` on each inspect, preview, scan, or query command.

```sh
table=uc://main.sales.orders
profile=work
```

Use your task's snapshot version. If none was supplied, discover one:

```sh
dar inspect "$table" --view summary --databricks-profile "$profile"
```

Inspection prints JSON containing `table_version` and read checks. It loads
snapshot metadata; it does not prove that every data file is readable. Set
`version` to the supplied version or returned `table_version`:

```sh
version=42
```

Replace `42` with your chosen version. If you do not know the needed column's exact
name, search at that version with a likely prefix:

```sh
dar inspect "$table" --view summary --databricks-profile "$profile" \
  --table-version "$version" --column-prefix customer
```

Replace `customer` with a prefix relevant to your table. If too many names
match, narrow the prefix. To inspect a column's type, replace
`--column-prefix customer` with `--column COLUMN_NAME`, using a returned name.

## Read a small sample

Choose a column relevant to your question, replacing `customer_id` below:

```sh
dar preview "$table" --databricks-profile "$profile" --table-version "$version" \
  --column customer_id --limit 3
```

This prints up to three rows as typed JSON in the terminal. Integer values are
strings so large values remain exact. Row order is unspecified, and the result
is marked `sample`. Preview defaults to an 8 KiB response budget and a 30-second
deadline. Select only the columns and rows you need; see
[preview limits](cli.md#preview-rows) for overrides.

### Save rows as Arrow

```sh
dar scan "$table" --databricks-profile "$profile" --table-version "$version" \
  --column customer_id --limit 10 --output orders.arrow
```

This saves at most ten values of the selected column as a binary Arrow IPC
stream. Row order is unspecified. Choose an unused output name, then
[read the saved result](cli-results.md#read-a-saved-result) with an Arrow IPC
stream reader.
A limited scan covers the requested prefix, not the whole table.

To select columns or filter rows, use the ordinary [scan options](cli.md#scan-rows)
and [predicates](reference/cli-inputs.md#filter-rows) with the same table, profile, and version.

## Run SQL locally

A positional table is available to SQL as `t`. For example, count its rows:

```sh
cat > count.sql <<'SQL'
SELECT COUNT(*) AS row_count FROM t;
SQL
dar query "$table" --databricks-profile "$profile" --table-version "$version" \
  --sql-file count.sql --format json
```

The result prints as typed JSON, with `completion: "complete"`. The query can
read the whole snapshot even though it returns one row. SQL executes locally
and uses the same authorization as a scan. See [query limits](cli.md#query-a-snapshot)
for output and memory budgets, and the [JSON reference](reference/cli-output.md#read-a-small-result-as-typed-json)
for the response format.

### Join with a local Delta table

Declare each table and its authorization in `tables.json`. Replace the UC name,
profile, and both versions with your own. Inspect the local table only if its
version was not supplied. Here, `customers` is a Delta directory beside the manifest:

```json
{"format_version":1,"tables":[
  {"alias":"orders","location":"uc://main.sales.orders","table_version":"42",
   "authorization":{"source":"databricks","databricks_profile":"work"}},
  {"alias":"customers","location":"./customers","table_version":"0"}
]}
```

For tables with a `customer_id` column in common, save this as `join.sql`:

```sql
SELECT o.customer_id, COUNT(*) AS order_count
FROM orders o JOIN customers c ON o.customer_id = c.customer_id
GROUP BY o.customer_id
ORDER BY o.customer_id;
```

```sh
dar query --tables-file tables.json --sql-file join.sql --format json --output joined.json
```

Authorization now belongs to each manifest entry, so omit the command-line
`--databricks-profile`. For S3-backed UC tables, add
`"storage_options_file":"./storage.json"` inside that entry's `authorization`.
Each table stays on its selected snapshot; the versions do not form a transaction
across tables. [Read the saved result](cli-results.md#read-a-saved-result)
after exit 0 and command completion. See the [manifest reference](reference/cli-inputs.md#join-table-snapshots)
for direct-storage tables, additional fields, and path rules.

## If a command fails

| Failure | Next action |
| --- | --- |
| `databricks_cli_missing` | Make `databricks` available on `PATH` in the environment running DAR, including inside a container. |
| `databricks_cli_failed` or `databricks_cli_timeout` | Run the [Databricks check below](#check-databricks-directly) in the same environment. Check workspace connectivity and [CLI compatibility](reference/unity-catalog.md#databricks-cli-compatibility). |
| `uc_table_request_failed` or `uc_credential_request_failed` | Check the selected profile with the [Databricks command below](#check-databricks-directly). [Renew user OAuth](#user-oauth-login) if needed, then ask the administrator to check the grants and external access setting. Login cannot grant access. |
| `uc_s3_region_required` | Supply the [S3 region file](reference/unity-catalog.md#s3-region). |
| `unsupported_uc_table` | Check the [supported tables](reference/unity-catalog.md#table-access) and [credential forms](reference/unity-catalog.md#storage-backends-and-credentials). |
| `uc_credentials_expired` | Check the system clock and [renew user OAuth](#user-oauth-login) if needed, then retry. |
| Failure after output was written | Discard failed stdout results. In file mode, preserve existing output and retry with an unused path at the same version if completion is uncertain. With `--manifest`, follow the [manifest checks](cli-automation.md#save-provenance-for-replay). |

DAR renews storage credentials during a command. If renewal fails, the command
fails; it never switches to another identity. See the
[Unity Catalog reference](reference/unity-catalog.md) for credential forms,
renewal rules, and tested compatibility, or [CLI diagnostics](reference/cli-diagnostics.md)
for scripting and machine-readable errors.

### Check Databricks directly

DAR discards the Databricks subprocess's diagnostics. To see its own error
message, run this read-only metadata request with the failing table and profile:

```sh
databricks tables get main.sales.orders --profile work \
  --include-manifest-capabilities --output json
```

Replace the table and profile with yours, without the `uc://` prefix. This
[Databricks command](https://docs.databricks.com/aws/en/dev-tools/cli/reference/tables-commands#databricks-tables-get)
does not use a cluster. Success confirms access to metadata; it does not prove
that external storage credentials or data-file reads will succeed. An unknown
flag means the CLI needs an update. For a timeout, check the workspace URL,
network, and proxy settings; DAR gives each Databricks request 30 seconds.
