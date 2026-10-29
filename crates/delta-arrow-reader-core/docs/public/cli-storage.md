# Connect the CLI to storage

Use the table location and authorization reference provided by your administrator.
You can pass a credential file's path without opening it or copying its contents
into a command.

| What you have | Where to start |
| --- | --- |
| A local Delta directory | [Read your first rows](cli-quickstart.md); no storage credentials are needed. |
| A storage URL and a credential env file | [Use an env file](#use-a-provisioned-env-file). |
| A storage URL and a JSON storage-options file | [Use a JSON file](#use-a-provisioned-json-file). |
| A Unity Catalog name and a Databricks profile | [Read Unity Catalog tables](unity-catalog.md). |

## Use a provisioned env file

For S3, replace the table URL and file path below:

```sh
table=s3://example-bucket/orders
credentials=/run/secrets/orders.env
```

The file must already contain the credentials and connection settings needed
for that service. If you were also given a noncredential endpoint/region JSON
file, add `--storage-options-file /path/to/connection.json` on every command.

Use your task's snapshot version. If none was supplied, discover one:

```sh
dar inspect "$table" --view summary --env-file "$credentials"
```

Set `version` to the supplied version or returned `table_version`, then select
a column you need:

```sh
version=0
dar preview "$table" --table-version "$version" --column id --limit 3 \
  --env-file "$credentials"
```

Replace `0` and `id` with your version and column. The same `--env-file` argument
works with `query`, `scan`, and `explain`. Keep it when following the
[SQL recipes](cli-workflows.md) or [saving results](cli-results.md#save-a-complete-result).

Do not `source` the file, print it, or expand credential values into CLI
arguments. The CLI parses it without executing shell code. It reads only the
selected file and does not search for `.env` files or fill missing credentials
from another identity.

## Use a provisioned JSON file

Choose this alternative when your administrator supplies a JSON storage-options
file containing credentials or references to them. Use `--auth-source json`
and `--storage-options-file` on your reads. To discover a missing snapshot version:

```sh
dar inspect s3://example-bucket/orders --view summary \
  --auth-source json --storage-options-file /run/secrets/orders.json
```

Keep both arguments on later reads. The file's contents stay out of command
arguments. Do not combine a credential JSON file with `--env-file`; that mode
allows only noncredential settings in the optional JSON file.

## Another service or a failed connection

Env-file and JSON-file references also support Azure Blob Storage and GCS.
Use that service's table URL and file, with the same flags. When the file needs
correction, request its selected guide, for example:

```sh
dar capabilities --authorization s3 --detail env-file
```

Replace `s3` with `azure-blob` or `gcs` for those services. Ask the person managing
the credential file to correct or renew it. If access is denied, request access
for the selected identity; do not retry using a different identity or by removing
the authorization selector. An unclassified error does not establish that
credentials expired. See [connection failure recovery](cli-recovery.md#authorization-failed).

After the connection works, return to [Read your first rows](cli-quickstart.md)
or [Analyze a table with SQL](cli-workflows.md), keeping the same authorization
arguments. For other credential sources, precedence, and file grammar, use the
[authorization reference](reference/cli-authorization.md#explicit-credential-sources).
