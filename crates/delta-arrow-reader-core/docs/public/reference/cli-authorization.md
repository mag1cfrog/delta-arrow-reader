# CLI authorization

For a provisioned credential file or Databricks profile, start with
[Connect to storage](../cli-storage.md). This reference defines credential selection,
file formats, and the supported identities for each storage service.

## Storage options file

`--storage-options-file` accepts a local UTF-8 JSON file containing an object
of string keys and string values. For example, save this as `storage.json`
to select an S3 region while using explicitly selected environment credentials:

```json
{"aws_region":"us-west-2"}
```

Then pass the file when opening the table:

```sh
dar inspect --view summary --auth-source env --storage-options-file storage.json s3://example-bucket/orders
```

Use the same option with `dar scan`, `dar preview`, or `dar query`. The
[storage guide](../configuration.md#read-from-s3) explains table URLs and credentials,
including [Azure Blob](../configuration.md#read-from-azure). Use its storage keys as JSON
keys here. Keep files containing credentials out of source control.

For stdout without `--auth-source` or `--env-file`, the CLI forwards keys and values unchanged,
including case, spaces, and commas. The core reader interprets provider options. Omitting
the file supplies an empty map; an empty object is also valid. This path does
not automatically read AWS access keys from the environment. Use
`--auth-source env` to select them. For remote direct storage with `--output`,
a storage-options file alone selects the `json` source and its validation rules.

For a local HTTP test endpoint, `{"allow_http":"true"}` enables plain HTTP.

<details markdown="1">
<summary>Storage file parsing and size limits</summary>

The file limit is 1 MiB (1,048,576 bytes), measured before JSON parsing. Reading
stops when that limit is exceeded. Invalid UTF-8, malformed JSON, trailing
non-whitespace content, duplicate keys (including equivalent escaped keys),
and non-string values are rejected before table loading. File paths resolve
against the working directory. A path of `-` names a local file; it does not
read standard input.

</details>

## Explicit credential sources

`--auth-source` selects authorization for direct cloud reads.
`--env-file PATH` instead selects a [credential env file](#credential-env-files).
Both options are available to `inspect`, `scan`, `preview`, and `query`.
Help lists the installed selectors without reading input files or resolving
credentials.

| Backend | Source | Credentials | Renewal |
| --- | --- | --- | --- |
| S3 | [`json` or `env`](cli-s3.md#s3-credential-sources) | Access-key pair, with an optional session token | Supply new credentials for another command |
| S3 | [`aws-web-identity`](cli-s3.md#s3-web-identity) | Role ARN and web identity token file | Refreshes temporary credentials; rereads the token file |
| S3 | [`aws-container`](cli-s3.md#s3-container-credentials) | ECS relative URI, or EKS full URI and token file | Refreshes at the selected endpoint; rereads the EKS token file |
| Azure Blob | [`json` or `env`](cli-azure.md#azure-credential-sources) | Account key, SAS token, or bearer token | Supply new credentials for another command |
| Azure Blob | [`json` or `env`](cli-azure.md#azure-credential-sources) | Client ID, client secret, and tenant ID | Refreshes access tokens for the loaded principal |
| Azure Blob | [`azure-workload-identity`](cli-azure.md#azure-workload-identity) | Client ID, tenant ID, and federated token file | Refreshes access tokens; rereads the token file |
| Azure Blob | [`azure-managed-identity`](cli-azure.md#azure-managed-identity) | One explicit client, object, or resource ID | Refreshes access tokens for that identity |
| GCS | [`json` or `env`](cli-gcs.md#gcs-credential-sources) | Explicit service-account or authorized-user ADC file | Renews using the identity loaded from the file |

Local paths and Unity Catalog do not accept `--auth-source` or `--env-file`.
Unity Catalog uses `--databricks-profile`. For `scan --output` and `query --output`,
remote storage requires an explicit source. A storage-options file alone selects
`json`; otherwise select `--auth-source` or `--env-file`. Every remote binding in
`--tables-file` needs its own `authorization`. Stdout mode preserves the existing
storage-options behavior, including native provider defaults when no source is
selected.
The explicit sources above do not fall back to other credential sources after
configuration, acquisition, renewal, or permission failures.

### Provider limits

The CLI exposes the mechanisms it can select through the installed
`object_store` provider. It does not implement a full cloud SDK credential
chain, AWS profiles or SSO, automatic home-directory ADC discovery, or an
anonymous source selector.

AWS instance metadata, Azure CLI accounts, and GCP metadata identities cannot
be selected with `--auth-source`. Explicit GCS ADC files use `json` or `env`.
See the [S3](cli-s3.md#s3-credential-sources), [Azure](cli-azure.md#azure-credential-sources), or
[GCS](cli-gcs.md#gcs-credential-sources) reference for accepted keys and renewal behavior.

## Credential env files

Pass the path to a provisioned file containing credentials for the selected
storage service:

```sh
dar preview s3://example-bucket/orders --env-file /run/secrets/orders.env \
  --column id --limit 3 --diagnostics=json
```

The command reads that file once. It does not search for `.env` files or use
inherited credentials to fill missing values. `--env-file` conflicts with
`--auth-source` and `--databricks-profile`. For queries using `--tables-file`,
set each table's `authorization.env_file` in the
[manifest](cli-inputs.md#join-table-snapshots) instead of passing `--env-file`.

An optional `--storage-options-file` may supply noncredential connection
settings. Its values override env-file settings such as the S3 region or Azure
account name. Credential values in that JSON file are rejected when using
`--env-file`. File, configuration, token acquisition, renewal, and storage
failures do not select another identity.

Env files accept S3 access keys, the Azure credential families supported by
`--auth-source env`, or an explicit GCS ADC file reference. S3 access keys and
Azure account keys, SAS tokens, and bearer tokens stay fixed during a command;
Azure client-secret and GCS ADC providers renew tokens for the loaded identity.
Use a focused guide when preparing or correcting a file:

```sh
dar capabilities --authorization s3 --detail env-file
dar capabilities --authorization azure-blob --detail env-file
dar capabilities --authorization gcs --detail env-file
```

Each guide lists only that service's accepted, case-sensitive variable names
and credential combinations. JSON key aliases and unrelated environment
variables are not accepted. A relative `GOOGLE_APPLICATION_CREDENTIALS` path
resolves against the env file's directory. The `--env-file` path itself resolves
against the working directory; `-` names a file rather than standard input.

<details markdown="1">
<summary>Env-file grammar and limits</summary>

Files contain UTF-8 text with LF or CRLF line endings, at most 1 MiB, and 1 to
1,024 variable records. Blank lines and whole-line `#` comments do not count as
records. Each record is `NAME=VALUE`; names match `[A-Za-z_][A-Za-z0-9_]*`.
Spaces and tabs around names and values are trimmed.

Values may be unquoted text without quotes, or enclosed in matching single or
double quotes. Enclosing quotes are removed and preserve interior whitespace;
the enclosing quote cannot appear inside the value. Empty or whitespace-only
values, duplicate names, control characters, and text after a closing quote
are invalid. The parser does not support escapes, multiline values, inline
comments, `export`, interpolation, or execution. Dollar signs, backticks, `#`,
and backslashes inside values are literal. For example,
`AWS_SECRET_ACCESS_KEY='$VALUE'` supplies the literal string `$VALUE`.

The same grammar is available offline through
`dar capabilities --authorization s3 --detail env-file-grammar`; it applies
to all three storage services. For an invalid record, diagnostics identify its
line and expected syntax without including names, values, or file contents.

</details>
