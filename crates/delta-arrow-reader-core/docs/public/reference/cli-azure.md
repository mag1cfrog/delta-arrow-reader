# Azure credential sources

Use this reference when provisioning credentials for an Azure Blob table. For
passing an existing file to the CLI, see [Connect to storage](../cli-storage.md).

## Azure credential sources

`inspect`, `scan`, `preview`, and `query` accept `--auth-source json|env` for Azure account
keys, SAS tokens, bearer tokens, and service principals using a client secret.
Select exactly one credential family. A service principal requires all three
fields: client ID, client secret, and tenant ID. Partial or mixed families are
rejected before reading the table.

The identity is loaded once per command. Client-secret authorization uses the
native Azure provider to obtain and refresh access tokens for that identity.
An authorization or refresh failure fails the read without switching identities.
Account keys, SAS tokens, and supplied bearer tokens are not refreshed.

| Credential | Canonical JSON key | Environment variable |
| --- | --- | --- |
| Account key | `azure_storage_account_key` | `AZURE_STORAGE_ACCOUNT_KEY` |
| SAS token | `azure_storage_sas_key` | `AZURE_STORAGE_SAS_KEY` |
| Bearer token | `azure_storage_token` | `AZURE_STORAGE_TOKEN` |
| Client ID | `azure_storage_client_id` | `AZURE_CLIENT_ID` |
| Client secret | `azure_storage_client_secret` | `AZURE_CLIENT_SECRET` |
| Tenant ID | `azure_storage_tenant_id` | `AZURE_TENANT_ID` |

`--auth-source json` requires `--storage-options-file` and ignores environment
credentials. For example, provision a protected `storage.json` file:

```json
{
  "azure_storage_account_name": "exampleaccount",
  "azure_storage_sas_key": "<sas-token>"
}
```

```sh
dar inspect az://orders/table --view summary --auth-source json --storage-options-file storage.json
```

For a service principal, replace the SAS field with the complete client-secret
family:

```json
{
  "azure_storage_account_name": "exampleaccount",
  "azure_storage_client_id": "<application-id>",
  "azure_storage_client_secret": "<client-secret>",
  "azure_storage_tenant_id": "<tenant-id>"
}
```

`--auth-source env` reads only the six credential variables above and
`AZURE_STORAGE_ACCOUNT_NAME`. A storage-options file may supply noncredential
settings, but credential fields in that file are rejected.

```sh
dar query abfss://orders@exampleaccount.dfs.core.windows.net/table \
  --auth-source env --sql-file count.sql > count.arrow
```

An account embedded in an `abfss://container@account...` or native HTTPS storage
URL takes precedence, as in the native Azure builder. Otherwise, supply
`azure_storage_account_name` in JSON or, with `--auth-source env`,
`AZURE_STORAGE_ACCOUNT_NAME`. JSON takes precedence over the environment.

JSON accepts the native Azure aliases, case-insensitively. Examples include
`access_key`/`account_key`, `sas_key`/`sas_token`, `bearer_token`/`token`,
`client_id`, `client_secret`, and `tenant_id`/`authority_id`.
Duplicate aliases are rejected, even with identical values. Account keys must
be base64-encoded. SAS tokens may start with `?` and must include a nonempty
`sig`; literal and percent-encoded `+` characters are preserved. Bearer tokens
must contain only visible ASCII characters.

Explicit selection accepts account, endpoint, HTTP client, and Fabric endpoint
settings. Client-secret authorization uses the native provider's public Azure
authority by default; set `azure_storage_authority_host` in JSON to override it.
The `authority_host` alias is also accepted. The `json` and `env` sources do not
read `AZURE_AUTHORITY_HOST`.

Unknown keys, container overrides, emulator mode, unsigned requests, and
provider-selection options are rejected. Workload and managed identities require
their own source selectors, described below. Azure CLI is not offered by
`--auth-source`. Invalid source configuration exits with status 2 and code
`invalid_storage_authorization`.

Keep credential files out of source control and restrict access, for example
with `chmod 600 storage.json` on Unix. File references do not isolate credentials
from a process that can read the file. Credential values and file contents are
omitted from text and JSON diagnostics.

### Azure workload identity

Use `--auth-source azure-workload-identity` with an Azure service principal
configured to trust the issuer of a federated token file. It is available for
tables in Azure Blob Storage with `inspect`, `scan`, `preview`, and `query`.

```sh
dar query abfss://orders@exampleaccount.dfs.core.windows.net/table \
  --auth-source azure-workload-identity --sql-file count.sql > count.arrow
```

Provision all three required references in the environment or a protected
storage-options file:

| Reference | Canonical JSON key | Environment variable |
| --- | --- | --- |
| Client ID | `azure_storage_client_id` | `AZURE_CLIENT_ID` |
| Tenant ID | `azure_storage_tenant_id` | `AZURE_TENANT_ID` |
| Token file path | `azure_federated_token_file` | `AZURE_FEDERATED_TOKEN_FILE` |

JSON accepts native aliases such as `client_id`, `tenant_id`, and
`federated_token_file`. Each JSON field takes precedence over its environment
counterpart. Missing, empty, and duplicate references are rejected before
reading the table. Account names follow the same URL, JSON, and environment
precedence as the other Azure sources.

The optional `azure_storage_authority_host` JSON setting takes precedence over
`AZURE_AUTHORITY_HOST`. If both are absent, the native provider uses the public
Azure authority. Endpoint and HTTP client settings remain in the storage-options
file.

The native provider fixes the client ID, tenant ID, authority, and token file
path for the command. When an access token needs renewal, it rereads the same
file, so the workload issuer can rotate its contents. Unreadable files, rejected
assertions, and refresh failures fail the read without selecting another
identity. The CLI does not create the file or manage its rotation.

Other credential families in JSON are rejected. Ambient account keys, SAS
tokens, bearer tokens, and client secrets are ignored by this source. Token
contents are not passed in command arguments or included in diagnostics.

### Azure managed identity

Use `--auth-source azure-managed-identity` on a host that provides Azure managed
identity tokens. Select exactly one identity by client ID, object ID, or managed
identity resource ID. This source requires an explicit ID; it does not select
the host's default identity when the ID is missing or rejected.

| Identity selector | Canonical JSON key | Environment variable |
| --- | --- | --- |
| Client ID | `azure_storage_client_id` | `AZURE_CLIENT_ID` |
| Object ID | `azure_object_id` | `AZURE_OBJECT_ID` |
| Resource ID | `azure_msi_resource_id` | `AZURE_MSI_RESOURCE_ID` |

JSON accepts native aliases such as `client_id`, `object_id`, and
`msi_resource_id`. One selector in JSON takes precedence over all three
environment selectors. Without a JSON selector, set exactly one of the listed
environment variables. Missing IDs, empty values, duplicate aliases, and
multiple selectors fail before table access.

For example, provision a `storage.json` file containing the selected client ID:

```json
{
  "azure_storage_client_id": "<managed-identity-client-id>"
}
```

```sh
dar inspect abfss://orders@exampleaccount.dfs.core.windows.net/table --view summary \
  --auth-source azure-managed-identity --storage-options-file storage.json
```

The source also works with `scan`, `preview`, and `query`. Account names follow the same
URL, JSON, and environment precedence as the other Azure sources.

The token endpoint comes from `azure_msi_endpoint` in JSON, then
`IDENTITY_ENDPOINT`, then the native Azure IMDS endpoint. JSON aliases include
`msi_endpoint` and `identity_endpoint`. If the host supplies `IDENTITY_HEADER`,
the native provider sends it as `x-identity-header` on token requests only.
That secret stays in the environment; it is not a JSON field or command argument.
`AZURE_MSI_ENDPOINT` is not read by this source.

The native provider obtains and refreshes tokens with the same identity
selector and endpoint throughout the command. Token acquisition, refresh, and
storage permission failures stop the read without selecting another identity.
Other credential families in JSON are rejected; ambient storage credentials,
client secrets, workload tokens, and Azure CLI sessions are ignored.
