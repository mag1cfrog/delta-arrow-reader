# GCS credential sources

Use this reference when provisioning credentials for a GCS table. For passing an
existing file to the CLI, see [Connect to storage](../cli-storage.md).

## GCS credential sources

`inspect`, `scan`, `preview`, and `query` accept `--auth-source json|env` to select an
explicit Application Default Credentials (ADC) file for GCS. The installed
native provider supports these file types:

| ADC `type` | Authorization |
| --- | --- |
| `service_account` | Signs JWT bearer tokens with the file's RSA private key |
| `authorized_user` | Exchanges the file's refresh token for OAuth access tokens |

`--auth-source json` requires `--storage-options-file`. Use that file to point
to the separately provisioned credentials file:

```json
{"google_application_credentials":"/run/secrets/gcs-credentials.json"}
```

```sh
dar inspect gs://example-bucket/orders --view summary --auth-source json --storage-options-file storage.json
```

`--auth-source env` reads only `GOOGLE_APPLICATION_CREDENTIALS`, which must
contain a nonempty file path. An optional storage-options file may contain
noncredential settings, but an ADC reference in that file is rejected.

```sh
dar query gs://example-bucket/orders --auth-source env --sql-file count.sql > count.arrow
```

For a positional table with `--auth-source json` or `env`, relative credential
paths use the command's working directory. Inside a credential env file, they
use that file's directory. A JSON file selected by a tables manifest also
resolves credential paths against the JSON file's directory; see
[manifest path rules](cli-inputs.md#join-table-snapshots).

The provider loads the selected file once and retains that identity for token renewal.
Changing the file during a command does not change the identity. Missing,
invalid, expired, or denied credentials never trigger home-directory ADC
lookup, a metadata request, or selection of another source. Authorized-user
token renewal requires access to Google's OAuth endpoint.

JSON keys are case-insensitive. `application_credentials` is an alias for
`google_application_credentials`; supplying both is an error. Storage endpoint
(`google_base_url`/`base_url`) and HTTP client settings are accepted. Bucket
overrides, unsigned requests, and unknown keys are rejected. Explicit source
selection uses ADC files; it rejects the legacy `google_service_account` and
`google_service_account_key` settings. Standard service-account files with
`"type":"service_account"` use the ADC reference shown above. Other ADC types,
including external-account and impersonated-service-account files, are not
supported by the installed provider.

Invalid source configuration or an unreadable or malformed ADC file exits with
status 2 and code `invalid_storage_authorization`. Token-fetch, renewal, and
storage failures retain the reader's normal diagnostics. Credential values and
file contents are omitted from text and JSON diagnostics. Protect both files
with appropriate filesystem permissions and keep credentials out of source
control. A file reference does not prevent a process with read access from
reading the credentials.
