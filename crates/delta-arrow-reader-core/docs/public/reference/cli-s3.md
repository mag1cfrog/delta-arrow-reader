# S3 credential sources

Use this reference when provisioning credentials for an S3 table. For passing an
existing file to the CLI, see [Connect to storage](../cli-storage.md).

## S3 credential sources

`inspect`, `scan`, `preview`, and `query` accept `--auth-source json|env` for explicit S3
access-key credentials. The key ID and secret key must both be present; a
session token is optional. The source is loaded once per command. Expired or
denied credentials fail the read without selecting another identity. Supply
updated credentials when starting another command.

`--auth-source json` requires `--storage-options-file`. That file supplies the
credentials and storage configuration; AWS environment credentials are ignored.
For example, provision a protected `storage.json` file with this shape:

```json
{
  "aws_access_key_id": "<access-key-id>",
  "aws_secret_access_key": "<secret-access-key>",
  "aws_region": "eu-west-1"
}
```

```sh
dar inspect s3://example-bucket/orders --view summary --auth-source json --storage-options-file storage.json
```

`--auth-source env` reads only `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`,
`AWS_SESSION_TOKEN`, `AWS_REGION`, and `AWS_DEFAULT_REGION`. Provision these in
the command's environment. An optional storage-options file may supply
noncredential settings, but credential fields in that file are rejected.

```sh
dar query s3://example-bucket/orders --auth-source env --sql-file count.sql > count.arrow
```

JSON region settings take precedence over both environment region settings.
Within either source, `aws_region`/`AWS_REGION` takes precedence over
`aws_default_region`/`AWS_DEFAULT_REGION`. Without a region, the native S3
builder uses `us-east-1`; set the bucket's region explicitly for other regions.

JSON keys use the native S3 aliases and are case-insensitive. For example,
`access_key_id` and `AWS_ACCESS_KEY_ID` refer to the same setting, as do
`token`, `session_token`, `aws_token`, and `aws_session_token`. Supplying two
aliases for the same setting is an error, even if their values match.
Credentials must be nonempty and contain no control characters.

The `json` and `env` sources accept region, endpoint, HTTP client, virtual-hosted
request, unsigned-payload, checksum, and requester-pays settings. They reject
unknown keys, bucket overrides, unsigned requests, and provider-selection
settings. They do not select AWS profiles, SSO, web identity, container
credentials, or instance metadata. Unity Catalog uses `--databricks-profile`
and rejects `--auth-source`.

Invalid source configuration exits with status 2 and diagnostic code
`invalid_storage_authorization`. Storage failures retain the reader's normal
diagnostics. Credential values and file contents are omitted from both text
and JSON diagnostics. Keep credential files out of source control and restrict
access, for example with `chmod 600 storage.json` on Unix. A file reference does
not isolate credentials from an agent or process that can read that file.

### S3 web identity

Use `--auth-source aws-web-identity` with `inspect`, `scan`, `preview`, or `query` to assume
an AWS role using a web identity token file. The role ARN and token file path
are required. Each JSON field takes precedence over its environment variable;
fields absent from JSON are read from the environment.

| Setting | Canonical JSON key | Environment variable |
| --- | --- | --- |
| Role ARN | `aws_role_arn` | `AWS_ROLE_ARN` |
| Token file path | `aws_web_identity_token_file` | `AWS_WEB_IDENTITY_TOKEN_FILE` |
| Session name | `aws_role_session_name` | `AWS_ROLE_SESSION_NAME` |
| STS endpoint | `aws_endpoint_url_sts` | `AWS_ENDPOINT_URL_STS` |

For example, with `AWS_ROLE_ARN`, `AWS_WEB_IDENTITY_TOKEN_FILE`, and `AWS_REGION`
set by the workload environment:

```sh
dar query s3://example-bucket/orders --auth-source aws-web-identity \
  --sql-file count.sql > count.arrow
```

An optional `--storage-options-file` can supply the fields above and the S3
storage settings described earlier. The same region precedence applies: JSON
over environment, then `aws_region` over `aws_default_region`. The default
region is `us-east-1`, the default session name is `WebIdentitySession`, and
the default STS endpoint is `https://sts.<region>.amazonaws.com`. STS requires
HTTPS even when `allow_http` enables HTTP for storage requests.

The command fixes the role, session name, token file path, and endpoint when
it starts. The native provider obtains temporary credentials and rereads that
token file when credentials need refreshing. Your workload must provision and
rotate the token file. Changing the options file during a command does not
change the selected identity.

Missing references, duplicate aliases, empty values, and control characters
are rejected. JSON access keys and alternative provider settings are also
rejected. Ambient access keys, profiles, SSO, container credentials, and
instance metadata are not selected. An unreadable token file, failed STS
request, or storage permission denial fails the read without switching
identities. This source supports S3 tables only.

### S3 container credentials

Use `--auth-source aws-container` with `inspect`, `scan`, `preview`, or `query` for
[ECS task roles or EKS Pod Identity](https://docs.aws.amazon.com/sdkref/latest/guide/feature-container-credentials.html).
Configure exactly one of these references:

- ECS: a relative credentials URI. It is appended to `http://169.254.170.2`.
- EKS: a full credentials URI and an authorization token file path.

| Setting | Canonical JSON key | Environment variable |
| --- | --- | --- |
| Relative URI | `aws_container_credentials_relative_uri` | `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI` |
| Full URI | `aws_container_credentials_full_uri` | `AWS_CONTAINER_CREDENTIALS_FULL_URI` |
| Token file path | `aws_container_authorization_token_file` | `AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE` |

For example, when the container runtime supplies the environment reference:

```sh
dar query s3://example-bucket/orders --auth-source aws-container \
  --sql-file count.sql > count.arrow
```

If `--storage-options-file` contains any container reference field, it must
contain the complete reference. All container environment references are then
ignored. Otherwise, the command reads the three environment variables above.
It rejects mixed ECS/EKS references and incomplete EKS references, including
a full URI without a token file. Region and other S3 storage settings follow
the rules described earlier.

Relative URIs must begin with a single `/` and contain no backslashes. Full
URIs must use HTTP or HTTPS. Both forms may include a query string; embedded
user credentials and URL fragments are rejected. The native provider allows
HTTP for container credential endpoints independently of storage `allow_http`.

The endpoint and token file path are fixed when the command starts. The native
provider refreshes temporary credentials from that endpoint. For EKS, it
rereads the token file and sends its contents in the `Authorization` header;
the container runtime must provision and rotate this file. The CLI does not
use `AWS_CONTAINER_AUTHORIZATION_TOKEN` or support a full URI without a token
file. These limits reflect the installed provider's supported mechanisms.

Unreadable token files, failed credential requests, malformed responses, and
storage permission denials fail the read. The command does not switch to
ambient access keys, web identity, profiles, or instance metadata. JSON fields
selecting those other sources are rejected. This source supports S3 only.
