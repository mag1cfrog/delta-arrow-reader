# Unity Catalog access reference

Use this page to check table eligibility, storage options, and credential
behavior. For a first read, follow [Read Unity Catalog tables](../unity-catalog.md).

## Table names and profiles

`inspect`, `preview`, `scan`, and `query` accept `uc://catalog.schema.table` with an explicit
`--databricks-profile PROFILE`. Use exactly three unquoted name components.
Each component contains ASCII letters, digits, underscores, or hyphens and
cannot start with a hyphen. Quoted identifiers, URL escapes, query strings,
and fragments are unsupported.

Profiles must be nonblank and contain no control characters. Quote profile
names containing spaces. `--databricks-profile` is invalid for a direct path
or storage URL; `--auth-source` is invalid for a UC table.

In a [tables manifest](../cli.md#join-table-snapshots), each UC entry requires
`authorization.source: "databricks"` and `authorization.databricks_profile`.
Entries can use different profiles, including for the same table name in
different workspaces. Each binding retains its own identity and snapshot.

## Table access

The metastore must allow external data access. The requesting principal needs
`USE CATALOG`, `USE SCHEMA`, `SELECT`, and explicit `EXTERNAL USE SCHEMA`.
Selecting a profile does not grant additional permissions. Administrators can
follow [Databricks external access setup](https://docs.databricks.com/aws/en/external-access/admin).

DAR accepts ordinary managed or external Delta tables that permit direct
external-engine reads and use a supported Delta read protocol. It rejects:

- Views, pipeline tables, and shallow clones.
- Row filters and column masks.
- Catalog commits (`catalogManaged`) and coordinated commits.

Tables in [Databricks default storage](https://docs.databricks.com/aws/en/storage/default-storage)
cannot vend the credentials needed for this path. Delta Sharing/OpenSharing
also uses a different authorization flow and is unsupported by this integration.

Reader-protocol validation accepts versions 1, 2, and 3. Supported reader features
are `timestampNtz`, `deletionVectors`, `columnMapping`, `v2Checkpoint`,
`vacuumProtocolCheck`, `typeWidening`, and `typeWidening-preview`. The table and
policy restrictions above still apply.

Run the [inspection step](../unity-catalog.md#inspect-and-pin-the-table) to check
your table with the installed DAR version. UC inspection fails if it rejects
the table or its read protocol; a successful summary reports
`checks.scan_protocol: "passed"`. This checks snapshot metadata, not access to
every data file. An unsupported result means that DAR cannot read the selected
snapshot; changing login credentials cannot add protocol support.

DAR gets the storage location and READ credentials from Unity Catalog. It reads
the Delta log for the snapshot version and Arrow schema; UC column metadata
does not define the output schema. It submits no SQL or compute workload to
Databricks. UC scans and previews accept the same projection, predicate, row
limit, snapshot, and execution options as direct-path reads.

## Storage backends and credentials

R2 can be configured as a UC [external location](https://docs.databricks.com/aws/en/connect/unity-catalog/cloud-storage/external-locations-r2).

| Backend | Temporary credentials | Accepted storage locations returned by UC |
| --- | --- | --- |
| S3 | Access key, secret key, session token | `s3://bucket/path` or `s3a://bucket/path`; S3 access points are unsupported |
| Azure Blob / ADLS | User-delegation SAS or Azure AD bearer token | `abfss://` or Azure Blob/DFS HTTPS URLs identifying the account and container |
| GCS | OAuth bearer token | `gs://bucket/path` |
| R2 | Access key, secret key, session token | `r2://bucket@account.r2.cloudflarestorage.com/path` or `https://account.r2.cloudflarestorage.com/bucket/path` |

Other credential forms and other Azure Blob/R2 endpoint forms are unsupported.
The location comes from UC; users supply the catalog name, not these storage URLs.

### S3 region

S3 needs one nonempty region setting. The credential response does not include
the region, and DAR does not assume a default. Ask the table or storage owner
for the bucket's region and save it as `storage.json`, for example:

```json
{"aws_region":"eu-west-1"}
```

Pass the file to each positional-table command:

```sh
dar inspect uc://main.sales.orders --view summary --databricks-profile work \
  --storage-options-file storage.json
```

For a manifest entry, use `authorization.storage_options_file` instead. That
path is relative to the manifest directory.

### Other storage options

`--storage-options-file` may supply supported region and transport settings,
including endpoints, timeouts, and proxies. It cannot replace the credentials,
bucket, account, container, authentication source, or signature requirements
returned by UC. Those overrides and their aliases are rejected. Unknown
options also fail.

R2 selects region `auto` and needs no region option; an explicit region must be
`auto`. To override its transport endpoint, use `aws_endpoint_url_s3`. The
generic endpoint option is rejected because the R2 URL would override it.

## Credential renewal and failures

Credentials stay in memory for one command. Storage requests reuse the grant
until it approaches expiry, then request new READ credentials for the original
table ID and profile. Concurrent requests share one refresh. A replacement must
keep the same storage root and credential type and have a future expiry.

Renewal preserves the selected Delta snapshot. It does not restart a scan or
repeat rows already written. If renewal fails, the command fails without using
the old grant or falling back to environment or other cloud credentials.
Renewal failures leave preview and JSON query stdout empty. Arrow scans or
queries may leave partial output; discard it and retry the whole command after
correcting the cause.

Each Databricks subprocess has a 30-second timeout, no interactive stdin, and a
1 MiB limit on each output stream. This is a subprocess limit, not a total read
deadline. DAR captures and discards child diagnostics. Its stderr errors omit
profile names, table names, storage URLs, and credentials. See
[troubleshooting](../unity-catalog.md#if-a-command-fails) for next actions and
[CLI diagnostics](cli-diagnostics.md) for exits, signals, and completion records.

## Databricks CLI compatibility

The Databricks CLI handles the selected profile's PAT, user OAuth, or
service-principal OAuth authentication. DAR does not start a login flow or read
the CLI token cache. The executable must support:

- `tables get --include-manifest-capabilities`
- `temporary-table-credentials generate-temporary-table-credentials --operation READ`

These commands and flags were checked with CLI 1.6.0; live Azure validation used
1.15.0. This does not establish a minimum supported version.

## Validation coverage

Offline tests cover all credential forms in the storage table above, including
renewal failures. Live validation covers Azure user OAuth, user-delegation SAS,
and a managed Delta table on macOS and Linux ARM64. It passed inspection, scans,
SQL aggregation, and UC/local joins with native source builds, a macOS dev-profile
wheel, and a Linux release archive.
Those live runs used Arrow output; they did not exercise previews or JSON queries.

Other storage backends, Azure bearer credentials, other authentication modes,
live renewal, and the full release platform matrix remain unverified online.
Exact environments and coverage gaps are in the repository's
[Azure validation record](https://github.com/mag1cfrog/delta-arrow-reader/blob/main/crates/delta-arrow-reader-cli/release/uc-azure-validation-2026-10-22.md).
